//! 任务管理器：运行期任务表 + 增删改状态机。
//!
//! 对齐 Java `TrdPlatformCloudServerImpl.operateTaskWork`（:571-607）：
//! - ADD(1)：已存在 → 保留原任务只打日志（:590-596）；否则新建调度。
//! - UPDATE(2)：存在先移除再新建（:597-603）。
//! - REMOVE(3)：移除；不存在 → warn 日志（失败可见，不算错误）。
//! - 其他 operate → Err。
//!
//! 与 Java 的刻意差异：
//! - Java 请求上下文查 DB（taskReqContext），Rust 一期 ADD/UPDATE 必须内嵌完整任务定义；
//! - Java `frequency` 空白/非法时静默 return（:583-584），Rust 报错（失败可见）；
//! - 调度用 cron 表达式（frequency 优先于任务自带 cron），替代 xxl-job。

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use connector_common::sink::DeviceSink;
use connector_common::{CommonError, Result};
use cron::Schedule;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::config::{RequestTaskConfig, parse_cron};
use crate::task::{Counters, TaskRunner, run_cron_loop};

/// operate 编码，对齐 Java `TrdPlatformEnum`（ADD=1/UPDATE=2/REMOVE=3）。
pub(crate) const OP_ADD: i32 = 1;
pub(crate) const OP_UPDATE: i32 = 2;
pub(crate) const OP_REMOVE: i32 = 3;

struct RunningTask {
    runner: Arc<TaskRunner>,
    /// cron 调度句柄；None = 仅下控任务（无 cron）。
    handle: Option<JoinHandle<()>>,
}

/// start() 注入的运行时依赖（构建 runner / spawn 调度所需）。
struct Runtime {
    sink: Arc<dyn DeviceSink>,
    client: reqwest::Client,
}

pub(crate) struct TaskManager {
    tasks: Mutex<HashMap<String, RunningTask>>,
    runtime: Mutex<Option<Runtime>>,
    /// 脚本目录白名单（None = 不启用，向后兼容），构建 runner 时透传。
    script_dir: Option<String>,
    counters: Arc<Counters>,
    shutdown: watch::Sender<bool>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl TaskManager {
    pub(crate) fn new(
        counters: Arc<Counters>,
        shutdown: watch::Sender<bool>,
        script_dir: Option<String>,
    ) -> Self {
        Self {
            tasks: Mutex::new(HashMap::new()),
            runtime: Mutex::new(None),
            script_dir,
            counters,
            shutdown,
        }
    }

    /// start() 时注入 sink 与共享 HTTP client；此前任何注册操作报错（失败可见）。
    pub(crate) fn set_runtime(&self, sink: Arc<dyn DeviceSink>, client: reqwest::Client) {
        *lock(&self.runtime) = Some(Runtime { sink, client });
    }

    /// 启动加载：注册 TOML 配置任务（对齐 RequestConnectorStater.init:59-83 的任务列表加载）。
    /// 重复键 → Err（配置错误必须启动期暴露）。
    pub(crate) fn register_config_task(&self, def: &RequestTaskConfig) -> Result<()> {
        let cron_expr = def.cron_expr().map(str::to_string);
        let schedule = cron_expr.as_deref().map(parse_cron).transpose()?;
        if lock(&self.tasks).contains_key(&def.key()) {
            return Err(CommonError::Config(format!(
                "request 任务重复: {}",
                def.key()
            )));
        }
        self.spawn(def, &def.appid, &def.server, cron_expr, schedule)?;
        Ok(())
    }

    /// Kafka 动态增删改入口（对齐 RequestConnectorStater.listen:99-113 → operateTaskWork）。
    ///
    /// - `frequency`：消息里的 cron（Java 用 frequency 调度 xxl-job），非空时优先于 def.cron；
    /// - `def`：ADD/UPDATE 必须携带完整任务定义（Rust 一期无 DB）；REMOVE 可为 None。
    pub(crate) fn apply_operate(
        &self,
        operate: i32,
        pcode: &str,
        task_code: &str,
        frequency: Option<&str>,
        def: Option<RequestTaskConfig>,
    ) -> Result<()> {
        let key = format!("{pcode}-{task_code}");
        match operate {
            OP_ADD => {
                if lock(&self.tasks).contains_key(&key) {
                    info!(target: "request", "任务 {key} 已存在，忽略 ADD（对齐 Java operateTaskWork:591-593）");
                    return Ok(());
                }
                let def = require_def(operate, &key, def)?;
                let (cron_expr, schedule) = resolve_cron(&key, frequency, &def)?;
                self.spawn(&def, pcode, task_code, cron_expr, schedule)
            }
            OP_UPDATE => {
                self.cancel(&key);
                let def = require_def(operate, &key, def)?;
                let (cron_expr, schedule) = resolve_cron(&key, frequency, &def)?;
                self.spawn(&def, pcode, task_code, cron_expr, schedule)
            }
            OP_REMOVE => {
                if self.cancel(&key) {
                    info!(target: "request", "任务 {key} 已移除");
                } else {
                    warn!(target: "request", "REMOVE 任务 {key} 不存在（忽略）");
                }
                Ok(())
            }
            other => Err(CommonError::Config(format!(
                "未知 operate={other}（1=ADD 2=UPDATE 3=REMOVE，对齐 TrdPlatformEnum）"
            ))),
        }
    }

    /// 下控任务查找：function == taskCode（大小写不敏感，对齐 DownRequestHandler:84
    /// `server.equalsIgnoreCase(protocolConfig.getServer())`）。
    /// ponytail: 无 Redis 的 deviceId→productId 映射，仅按 taskCode 匹配；多平台同 taskCode
    /// 时取键序最小者保证确定性；升级路径：接设备目录后按 productId 过滤。
    pub(crate) fn find_by_function(&self, function: &str) -> Option<Arc<TaskRunner>> {
        let tasks = lock(&self.tasks);
        let mut keys: Vec<&String> = tasks
            .iter()
            .filter(|(_, t)| t.runner.task_code.eq_ignore_ascii_case(function))
            .map(|(k, _)| k)
            .collect();
        keys.sort();
        keys.first().map(|k| tasks[*k].runner.clone())
    }

    pub(crate) fn task_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = lock(&self.tasks).keys().cloned().collect();
        keys.sort();
        keys
    }

    pub(crate) fn task_count(&self) -> usize {
        lock(&self.tasks).len()
    }

    /// stop()：移除全部任务并等待调度循环退出（shutdown 已发信号，sleep 会被打断）。
    pub(crate) async fn stop_all(&self) {
        let tasks: Vec<RunningTask> = lock(&self.tasks).drain().map(|(_, t)| t).collect();
        for task in tasks {
            if let Some(handle) = task.handle {
                let _ = handle.await;
            }
        }
    }

    /// 移除并中止单个任务，返回是否存在。REMOVE 用 abort（Java removeTask 直接删 xxl-job 触发器）。
    fn cancel(&self, key: &str) -> bool {
        let Some(task) = lock(&self.tasks).remove(key) else {
            return false;
        };
        if let Some(handle) = task.handle {
            handle.abort();
        }
        true
    }

    fn spawn(
        &self,
        def: &RequestTaskConfig,
        pcode: &str,
        task_code: &str,
        cron_expr: Option<String>,
        schedule: Option<Schedule>,
    ) -> Result<()> {
        let runtime = lock(&self.runtime);
        let runtime = runtime.as_ref().ok_or_else(|| {
            CommonError::Internal("request connector 未启动（无 sink），无法注册任务".to_string())
        })?;
        let runner = TaskRunner::new(
            pcode,
            task_code,
            cron_expr,
            def,
            runtime.client.clone(),
            self.counters.clone(),
            self.script_dir.as_deref(),
        )?;
        let handle = schedule.map(|schedule| {
            tokio::spawn(run_cron_loop(
                runner.clone(),
                runtime.sink.clone(),
                schedule,
                self.shutdown.subscribe(),
            ))
        });
        info!(
            target: "request",
            "注册任务 {}（cron: {}）",
            runner.key,
            runner.cron_expr.as_deref().unwrap_or("无，仅下控")
        );
        lock(&self.tasks).insert(runner.key.clone(), RunningTask { runner, handle });
        Ok(())
    }
}

fn require_def(
    operate: i32,
    key: &str,
    def: Option<RequestTaskConfig>,
) -> Result<RequestTaskConfig> {
    def.ok_or_else(|| {
        CommonError::Config(format!(
            "operate={operate} 任务 {key}: 消息缺少 task 定义（Rust 一期无 DB，ADD/UPDATE 必须内嵌完整任务定义）"
        ))
    })
}

/// 生效 cron：消息 frequency（非空必须合法）优先，其次任务自带 cron；都没有 → Err。
fn resolve_cron(
    key: &str,
    frequency: Option<&str>,
    def: &RequestTaskConfig,
) -> Result<(Option<String>, Option<Schedule>)> {
    let expr = match frequency.map(str::trim).filter(|s| !s.is_empty()) {
        Some(f) => f.to_string(),
        None => def.cron_expr().map(str::to_string).ok_or_else(|| {
            CommonError::Config(format!(
                "任务 {key}: 消息 frequency 与任务 cron 均为空，无法调度（Java 侧此场景静默跳过，Rust 报错可见）"
            ))
        })?,
    };
    let schedule = parse_cron(&expr)?;
    Ok((Some(expr), Some(schedule)))
}
