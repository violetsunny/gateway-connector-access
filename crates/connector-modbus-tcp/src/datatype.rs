//! DataType / ByteOrder / 寄存器字节解析——Java `DataTypeEnum` + `IotByteUtils` 的等价重写
//! （票 07 验收核心，逐类型对拍测试见本模块 tests）。
//!
//! 语义基准（Java 源码位于 connectors/modbus-tcp/.../gateway/modbus/，行号引用原文件）：
//! - `constant/DataTypeEnum.java`：类型字符串与 byteNum（读请求数量，单位寄存器/线圈，
//!   经 `ModbusDataHandler.transAmount` 使用）。注意 `getInstance`(:152-216) 没有
//!   int24/uint24 分支——枚举常量 INT24/UINT24 是不可达死类型，Rust 不建模，
//!   字符串 "int24"/"uint24" 与 Java 一致地报「未知的数据类型」。
//! - `constant/ByteOrderEnum.java`(:21-41)：8 个字节序字符串 → (byteSwap, wordSwap) 组合，
//!   ABCD≡ABCDEFGH、BADC≡BADCFEHG、CDAB≡GHEFCDAB、DCBA≡HGFEDCBA。
//! - `utils/IotByteUtils.java`：
//!   - `calculateRegisterNumberForRegisterRegion`(:44-88) → [`DataType::parse_size`]；
//!   - `getNumberFromRegisterRegion`(:160-269) → [`DataType::parse_value`]
//!     （长度校验 → 截取 bytesNum → 归一为大端 → 按类型取值）；
//!   - `swap2BigEndian/swapByte/swapWord/swapDword`(:292-357) → [`ByteOrder::swap_to_big_endian`]；
//!   - `getBooleanFromRegister`(:280-282) → Bit(n) 分支；
//!   - `getTime48`(:372-382) / `getTime64`(:400-411) → Time48/Time64 分支。
//!
//! Java 内部的不一致原样保留：TIME64 的 byteNum=4（请求 4 寄存器=8 字节），
//! 但 calculateRegisterNumberForRegisterRegion(TIME64)=3（解析只用前 6 字节）。

use std::str::FromStr;

use connector_common::{CommonError, Result};
use serde_json::Value as JsonValue;

/// 点表 dataType，对齐 Java `DataTypeEnum` 的可达集合（bit0..bit15 + 14 个数值/时间类型）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    /// bit0..bit15：从 16 位寄存器提取第 n 位（Java BIT0..BIT15）。
    Bit(u8),
    Int8,
    Uint8,
    Int16,
    Uint16,
    Int32,
    Uint32,
    Int48,
    Uint48,
    Int64,
    Uint64,
    Float32,
    Float64,
    Time48,
    Time64,
}

impl FromStr for DataType {
    type Err = CommonError;

    /// 对齐 Java `DataTypeEnum.getInstance`(:152-216)：精确字符串匹配，未知类型报错。
    fn from_str(s: &str) -> Result<Self> {
        let dt = match s {
            "bit0" => Self::Bit(0),
            "bit1" => Self::Bit(1),
            "bit2" => Self::Bit(2),
            "bit3" => Self::Bit(3),
            "bit4" => Self::Bit(4),
            "bit5" => Self::Bit(5),
            "bit6" => Self::Bit(6),
            "bit7" => Self::Bit(7),
            "bit8" => Self::Bit(8),
            "bit9" => Self::Bit(9),
            "bit10" => Self::Bit(10),
            "bit11" => Self::Bit(11),
            "bit12" => Self::Bit(12),
            "bit13" => Self::Bit(13),
            "bit14" => Self::Bit(14),
            "bit15" => Self::Bit(15),
            "int8" => Self::Int8,
            "uint8" => Self::Uint8,
            "int16" => Self::Int16,
            "uint16" => Self::Uint16,
            "int32" => Self::Int32,
            "uint32" => Self::Uint32,
            "int48" => Self::Int48,
            "uint48" => Self::Uint48,
            "int64" => Self::Int64,
            "uint64" => Self::Uint64,
            "float32" => Self::Float32,
            "float64" => Self::Float64,
            "time48" => Self::Time48,
            "time64" => Self::Time64,
            // Java getInstance 无 int24/uint24 分支（死类型），与未知类型同样拒绝
            other => {
                return Err(CommonError::Config(format!(
                    "未知的数据类型，dataType:{other}"
                )));
            }
        };
        Ok(dt)
    }
}

impl DataType {
    /// 读请求数量（寄存器/线圈个数），对齐 Java `DataTypeEnum.byteNum`
    /// （`ModbusDataHandler.buildReadReq` 经 `transAmount`(:142-151) 使用）。
    pub fn request_quantity(self) -> u16 {
        match self {
            Self::Bit(_) | Self::Int8 | Self::Uint8 | Self::Int16 | Self::Uint16 => 1,
            Self::Int32 | Self::Uint32 | Self::Float32 => 2,
            Self::Int48 | Self::Uint48 | Self::Time48 => 3,
            Self::Int64 | Self::Uint64 | Self::Float64 | Self::Time64 => 4,
        }
    }

    /// 解析用字节数 = 2 × 寄存器数，对齐 Java
    /// `IotByteUtils.calculateRegisterNumberForRegisterRegion`(:44-88)。
    /// 注意 Time64=6 字节（3 寄存器）而 [`request_quantity`]=4，Java 原样如此。
    pub fn parse_size(self) -> usize {
        match self {
            Self::Bit(_) | Self::Int8 | Self::Uint8 | Self::Int16 | Self::Uint16 => 2,
            Self::Int32 | Self::Uint32 | Self::Float32 => 4,
            Self::Int48 | Self::Uint48 | Self::Time48 | Self::Time64 => 6,
            Self::Int64 | Self::Uint64 | Self::Float64 => 8,
        }
    }

    /// 从响应字节提取值，对齐 Java `IotByteUtils.getNumberFromRegisterRegion`(:160-269)：
    /// 长度校验（不足报「数组长度不够」）→ 截取前 bytesNum 字节 → 按字节序归一为大端 →
    /// 按类型取值。返回 JSON 数值（Java Number 经 Jackson 序列化后的 wire 形态）。
    pub fn parse_value(self, wire: &[u8], order: ByteOrder) -> Result<JsonValue> {
        let need = self.parse_size();
        if wire.len() < need {
            return Err(CommonError::Config(format!(
                "数组长度不够, readableBytes:{}, dataType:{self:?}, bytesNum:{need}",
                wire.len()
            )));
        }
        // Java: byteBuf.copy(startIndex, bytesNum)，多余字节截断
        let mut buf = wire[..need].to_vec();
        order.swap_to_big_endian(&mut buf);
        Ok(match self {
            Self::Bit(n) => {
                // Java getBooleanFromRegister(:280-282): (byte)(readShort() >> bitIndex & 0x01)。
                // 有符号 short 算术右移后取最低位，与无符号提取逐位等价。
                let bits = u16::from_be_bytes(be(&buf, 0));
                serde_json::json!((bits >> n) & 1)
            }
            // Java case INT8/UINT8(:179-186)：寄存器 2 字节，大端时跳过高字节读低字节
            Self::Int8 => serde_json::json!(buf[1] as i8),
            Self::Uint8 => serde_json::json!(buf[1]),
            Self::Int16 => serde_json::json!(i16::from_be_bytes(be(&buf, 0))),
            Self::Uint16 => serde_json::json!(u16::from_be_bytes(be(&buf, 0))),
            Self::Int32 => serde_json::json!(i32::from_be_bytes(be(&buf, 0))),
            Self::Uint32 => serde_json::json!(u32::from_be_bytes(be(&buf, 0))),
            Self::Int48 => {
                // Java case INT48(:201-205): hInt = readInt()（有符号）; hInt << 16 | lShort
                let hi = i64::from(i32::from_be_bytes(be(&buf, 0)));
                let lo = u16::from_be_bytes(be(&buf, 4)) as i64;
                serde_json::json!(hi << 16 | lo)
            }
            Self::Uint48 => {
                // Java case UINT48(:206-210): hInt = readUnsignedInt(); hInt << 16 | lShort
                let hi = u32::from_be_bytes(be(&buf, 0)) as u64;
                let lo = u16::from_be_bytes(be(&buf, 4)) as u64;
                serde_json::json!(hi << 16 | lo)
            }
            Self::Int64 => serde_json::json!(i64::from_be_bytes(be(&buf, 0))),
            Self::Uint64 => {
                // Java case UINT64(:213-221): BigInteger(hInt * 2^32 + lInt)，即无符号 64 位全量
                serde_json::json!(u64::from_be_bytes(be(&buf, 0)))
            }
            // Java Float 经 Jackson Float.toString 输出单精度最短表示（0.1 而非
            // 0.10000000149011612）。serde_json Number 内部是 f64，f32→f64 直接扩宽会
            // 引入双精度噪声，故先按 f32 最短表示格式化再解析回 f64。
            // NaN/无穷无 JSON 字面量（Java Jackson 输出非标准 NaN），Value::from 落 Null。
            Self::Float32 => {
                let v = f32::from_be_bytes(be(&buf, 0));
                let widened = v
                    .to_string()
                    .parse::<f64>()
                    .expect("f32 最短表示必可解析为 f64");
                JsonValue::from(widened)
            }
            Self::Float64 => serde_json::json!(f64::from_be_bytes(be(&buf, 0))),
            Self::Time48 => {
                // Java getTime48(:372-382)：格式 yyyyMMddHHmm，
                // 值 = 1e8*yyyy + 1e6*MM + 1e4*dd + 100*HH + mm
                let year = i64::from(u16::from_be_bytes(be(&buf, 0)));
                serde_json::json!(
                    100_000_000 * year
                        + 1_000_000 * i64::from(buf[2])
                        + 10_000 * i64::from(buf[3])
                        + 100 * i64::from(buf[4])
                        + i64::from(buf[5])
                )
            }
            Self::Time64 => {
                // Java getTime64(:400-411)：格式 20yyMMddHHmmss，
                // 值 = 1e10*(yy+2000) + 1e8*MM + 1e6*dd + 1e4*HH + 100*mm + ss
                serde_json::json!(
                    10_000_000_000 * (i64::from(buf[0]) + 2000)
                        + 100_000_000 * i64::from(buf[1])
                        + 1_000_000 * i64::from(buf[2])
                        + 10_000 * i64::from(buf[3])
                        + 100 * i64::from(buf[4])
                        + i64::from(buf[5])
                )
            }
        })
    }
}

/// 取大端字节片段。`buf` 长度恒等于 `parse_size`（parse_value 已截取），
/// 各类型分支的读取范围均在界内，expect 仅在内部不变量被破坏时触发。
fn be<const N: usize>(buf: &[u8], at: usize) -> [u8; N] {
    buf[at..at + N].try_into().expect("parse_size 不变量被破坏")
}

/// 字节序，对齐 Java `ByteOrderEnum`：8 个字符串实为 (byteSwap, wordSwap) 的 4 种组合。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteOrder {
    /// 字节交换（每 16 位字内高低字节互换），Java BADC/BADCFEHG/DCBA/HGFEDCBA。
    pub byte_swap: bool,
    /// 字交换（swapWord + swapDword），Java CDAB/GHEFCDAB/DCBA/HGFEDCBA。
    pub word_swap: bool,
}

impl FromStr for ByteOrder {
    type Err = CommonError;

    /// 对齐 Java `ByteOrderEnum.getInstance`(:69-90)：按 value 字符串匹配，未知报「未知的字节序」。
    fn from_str(s: &str) -> Result<Self> {
        let order = match s {
            // ABCD / ABCDEFGH：大端，无交换
            "1234" | "12345678" => Self {
                byte_swap: false,
                word_swap: false,
            },
            // BADC / BADCFEHG：仅字节交换
            "2143" | "21436587" => Self {
                byte_swap: true,
                word_swap: false,
            },
            // CDAB / GHEFCDAB：仅字交换
            "3412" | "78563412" => Self {
                byte_swap: false,
                word_swap: true,
            },
            // DCBA / HGFEDCBA：字节 + 字交换（= 整体反转）
            "4321" | "87654321" => Self {
                byte_swap: true,
                word_swap: true,
            },
            other => {
                return Err(CommonError::Config(format!(
                    "未知的字节序，byteOrder:{other}"
                )));
            }
        };
        Ok(order)
    }
}

impl ByteOrder {
    /// 把 wire 字节归一为大端，对齐 Java `IotByteUtils.swap2BigEndian`(:292-300)：
    /// 先 swapByte，再 swapWord + swapDword。
    /// 交换分组按缓冲区长度整除截断（as_chunks 丢余数），与 Java 整数除法循环界等价：
    /// 6 字节缓冲做 wordSwap 时只交换前 4 字节（swapWord 一组、swapDword 零组）。
    pub fn swap_to_big_endian(self, buf: &mut [u8]) {
        if self.byte_swap {
            // swapByte(:309-319)：每 2 字节内互换
            for word in buf.as_chunks_mut::<2>().0 {
                word.swap(0, 1);
            }
        }
        if self.word_swap {
            // swapWord(:328-338)：每 4 字节组内交换两个 16 位半区
            for group in buf.as_chunks_mut::<4>().0 {
                let (hi, lo) = group.split_at_mut(2);
                hi.swap_with_slice(lo);
            }
            // swapDword(:347-357)：每 8 字节组内交换两个 32 位半区
            for group in buf.as_chunks_mut::<8>().0 {
                let (hi, lo) = group.split_at_mut(4);
                hi.swap_with_slice(lo);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn dt(s: &str) -> DataType {
        s.parse::<DataType>().unwrap()
    }

    fn order(s: &str) -> ByteOrder {
        s.parse::<ByteOrder>().unwrap()
    }

    /// 大端无交换（ABCD "1234"）。
    fn abcd() -> ByteOrder {
        order("1234")
    }

    fn parse(dt: DataType, wire: &[u8], order: ByteOrder) -> JsonValue {
        dt.parse_value(wire, order).unwrap()
    }

    // ---- bit0..bit15：Java IotByteUtils.getBooleanFromRegister(:280-282) ----

    /// 全部 16 个 bit 类型逐一提取。期望值手工推导：
    /// 0xA53C = 0b1010_0101_0011_1100，bit0 为 LSB。
    #[test]
    fn bits_all_sixteen_extract_from_register() {
        let wire = [0xA5, 0x3C];
        let expected = [0, 0, 1, 1, 1, 1, 0, 0, 1, 0, 1, 0, 0, 1, 0, 1];
        for (n, want) in expected.iter().enumerate() {
            let name = format!("bit{n}");
            assert_eq!(
                parse(dt(&name), &wire, abcd()),
                json!(want),
                "{name} of 0xA53C"
            );
        }
    }

    /// bit15 对负 short（0x8000 = -32768）：Java 用有符号 readShort 算术右移，
    /// (-32768 >> 15) & 1 = 1，与无符号提取一致。
    #[test]
    fn bit15_of_negative_short_matches_java_arithmetic_shift() {
        assert_eq!(parse(dt("bit15"), &[0x80, 0x00], abcd()), json!(1));
        assert_eq!(parse(dt("bit14"), &[0x80, 0x00], abcd()), json!(0));
        assert_eq!(parse(dt("bit0"), &[0x80, 0x00], abcd()), json!(0));
    }

    // ---- int8/uint8：Java case INT8/UINT8(:179-186)，跳过高字节读低字节 ----

    #[test]
    fn int8_reads_low_byte_signed() {
        assert_eq!(parse(dt("int8"), &[0x12, 0xFF], abcd()), json!(-1));
        assert_eq!(parse(dt("int8"), &[0x00, 0x7F], abcd()), json!(127));
        assert_eq!(parse(dt("int8"), &[0xFF, 0x80], abcd()), json!(-128));
    }

    #[test]
    fn uint8_reads_low_byte_unsigned() {
        assert_eq!(parse(dt("uint8"), &[0xFF, 0xFF], abcd()), json!(255));
        assert_eq!(parse(dt("uint8"), &[0x12, 0x34], abcd()), json!(52));
    }

    // ---- int16/uint16：Java readShort/readUnsignedShort(:187-190) ----

    #[test]
    fn int16_big_endian_signed() {
        assert_eq!(parse(dt("int16"), &[0xFF, 0xFF], abcd()), json!(-1));
        assert_eq!(parse(dt("int16"), &[0x80, 0x00], abcd()), json!(-32768));
        assert_eq!(parse(dt("int16"), &[0x12, 0x34], abcd()), json!(4660));
    }

    #[test]
    fn uint16_big_endian_unsigned() {
        assert_eq!(parse(dt("uint16"), &[0xFF, 0xFF], abcd()), json!(65535));
        assert_eq!(parse(dt("uint16"), &[0x12, 0x34], abcd()), json!(4660));
    }

    // ---- int32/uint32：Java readInt/readUnsignedInt(:197-200) ----

    #[test]
    fn int32_big_endian_signed() {
        assert_eq!(
            parse(dt("int32"), &[0xFF, 0xFF, 0xFF, 0xFE], abcd()),
            json!(-2)
        );
        assert_eq!(
            parse(dt("int32"), &[0x12, 0x34, 0x56, 0x78], abcd()),
            json!(305419896)
        );
    }

    #[test]
    fn uint32_big_endian_unsigned() {
        assert_eq!(
            parse(dt("uint32"), &[0xFF, 0xFF, 0xFF, 0xFE], abcd()),
            json!(4294967294u64)
        );
    }

    // ---- int48/uint48：Java case INT48/UINT48(:201-210)，hInt << 16 | lShort ----

    #[test]
    fn int48_sign_extends_from_high_int() {
        // hInt = 0xFFFFFFFF = -1；-1i64 << 16 = 0xFFFF_FFFF_FFFF_0000；| 0x8000 → -32768
        assert_eq!(
            parse(dt("int48"), &[0xFF, 0xFF, 0xFF, 0xFF, 0x80, 0x00], abcd()),
            json!(-32768)
        );
        // hInt = 0x000087A2 = 34722；34722 << 16 | 0x3800 = 2275555328
        assert_eq!(
            parse(dt("int48"), &[0x00, 0x00, 0x87, 0xA2, 0x38, 0x00], abcd()),
            json!(2275555328i64)
        );
    }

    #[test]
    fn uint48_full_range() {
        assert_eq!(
            parse(dt("uint48"), &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF], abcd()),
            json!(281474976710655u64) // 2^48 - 1
        );
    }

    // ---- int64/uint64：Java readLong(:211-212) / BigInteger 组合(:213-221) ----

    #[test]
    fn int64_big_endian_signed() {
        assert_eq!(
            parse(dt("int64"), &[0x80, 0, 0, 0, 0, 0, 0, 0], abcd()),
            json!(-9223372036854775808i64)
        );
        // Java IotByteUtils.main(:432) 的「大大」字节序样例：0x87A238 = 8888888
        assert_eq!(
            parse(dt("int64"), &[0, 0, 0, 0, 0, 0x87, 0xA2, 0x38], abcd()),
            json!(8888888)
        );
    }

    #[test]
    fn uint64_covers_full_unsigned_range() {
        // Java: BigInteger(4294967295 * 2^32 + 4294967295) = 2^64 - 1（超过 Long.MAX）
        assert_eq!(
            parse(dt("uint64"), &[0xFF; 8], abcd()),
            json!(18446744073709551615u64)
        );
    }

    // ---- float32/float64：Java readFloat/readDouble(:227-230) ----

    #[test]
    fn float32_big_endian() {
        // 256.0f32 = 0x43800000（Java IotByteUtils.main 的示例值 256.0）
        assert_eq!(
            parse(dt("float32"), &[0x43, 0x80, 0x00, 0x00], abcd()),
            json!(256.0)
        );
        // -1.5f32 = 0xBFC00000
        assert_eq!(
            parse(dt("float32"), &[0xBF, 0xC0, 0x00, 0x00], abcd()),
            json!(-1.5)
        );
    }

    /// f32 0.1 序列化为最短表示 0.1（Java Float 经 Jackson 输出一致），
    /// 而非 f64 扩宽后的 0.10000000149011612。
    #[test]
    fn float32_keeps_single_precision_shortest_repr() {
        // 0.1f32 = 0x3DCCCCCD。注意 json!(0.1f32) 会被 serde_json 扩宽成
        // 0.10000000149011612，期望值须用 f64 字面量 0.1。
        let v = parse(dt("float32"), &[0x3D, 0xCC, 0xCC, 0xCD], abcd());
        assert_eq!(v, json!(0.1));
        assert_eq!(v.to_string(), "0.1");
    }

    #[test]
    fn float64_big_endian() {
        // 256.0f64 = 0x4070000000000000
        assert_eq!(
            parse(dt("float64"), &[0x40, 0x70, 0, 0, 0, 0, 0, 0], abcd()),
            json!(256.0)
        );
    }

    // ---- time48/time64：Java getTime48(:372-382) / getTime64(:400-411) javadoc 样例 ----

    #[test]
    fn time48_formats_yyyymmddhhmm() {
        // Java javadoc：0x07D1,06,07,03,03 → 2001年6月7日3时03分 → 200106070303
        assert_eq!(
            parse(dt("time48"), &[0x07, 0xD1, 0x06, 0x07, 0x03, 0x03], abcd()),
            json!(200106070303i64)
        );
    }

    #[test]
    fn time64_formats_20yymmddhhmmss() {
        // Java javadoc：0x01,0A,06,07,03,13 → 2001年10月06日07时03分19秒 → 20011006070319
        assert_eq!(
            parse(dt("time64"), &[0x01, 0x0A, 0x06, 0x07, 0x03, 0x13], abcd()),
            json!(20011006070319i64)
        );
    }

    // ---- 字节序：Java swap2BigEndian(:292-300) + main(:414-443) 样例向量 ----

    /// int32 值 8888888（0x0087A238）在 4 种交换组合下的 wire 字节，
    /// 对应 Java main 注释：大大 ABCDEFGH / 小小 HGFEDCBA / 大字节序小字序 GHEFCDAB /
    /// 小字节序大字序 BADCFEHG（4 字节场景取前 4 位命名）。
    #[test]
    fn int32_parses_under_all_four_swap_combinations() {
        let cases: [(&str, [u8; 4]); 4] = [
            ("1234", [0x00, 0x87, 0xA2, 0x38]), // ABCD：大端原样
            ("2143", [0x87, 0x00, 0x38, 0xA2]), // BADC：字节交换
            ("3412", [0xA2, 0x38, 0x00, 0x87]), // CDAB：字交换
            ("4321", [0x38, 0xA2, 0x87, 0x00]), // DCBA：整体反转
        ];
        for (name, wire) in cases {
            assert_eq!(
                parse(dt("int32"), &wire, order(name)),
                json!(8888888),
                "byteOrder {name}"
            );
        }
    }

    /// Java main(:436-441) 原样例：BADCFEHG("21436587") 对
    /// {00,00,00,00,87,00,38,A2} 做字节交换后 readLong = 0x87A238 = 8888888。
    #[test]
    fn uint64_badcfehg_matches_java_main_sample() {
        let wire = [0x00, 0x00, 0x00, 0x00, 0x87, 0x00, 0x38, 0xA2];
        assert_eq!(
            parse(dt("uint64"), &wire, order("21436587")),
            json!(8888888)
        );
    }

    /// 8 字节 wordSwap（GHEFCDAB "78563412"）：swapWord 在每 4 字节组内交换半区，
    /// swapDword 再交换两个 4 字节半区。0x0123456789ABCDEF 的 wire =
    /// swapDword(swapWord(大端)) = [CD,EF,89,AB,45,67,01,23]。
    #[test]
    fn uint64_ghefcdab_word_and_dword_swap() {
        let wire = [0xCD, 0xEF, 0x89, 0xAB, 0x45, 0x67, 0x01, 0x23];
        assert_eq!(
            parse(dt("uint64"), &wire, order("78563412")),
            json!(0x0123456789ABCDEFu64)
        );
        // HGFEDCBA("87654321") = 字节 + 字交换 = 整体反转
        let reversed = [0xEF, 0xCD, 0xAB, 0x89, 0x67, 0x45, 0x23, 0x01];
        assert_eq!(
            parse(dt("uint64"), &reversed, order("87654321")),
            json!(0x0123456789ABCDEFu64)
        );
    }

    /// 单寄存器类型做 wordSwap 是 no-op：Java swapWord 循环界 = 2/4 = 0。
    #[test]
    fn word_swap_is_noop_for_single_register() {
        assert_eq!(
            parse(dt("uint16"), &[0x12, 0x34], order("3412")),
            json!(4660)
        );
        assert_eq!(parse(dt("int16"), &[0xFF, 0xFE], order("3412")), json!(-2));
    }

    /// 6 字节类型（int48/time48/time64）做 wordSwap：Java swapWord 只处理前 4 字节
    /// （6/4=1 组），第 3 个字不动；swapDword 0 组。
    #[test]
    fn word_swap_on_six_bytes_only_swaps_first_two_words() {
        // 大端目标 [00,00,87,A2,38,00]（int48 = 2275555328），wire = [87,A2,00,00,38,00]
        assert_eq!(
            parse(
                dt("int48"),
                &[0x87, 0xA2, 0x00, 0x00, 0x38, 0x00],
                order("3412")
            ),
            json!(2275555328i64)
        );
    }

    /// 8 个字节序字符串全部接受且映射到正确的交换标志（Java ByteOrderEnum:21-41）。
    #[test]
    fn all_eight_byte_order_strings_accepted() {
        let cases = [
            ("1234", false, false),
            ("12345678", false, false),
            ("2143", true, false),
            ("21436587", true, false),
            ("3412", false, true),
            ("78563412", false, true),
            ("4321", true, true),
            ("87654321", true, true),
        ];
        for (s, byte_swap, word_swap) in cases {
            let o = order(s);
            assert_eq!(o.byte_swap, byte_swap, "{s}");
            assert_eq!(o.word_swap, word_swap, "{s}");
        }
    }

    #[test]
    fn unknown_byte_order_rejected() {
        let err = "9999".parse::<ByteOrder>().unwrap_err();
        assert!(
            err.to_string().contains("未知的字节序，byteOrder:9999"),
            "{err}"
        );
    }

    // ---- DataType 字符串集合与 Java getInstance 一致 ----

    /// 30 个可达类型字符串全部接受（bit0..bit15 + 14 个数值/时间类型）。
    #[test]
    fn all_thirty_data_type_strings_accepted() {
        for n in 0..16 {
            assert_eq!(
                format!("bit{n}").parse::<DataType>().unwrap(),
                DataType::Bit(n)
            );
        }
        for s in [
            "int8", "uint8", "int16", "uint16", "int32", "uint32", "int48", "uint48", "int64",
            "uint64", "float32", "float64", "time48", "time64",
        ] {
            assert!(s.parse::<DataType>().is_ok(), "{s}");
        }
    }

    /// 未知类型报错，含 int24/uint24（Java getInstance 无分支 → 不可达死类型）、
    /// bit16、大小写变体（Java switch 精确匹配）。
    #[test]
    fn unknown_data_type_rejected() {
        for s in ["int24", "uint24", "bit16", "INT32", "float", ""] {
            let err = s.parse::<DataType>().unwrap_err();
            assert!(
                err.to_string()
                    .contains(&format!("未知的数据类型，dataType:{s}")),
                "{s}: {err}"
            );
        }
    }

    // ---- 请求数量 vs 解析字节数（Java byteNum vs calculateRegisterNumberForRegisterRegion）----

    #[test]
    fn request_quantity_and_parse_size_match_java_tables() {
        // (类型, 请求数量=byteNum, 解析字节数=2*寄存器数)
        let cases: [(&str, u16, usize); 16] = [
            ("bit0", 1, 2),
            ("bit15", 1, 2),
            ("int8", 1, 2),
            ("uint8", 1, 2),
            ("int16", 1, 2),
            ("uint16", 1, 2),
            ("int32", 2, 4),
            ("uint32", 2, 4),
            ("float32", 2, 4),
            ("int48", 3, 6),
            ("uint48", 3, 6),
            ("time48", 3, 6),
            ("time64", 4, 6), // Java 不一致点：byteNum=4 但解析只用 3 寄存器
            ("int64", 4, 8),
            ("uint64", 4, 8),
            ("float64", 4, 8),
        ];
        for (s, qty, size) in cases {
            let t = dt(s);
            assert_eq!(t.request_quantity(), qty, "{s} quantity");
            assert_eq!(t.parse_size(), size, "{s} parse_size");
        }
    }

    // ---- 长度校验与截断（Java getNumberFromRegisterRegion:162-175）----

    #[test]
    fn short_buffer_reports_length_error() {
        let err = dt("int32").parse_value(&[0x00, 0x01], abcd()).unwrap_err();
        assert!(err.to_string().contains("数组长度不够"), "{err}");
        assert!(err.to_string().contains("bytesNum:4"), "{err}");
    }

    /// 多余字节按 Java copy(startIndex, bytesNum) 截断（TIME64 请求 4 寄存器只用前 6 字节）。
    #[test]
    fn extra_bytes_are_truncated() {
        assert_eq!(
            parse(
                dt("time64"),
                &[0x01, 0x0A, 0x06, 0x07, 0x03, 0x13, 0xFF, 0xFF],
                abcd()
            ),
            json!(20011006070319i64)
        );
        assert_eq!(
            parse(dt("uint16"), &[0x12, 0x34, 0xFF, 0xFF], abcd()),
            json!(4660)
        );
    }
}
