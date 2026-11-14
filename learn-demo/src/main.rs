use rand::RngExt;
use std::cmp::Ordering;

/// 二分查找：在已排序切片中查找 target，返回其下标。
/// 前提：arr 必须升序有序，否则结果无意义。
fn binary_search<T: Ord>(arr: &[T], target: &T) -> Option<usize> {
    let (mut lo, mut hi) = (0usize, arr.len());
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        match arr[mid].cmp(target) {
            Ordering::Less => lo = mid + 1,
            Ordering::Greater => hi = mid,
            Ordering::Equal => return Some(mid),
        }
    }
    None
}

/// 归并排序：返回升序排序后的新 Vec，不修改原切片。
fn merge_sort<T: Ord + Clone>(arr: &[T]) -> Vec<T> {
    let n = arr.len();
    if n <= 1 {
        return arr.to_vec();
    }
    let mid = n / 2;
    let left = merge_sort(&arr[..mid]);
    let right = merge_sort(&arr[mid..]);
    merge(&left, &right)
}

/// 合并两个已排序切片为一个有序 Vec。
fn merge<T: Ord + Clone>(left: &[T], right: &[T]) -> Vec<T> {
    let mut result = Vec::with_capacity(left.len() + right.len());
    let (mut i, mut j) = (0, 0);
    while i < left.len() && j < right.len() {
        if left[i] <= right[j] {
            result.push(left[i].clone());
            i += 1;
        } else {
            result.push(right[j].clone());
            j += 1;
        }
    }
    result.extend_from_slice(&left[i..]);
    result.extend_from_slice(&right[j..]);
    result
}

fn main() {
    let mut rng = rand::rng();
    // 生成 10 个 1~100 的随机整数
    let data: Vec<i32> = (0..10).map(|_| rng.random_range(1..=100)).collect();
    println!("原始数据: {:?}", data);

    let sorted = merge_sort(&data);
    println!("归并排序: {:?}", sorted);

    let target = sorted[0];
    match binary_search(&sorted, &target) {
        Some(idx) => println!("二分查找 {}: 命中下标 {}", target, idx),
        None => println!("二分查找 {}: 未找到", target),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_sort_sorts_ascending() {
        assert_eq!(merge_sort(&[5, 2, 9, 1, 5, 6]), vec![1, 2, 5, 5, 6, 9]);
        assert_eq!(merge_sort(&[] as &[i32]), vec![]);
        assert_eq!(merge_sort(&[42]), vec![42]);
    }

    #[test]
    fn binary_search_finds_and_misses() {
        let arr = [1, 3, 5, 7, 9];
        assert_eq!(binary_search(&arr, &1), Some(0));
        assert_eq!(binary_search(&arr, &9), Some(4));
        assert_eq!(binary_search(&arr, &5), Some(2));
        assert_eq!(binary_search(&arr, &4), None);
        assert_eq!(binary_search(&[] as &[i32], &1), None);
    }

    #[test]
    fn sort_then_search_roundtrip() {
        let data = [10, -1, 3, 3, 0, 7];
        let sorted = merge_sort(&data);
        for x in &data {
            assert!(binary_search(&sorted, x).is_some());
        }
        assert_eq!(binary_search(&sorted, &100), None);
    }
}
