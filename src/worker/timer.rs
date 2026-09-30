//! 定时器调度 — deadline 驱动的最小堆
//!
//! 每个 `Rtc` 实例在 `poll_output()` 返回 `Output::Timeout(deadline)` 时
//! 告知「下次需要被驱动」的时刻。SFU 需要据此在正确的时刻调用
//! `handle_input(Input::Timeout)`，而不是定时全量扫描。
//!
//! 早期实现每 10ms 无条件遍历所有参与者调用 `handle_input(Timeout)`，
//! 复杂度 O(N)；参与者上千时，即使全部空闲也会持续消耗 CPU。
//!
//! 这里改为按 deadline 排序的最小堆:
//!   - `schedule(id, deadline)`: 登记/更新某个参与者的下次唤醒时刻；
//!   - `pop_due(now)`: 仅返回真正到期的参与者 id。
//!
//! 采用「惰性删除」: 同一 id 的旧 deadline 条目会残留在堆里，通过
//! `current` 表判断条目是否仍然有效，无效条目在 peek/pop 时顺带丢弃。
//! 这样 `schedule` 只需 O(log N) 入堆，无需定位并删除旧条目。

use ahash::AHashMap;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Instant;

/// 基于最小堆的 deadline 调度器
pub struct TimerWheel {
    /// 最小堆: (deadline, participant_id)。同一 id 可能残留多个历史条目。
    heap: BinaryHeap<Reverse<(Instant, String)>>,
    /// id → 当前有效的 deadline，用于识别堆中过期的历史条目。
    current: AHashMap<String, Instant>,
}

impl TimerWheel {
    pub fn new() -> Self {
        Self {
            heap: BinaryHeap::new(),
            current: AHashMap::new(),
        }
    }

    /// 登记/更新某个 id 的下次唤醒时刻。
    ///
    /// deadline 未变化时直接返回 (收包热路径每包都会调用，但多数时候值不变)。
    pub fn schedule(&mut self, id: &str, deadline: Instant) {
        if self.current.get(id) == Some(&deadline) {
            return;
        }
        self.current.insert(id.to_string(), deadline);
        self.heap.push(Reverse((deadline, id.to_string())));
    }

    /// 取消 id 的定时器 (参与者被移除时调用)。
    ///
    /// 堆中的历史条目会惰性丢弃，这里只清除权威表项。
    pub fn cancel(&mut self, id: &str) {
        self.current.remove(id);
    }

    /// 返回下一个到期时刻，同时清理堆顶的过期条目。
    ///
    /// 返回 `None` 表示当前没有任何待驱动的定时器。
    pub fn next_deadline(&mut self) -> Option<Instant> {
        loop {
            let valid = match self.heap.peek() {
                Some(Reverse((deadline, id))) => self.current.get(id) == Some(deadline),
                None => return None,
            };
            if valid {
                return self.heap.peek().map(|Reverse((deadline, _))| *deadline);
            }
            // 堆顶是历史条目，丢弃后继续看下一个
            self.heap.pop();
        }
    }

    /// 取出并返回所有 `deadline <= now` 的有效 id。
    ///
    /// 返回后这些 id 已从权威表中移除，调用方驱动完应重新 `schedule`。
    pub fn pop_due(&mut self, now: Instant) -> Vec<String> {
        let mut due = Vec::new();
        // 注意: 不能用 `while let Some(..) = self.heap.peek()`, 因为循环体
        // 需要 `self.heap.pop()`, 与 peek 的不可变借用冲突。
        #[allow(clippy::while_let_loop)]
        loop {
            // 先只读地看一眼堆顶：到期才进入下面的 pop 分支。
            let Some(Reverse((top_deadline, _))) = self.heap.peek() else {
                break;
            };
            if *top_deadline > now {
                break;
            }

            // 堆顶已到期，取出并判断是否为有效条目。
            let Reverse((deadline, id)) = self.heap.pop().expect("peek 已确认非空");
            if self.current.get(&id) == Some(&deadline) {
                self.current.remove(&id);
                due.push(id);
            }
            // 否则是过期历史条目，直接丢弃。
        }
        due
    }

    /// 当前登记的定时器数量 (仅测试使用)。
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.current.len()
    }

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.current.is_empty()
    }
}

impl Default for TimerWheel {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn only_due_ids_are_returned() {
        let mut wheel = TimerWheel::new();
        let now = Instant::now();
        wheel.schedule("a", now + Duration::from_millis(10));
        wheel.schedule("b", now + Duration::from_millis(20));
        wheel.schedule("c", now + Duration::from_millis(30));

        // 尚未到期
        assert!(wheel.pop_due(now).is_empty());
        assert_eq!(wheel.next_deadline(), Some(now + Duration::from_millis(10)));

        // a 到期
        let due = wheel.pop_due(now + Duration::from_millis(10));
        assert_eq!(due, vec!["a".to_string()]);
        assert!(wheel.pop_due(now + Duration::from_millis(10)).is_empty());
        assert_eq!(wheel.next_deadline(), Some(now + Duration::from_millis(20)));

        // b、c 一起到期
        let mut due = wheel.pop_due(now + Duration::from_millis(100));
        due.sort();
        assert_eq!(due, vec!["b".to_string(), "c".to_string()]);
        assert!(wheel.is_empty());
        assert_eq!(wheel.next_deadline(), None);
    }

    #[test]
    fn reschedule_overrides_previous_deadline() {
        let mut wheel = TimerWheel::new();
        let now = Instant::now();
        wheel.schedule("a", now + Duration::from_millis(100));
        // 改到更早，应只在新的时刻到期一次
        wheel.schedule("a", now + Duration::from_millis(5));

        assert!(wheel
            .pop_due(now + Duration::from_millis(5))
            .contains(&"a".to_string()));
        // 原 100ms 的历史条目应被惰性丢弃，不再触发
        assert!(wheel.pop_due(now + Duration::from_millis(200)).is_empty());
        assert_eq!(wheel.len(), 0);
    }

    #[test]
    fn cancel_prevents_firing() {
        let mut wheel = TimerWheel::new();
        let now = Instant::now();
        wheel.schedule("a", now + Duration::from_millis(10));
        wheel.cancel("a");
        assert!(wheel.pop_due(now + Duration::from_millis(20)).is_empty());
        assert_eq!(wheel.next_deadline(), None);
    }
}
