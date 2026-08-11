//! 定时器轮 — 管理多个 Rtc 实例的超时
//!
//! 使用分层时间轮 (Hierarchical Timing Wheel) 来高效管理大量定时器。
//! 对于 SFU 场景，主要的定时器来源是 str0m 的 Output::Timeout。

use std::time::{Duration, Instant};

/// 定时器回调类型
pub type TimerCallback = Box<dyn FnMut() + Send>;

/// 单个定时器条目
struct TimerEntry {
    /// 到期时间
    deadline: Instant,
    /// 回调函数
    callback: TimerCallback,
}

/// 简单的定时器管理器 (基于最小堆)
///
/// 对于 SFU 场景，定时器数量不多 (每个 Rtc 实例 1-2 个)，
/// 最小堆的 O(log n) 复杂度足够。
pub struct TimerWheel {
    entries: Vec<TimerEntry>,
}

impl TimerWheel {
    pub fn new() -> Self {
        Self {
            entries: Vec::with_capacity(256),
        }
    }

    /// 添加定时器
    pub fn schedule(&mut self, delay: Duration, callback: TimerCallback) {
        let deadline = Instant::now() + delay;
        self.entries.push(TimerEntry { deadline, callback });

        // 维持堆性质 (按 deadline 排序)
        self.entries.sort_by(|a, b| a.deadline.cmp(&b.deadline));
    }

    /// 处理所有已到期的定时器
    ///
    /// 返回距离下一个定时器的时间
    pub fn tick(&mut self) -> Option<Duration> {
        let now = Instant::now();

        // 执行所有已到期的定时器
        let mut i = 0;
        while i < self.entries.len() {
            if self.entries[i].deadline <= now {
                let mut entry = self.entries.swap_remove(i);
                (entry.callback)();
            } else {
                i += 1;
            }
        }

        // 返回下一个定时器的等待时间
        self.entries.first().map(|e| {
            let dur = e.deadline.saturating_duration_since(now);
            if dur.is_zero() { Duration::from_millis(1) } else { dur }
        })
    }

    /// 定时器数量
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}
