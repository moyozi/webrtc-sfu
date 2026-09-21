//! RTP 路由引擎 — SFU 的核心转发逻辑
//!
//! 职责:
//! 1. 维护发布者→订阅者的路由表
//! 2. 将收到的 RTP 媒体数据转发给所有订阅者
//! 3. 管理 simulcast 层选择
//! 4. 处理订阅/取消订阅
//!
//! 性能关键: 媒体转发热路径 (`subscribers`) 不得分配内存。
//! 因此路由表按 `publisher_id → mid → 订阅者` 两层组织，用 `&str`/`Mid`
//! 直接查找，避免为每次查找构造 `TrackId { String, Mid }`。

use ahash::AHashMap;
use tracing::debug;

/// 使用 str0m 的 Mid 和 MediaKind 类型
pub use str0m::media::Mid;
pub use str0m::media::MediaKind;

/// 轨道标识: (发布者 ID, 发布者轨道 Mid)
///
/// 仅用于非热路径 (订阅反向索引)，热路径请使用两层 HashMap 直接查找。
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TrackId {
    pub publisher_id: String,
    pub mid: Mid,
}

/// 发布的轨道信息
#[derive(Debug, Clone)]
pub struct PublishedTrack {
    pub publisher_id: String,
    pub mid: Mid,
    pub kind: MediaKind,
    /// Simulcast 层 (RTP IDs)，空表示非 simulcast
    pub simulcast_rids: Vec<String>,
}

/// 订阅关系
#[derive(Debug, Clone)]
pub struct Subscription {
    pub subscriber_id: String,
    /// 订阅者在自己的 Rtc 中对应的轨道 Mid
    pub subscriber_mid: Mid,
    /// 选择的 simulcast 层 (None = 自动选择或非 simulcast)
    pub selected_layer: Option<String>,
    /// 订阅者所在 worker。== 本 worker 则本地送达, 否则需中继。
    pub worker_idx: usize,
}

/// 路由引擎
pub struct Router {
    /// 发布轨道表: publisher_id → (mid → 轨道信息)
    publishers: AHashMap<String, AHashMap<Mid, PublishedTrack>>,

    /// 订阅表: publisher_id → (mid → 订阅者列表)
    subscriptions: AHashMap<String, AHashMap<Mid, Vec<Subscription>>>,

    /// 反向索引: 订阅者 ID → 其订阅的所有轨道
    subscriber_index: AHashMap<String, Vec<TrackId>>,
}

impl Router {
    pub fn new() -> Self {
        Self {
            publishers: AHashMap::new(),
            subscriptions: AHashMap::new(),
            subscriber_index: AHashMap::new(),
        }
    }

    /// 注册发布者轨道
    ///
    /// 当参与者发布媒体时 (str0m Event::MediaAdded)，调用此方法
    pub fn add_publisher(
        &mut self,
        publisher_id: String,
        mid: Mid,
        kind: MediaKind,
    ) {
        info!(
            "注册发布轨道: {} mid={:?} kind={:?}",
            publisher_id, mid, kind
        );

        self.publishers
            .entry(publisher_id.clone())
            .or_default()
            .insert(
                mid,
                PublishedTrack {
                    publisher_id: publisher_id.clone(),
                    mid,
                    kind,
                    simulcast_rids: Vec::new(),
                },
            );

        // 初始化订阅列表 (保留先前可能已登记的订阅关系)
        self.subscriptions
            .entry(publisher_id)
            .or_default()
            .entry(mid)
            .or_default();
    }

    /// 添加订阅关系
    ///
    /// 当参与者 A 想订阅参与者 B 的轨道时调用。
    ///
    /// 注意: 订阅可能先于发布者的 `MediaAdded` 事件到达 (客户端收到
    /// participant_joined 后会立即发起 subscribe)。因此这里**不要求**发布轨道
    /// 已存在，先登记订阅关系；之后 `add_publisher()` 会复用同一条目。
    pub fn add_subscription(
        &mut self,
        subscriber_id: &str,
        publisher_id: &str,
        publisher_mid: &str,
        subscriber_mid: Option<&str>,
        worker_idx: usize,
    ) {
        let publisher_key = Mid::from(publisher_mid);
        // 订阅者自身该轨道的 mid；缺省退化为发布者 mid，兼容浏览器 "0"/"1" 约定。
        let subscriber_mid = subscriber_mid.map(Mid::from).unwrap_or(publisher_key);

        let subs = self
            .subscriptions
            .entry(publisher_id.to_string())
            .or_default()
            .entry(publisher_key)
            .or_default();

        // 去重: 同一订阅者不要重复订阅同一轨道
        if subs.iter().any(|s| s.subscriber_id == subscriber_id) {
            debug!(
                "订阅已存在: {} → {}:{}",
                subscriber_id, publisher_id, publisher_mid
            );
            return;
        }

        subs.push(Subscription {
            subscriber_id: subscriber_id.to_string(),
            subscriber_mid,
            selected_layer: None,
            worker_idx,
        });

        // 更新反向索引
        self.subscriber_index
            .entry(subscriber_id.to_string())
            .or_default()
            .push(TrackId {
                publisher_id: publisher_id.to_string(),
                mid: publisher_key,
            });

        debug!(
            "添加订阅: {} → {}:{}",
            subscriber_id, publisher_id, publisher_mid
        );
    }

    /// 移除订阅关系
    pub fn remove_subscription(
        &mut self,
        subscriber_id: &str,
        publisher_id: &str,
        publisher_mid: &str,
    ) {
        let mid = Mid::from(publisher_mid);

        if let Some(mids) = self.subscriptions.get_mut(publisher_id) {
            if let Some(subs) = mids.get_mut(&mid) {
                subs.retain(|s| s.subscriber_id != subscriber_id);
                if subs.is_empty() {
                    mids.remove(&mid);
                }
            }
        }
        // 清理空的发布者条目
        let empty = self
            .subscriptions
            .get(publisher_id)
            .map(|m| m.is_empty())
            .unwrap_or(false);
        if empty {
            self.subscriptions.remove(publisher_id);
        }

        let track_id = TrackId {
            publisher_id: publisher_id.to_string(),
            mid,
        };
        if let Some(tracks) = self.subscriber_index.get_mut(subscriber_id) {
            tracks.retain(|t| t != &track_id);
        }
    }

    /// 移除参与者的所有发布和订阅
    pub fn remove_participant(&mut self, participant_id: &str) {
        // 该参与者发布的所有轨道 (以及挂在轨道上的订阅) 整体移除
        self.publishers.remove(participant_id);
        self.subscriptions.remove(participant_id);

        // 反向索引
        self.subscriber_index.remove(participant_id);

        // 从其他人的订阅列表中移除该参与者 (作为订阅者)
        for mids in self.subscriptions.values_mut() {
            mids.retain(|_, subs| {
                subs.retain(|s| s.subscriber_id != participant_id);
                !subs.is_empty()
            });
        }
    }

    /// 获取指定发布轨道的订阅者列表 (热路径, 零分配)
    ///
    /// 返回的切片借用 `self`，调用方可在同一函数内同时可变借用
    /// 参与者表 (两者为不同变量)，从而无需克隆订阅者 ID。
    pub fn subscribers(&self, publisher_id: &str, mid: Mid) -> &[Subscription] {
        self.subscriptions
            .get(publisher_id)
            .and_then(|mids| mids.get(&mid))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// 反向查找: 给定订阅者及其在自身 Rtc 中的 mid, 找出对应的发布轨道。
    ///
    /// 订阅者发来的 PLI/FIR 携带的是订阅者自己的 mid，需要据此找到
    /// 对应的 (publisher_id, publisher_mid) 才能回传给正确的发布者。
    /// 非热路径 (关键帧请求不频繁)，返回 owned Vec 以避免借用纠缠。
    pub fn publisher_for_subscriber_mid(
        &self,
        subscriber_id: &str,
        subscriber_mid: Mid,
    ) -> Vec<(String, Mid)> {
        let mut out = Vec::new();
        let Some(tracks) = self.subscriber_index.get(subscriber_id) else {
            return out;
        };
        for track in tracks {
            let Some(mids) = self.subscriptions.get(&track.publisher_id) else {
                continue;
            };
            let Some(subs) = mids.get(&track.mid) else {
                continue;
            };
            if subs.iter().any(|s| {
                s.subscriber_id == subscriber_id && s.subscriber_mid == subscriber_mid
            }) {
                out.push((track.publisher_id.clone(), track.mid));
            }
        }
        out
    }

    /// 统计信息
    pub fn stats(&self) -> RouterStats {
        RouterStats {
            published_tracks: self.publishers.values().map(|m| m.len()).sum(),
            total_subscriptions: self
                .subscriptions
                .values()
                .flat_map(|m| m.values())
                .map(|v| v.len())
                .sum(),
        }
    }
}

/// 路由器统计
#[derive(Debug)]
pub struct RouterStats {
    pub published_tracks: usize,
    pub total_subscriptions: usize,
}

// 导入日志宏
use tracing::info;

#[cfg(test)]
mod tests {
    use super::*;

    /// 订阅可能先于发布者的 MediaAdded 事件到达, 必须保留订阅关系。
    #[test]
    fn subscription_before_publisher_is_preserved() {
        let mut router = Router::new();

        // 发布者尚未注册 (MediaAdded 未到达) 时先订阅
        router.add_subscription("sub-1", "pub-1", "0", None, 0);
        assert_eq!(router.subscribers("pub-1", Mid::from("0")).len(), 1);

        // 发布者随后注册, 订阅关系应保留
        router.add_publisher("pub-1".to_string(), Mid::from("0"), MediaKind::Audio);
        assert_eq!(router.subscribers("pub-1", Mid::from("0")).len(), 1);

        // 重复订阅应去重
        router.add_subscription("sub-1", "pub-1", "0", None, 0);
        assert_eq!(router.subscribers("pub-1", Mid::from("0")).len(), 1);
    }

    /// 移除参与者后，其订阅不应再被转发。
    #[test]
    fn remove_participant_cleans_subscriptions() {
        let mut router = Router::new();
        router.add_publisher("pub-1".to_string(), Mid::from("0"), MediaKind::Video);
        router.add_subscription("sub-1", "pub-1", "0", None, 0);
        router.add_subscription("sub-2", "pub-1", "0", None, 0);
        router.remove_participant("sub-1");
        let subs = router.subscribers("pub-1", Mid::from("0"));
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].subscriber_id, "sub-2");

        // 移除发布者后轨道消失
        router.remove_participant("pub-1");
        assert_eq!(router.subscribers("pub-1", Mid::from("0")).len(), 0);
    }

    /// 订阅者自身 mid 与发布者不同时应被原样保存 (不再假定两者相同)。
    #[test]
    fn subscriber_mid_is_stored_independently() {
        let mut router = Router::new();
        router.add_publisher("pub-1".to_string(), Mid::from("pubA"), MediaKind::Video);

        // 未提供 subscriber_mid 时退化为发布者 mid
        router.add_subscription("sub-legacy", "pub-1", "pubA", None, 0);
        // 显式提供不同的 subscriber_mid
        router.add_subscription("sub-1", "pub-1", "pubA", Some("subA"), 0);
        let subs = router.subscribers("pub-1", Mid::from("pubA"));
        assert_eq!(subs.len(), 2);
        let legacy = subs.iter().find(|s| s.subscriber_id == "sub-legacy").unwrap();
        assert_eq!(legacy.subscriber_mid, Mid::from("pubA"));
        let explicit = subs.iter().find(|s| s.subscriber_id == "sub-1").unwrap();
        assert_eq!(explicit.subscriber_mid, Mid::from("subA"));
    }

    /// PLI/FIR 反向映射: 由订阅者自己的 mid 找到发布轨道。
    #[test]
    fn reverse_lookup_publisher_by_subscriber_mid() {
        let mut router = Router::new();
        router.add_publisher("pub-1".to_string(), Mid::from("p0"), MediaKind::Video);
        router.add_subscription("sub-1", "pub-1", "p0", Some("s0"), 0);
        let targets = router.publisher_for_subscriber_mid("sub-1", Mid::from("s0"));
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].0, "pub-1");
        assert_eq!(targets[0].1, Mid::from("p0"));

        // 未知 subscriber_mid / 未知订阅者均无结果。
        assert!(router
            .publisher_for_subscriber_mid("sub-1", Mid::from("sX"))
            .is_empty());
        assert!(router
            .publisher_for_subscriber_mid("sub-2", Mid::from("s0"))
            .is_empty());
    }
}
