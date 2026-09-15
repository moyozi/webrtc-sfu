//! RTP 路由引擎 — SFU 的核心转发逻辑
//!
//! 职责:
//! 1. 维护发布者→订阅者的路由表
//! 2. 将收到的 RTP 媒体数据转发给所有订阅者
//! 3. 管理 simulcast 层选择
//! 4. 处理订阅/取消订阅

use crate::net::udp::SendBatch;
use ahash::AHashMap;
use tracing::debug;

/// 使用 str0m 的 Mid 和 MediaKind 类型
pub use str0m::media::Mid;
pub use str0m::media::MediaKind;

/// 轨道标识: (发布者 ID, 发布者轨道 Mid)
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
}

/// 路由引擎
pub struct Router {
    /// 发布轨道表: TrackId → 轨道信息
    publishers: AHashMap<TrackId, PublishedTrack>,

    /// 订阅表: 发布轨道 TrackId → 订阅者列表
    subscriptions: AHashMap<TrackId, Vec<Subscription>>,

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
        let track_id = TrackId {
            publisher_id: publisher_id.clone(),
            mid,
        };

        info!(
            "注册发布轨道: {} mid={:?} kind={:?}",
            publisher_id, mid, kind
        );

        self.publishers.insert(
            track_id.clone(),
            PublishedTrack {
                publisher_id,
                mid,
                kind,
                simulcast_rids: Vec::new(),
            },
        );

        // 初始化订阅列表
        self.subscriptions.entry(track_id).or_default();
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
    ) {
        // 解析 Mid (str0m Mid 实现了 From<&str>)
        let mid = Mid::from(publisher_mid);

        let track_id = TrackId {
            publisher_id: publisher_id.to_string(),
            mid,
        };

        // 如果发布轨道已经注册，则无需二次确认；未注册也允许先订阅。
        let subs = self.subscriptions.entry(track_id.clone()).or_default();

        // 去重: 同一订阅者不要重复订阅同一轨道
        if subs.iter().any(|s| s.subscriber_id == subscriber_id) {
            debug!(
                "订阅已存在: {} → {}:{}",
                subscriber_id, publisher_id, publisher_mid
            );
            return;
        }

        // 添加到订阅列表
        // 注意: subscriber_mid 在实际实现中需要通过 SDP 协商确定
        // 这里简化处理，使用与发布者相同的 mid
        subs.push(Subscription {
            subscriber_id: subscriber_id.to_string(),
            subscriber_mid: mid,
            selected_layer: None,
        });

        // 更新反向索引
        self.subscriber_index
            .entry(subscriber_id.to_string())
            .or_default()
            .push(track_id);

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

        let track_id = TrackId {
            publisher_id: publisher_id.to_string(),
            mid,
        };

        if let Some(subs) = self.subscriptions.get_mut(&track_id) {
            subs.retain(|s| s.subscriber_id != subscriber_id);
        }

        if let Some(tracks) = self.subscriber_index.get_mut(subscriber_id) {
            tracks.retain(|t| t != &track_id);
        }
    }

    /// 移除参与者的所有发布和订阅
    pub fn remove_participant(&mut self, participant_id: &str) {
        // 移除其发布轨道
        let pub_keys: Vec<_> = self.publishers
            .keys()
            .filter(|k| k.publisher_id == participant_id)
            .cloned()
            .collect();

        for key in &pub_keys {
            self.publishers.remove(key);
            self.subscriptions.remove(key);
        }

        // 移除其订阅
        if let Some(tracks) = self.subscriber_index.remove(participant_id) {
            for track_id in &tracks {
                if let Some(subs) = self.subscriptions.get_mut(track_id) {
                    subs.retain(|s| s.subscriber_id != participant_id);
                }
            }
        }

        // 从其他人的订阅列表中移除该参与者
        for subs in self.subscriptions.values_mut() {
            subs.retain(|s| s.subscriber_id != participant_id);
        }
    }

    /// 转发媒体数据 — SFU 的核心操作
    ///
    /// 当发布者收到 RTP 媒体数据时，将其转发给所有订阅者。
    ///
    /// 流程:
    /// 1. 查找轨道的订阅者列表
    /// 2. 对每个订阅者:
    ///    a. 获取订阅者 Rtc 的 media writer
    ///    b. 写入媒体数据 (str0m 内部处理 SRTP 加密 + RTP 打包)
    ///    c. Drain 订阅者的 poll_output，收集 Transmit 包
    ///
    /// 注意: 由于 Rust 所有权限制，实际的参与者访问需要通过 Worker 传入。
    /// 这里只做订阅者查找，实际的写入操作在 Worker 中完成。
    pub fn forward_media(
        &mut self,
        publisher_id: &str,
        mid: Mid,
        pt: u8,
        _time: str0m::media::MediaTime,
        data: &[u8],
        _send_batch: &mut SendBatch,
    ) {
        let track_id = TrackId {
            publisher_id: publisher_id.to_string(),
            mid,
        };

        // 查找订阅者
        let subs = match self.subscriptions.get(&track_id) {
            Some(s) if !s.is_empty() => s.clone(),
            _ => return, // 没有订阅者，不需要转发
        };

        debug!(
            "转发媒体: publisher={} mid={:?} pt={} bytes={} subscribers={}",
            publisher_id, mid, pt, data.len(), subs.len()
        );

        // 注意: 在实际实现中，这里需要访问订阅者的 Rtc 实例。
        // 由于 participants HashMap 和 Router 都在 Worker 中，
        // Worker 需要协调两者的访问。
        //
        // 正确的做法是在 Worker 中实现转发逻辑:
        // 1. Router.forward_media() 返回订阅者列表
        // 2. Worker 遍历订阅者，访问其 Participant.rtc.writer(mid)
        // 3. Worker drain 订阅者的 poll_output

        // 这里提供一个回调式接口的占位实现
        // 实际转发逻辑在 Worker::handle_event 中完成
    }

    /// 获取指定发布轨道的订阅者列表
    ///
    /// Worker 使用此方法获取订阅者，然后直接访问参与者进行转发
    pub fn get_subscribers(&self, publisher_id: &str, mid: Mid) -> Vec<&Subscription> {
        let track_id = TrackId {
            publisher_id: publisher_id.to_string(),
            mid,
        };

        self.subscriptions
            .get(&track_id)
            .map(|s| s.iter().collect())
            .unwrap_or_default()
    }

    /// 统计信息
    pub fn stats(&self) -> RouterStats {
        RouterStats {
            published_tracks: self.publishers.len(),
            total_subscriptions: self.subscriptions.values().map(|s| s.len()).sum(),
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
        router.add_subscription("sub-1", "pub-1", "0");
        assert_eq!(router.get_subscribers("pub-1", Mid::from("0")).len(), 1);

        // 发布者随后注册, 订阅关系应保留
        router.add_publisher("pub-1".to_string(), Mid::from("0"), MediaKind::Audio);
        assert_eq!(router.get_subscribers("pub-1", Mid::from("0")).len(), 1);

        // 重复订阅应去重
        router.add_subscription("sub-1", "pub-1", "0");
        assert_eq!(router.get_subscribers("pub-1", Mid::from("0")).len(), 1);
    }
}
