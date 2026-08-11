//! 房间管理 — 多参与者房间
//!
//! 房间是一组参与者的集合，房间内的参与者可以互相订阅媒体轨道。

use std::collections::HashSet;

/// 房间
pub struct Room {
    /// 房间 ID
    pub id: String,
    /// 房间内的参与者 ID 集合
    pub participants: HashSet<String>,
    /// 最大参与者数 (0 = 无限制)
    pub max_participants: usize,
}

impl Room {
    pub fn new(id: String, max_participants: usize) -> Self {
        Self {
            id,
            participants: HashSet::new(),
            max_participants,
        }
    }

    /// 添加参与者到房间
    pub fn add_participant(&mut self, participant_id: &str) -> bool {
        if self.max_participants > 0 && self.participants.len() >= self.max_participants {
            return false;
        }
        self.participants.insert(participant_id.to_string())
    }

    /// 从房间移除参与者
    pub fn remove_participant(&mut self, participant_id: &str) -> bool {
        self.participants.remove(participant_id)
    }

    /// 房间是否为空
    pub fn is_empty(&self) -> bool {
        self.participants.is_empty()
    }

    /// 获取房间内所有其他参与者 (排除自己)
    pub fn other_participants(&self, exclude: &str) -> Vec<&String> {
        self.participants
            .iter()
            .filter(|p| p.as_str() != exclude)
            .collect()
    }

    /// 参与者数量
    pub fn len(&self) -> usize {
        self.participants.len()
    }
}
