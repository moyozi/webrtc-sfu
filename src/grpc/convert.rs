//! 进程内 `WorkerCommand` ↔ gRPC `Command` 转换。

use crate::grpc::pb;
use crate::worker::WorkerCommand;
use str0m::media::Rid;

/// 把需要跨服务发送的 `WorkerCommand` 转成 gRPC 命令。
///
/// `CreateParticipant` 走专用 RPC, `Shutdown` 仅本地有效, 均返回 `None`。
pub fn command_to_proto(cmd: WorkerCommand) -> Option<pb::Command> {
    let kind = match cmd {
        WorkerCommand::AddRemoteCandidate {
            participant_id,
            candidate,
            sdp_mid,
        } => pb::command::Kind::AddRemoteCandidate(pb::AddRemoteCandidate {
            participant_id,
            candidate,
            sdp_mid,
        }),
        WorkerCommand::RemoveParticipant { participant_id } => {
            pb::command::Kind::RemoveParticipant(pb::RemoveParticipant { participant_id })
        }
        WorkerCommand::Subscribe {
            room_id,
            subscriber_id,
            publisher_id,
            publisher_mid,
            subscriber_mid,
            publisher_worker,
        } => pb::command::Kind::Subscribe(pb::Subscribe {
            room_id,
            subscriber_id,
            publisher_id,
            publisher_mid,
            subscriber_mid,
            publisher_worker: publisher_worker as u32,
        }),
        WorkerCommand::Unsubscribe {
            subscriber_id,
            publisher_id,
            publisher_mid,
            publisher_worker,
        } => pb::command::Kind::Unsubscribe(pb::Unsubscribe {
            subscriber_id,
            publisher_id,
            publisher_mid,
            publisher_worker: publisher_worker as u32,
        }),
        WorkerCommand::RegisterRelaySubscriber {
            publisher_id,
            publisher_mid,
            subscriber_worker,
            subscriber_id,
            subscriber_mid,
        } => pb::command::Kind::RegisterRelaySubscriber(pb::RegisterRelaySubscriber {
            publisher_id,
            publisher_mid,
            subscriber_worker: subscriber_worker as u32,
            subscriber_id,
            subscriber_mid,
        }),
        WorkerCommand::UnregisterRelaySubscriber {
            publisher_id,
            publisher_mid,
            subscriber_id,
        } => pb::command::Kind::UnregisterRelaySubscriber(pb::UnregisterRelaySubscriber {
            publisher_id,
            publisher_mid,
            subscriber_id,
        }),
        WorkerCommand::RequestKeyframe {
            publisher_id,
            publisher_mid,
            rid,
        } => pb::command::Kind::RequestKeyframe(pb::RequestKeyframe {
            publisher_id,
            publisher_mid,
            rid: rid.map(|r| r.to_string()),
        }),
        WorkerCommand::CreateParticipant { .. }
        | WorkerCommand::Renegotiate { .. }
        | WorkerCommand::GetPublisherTracks { .. }
        | WorkerCommand::Shutdown => return None,
    };
    Some(pb::Command { kind: Some(kind) })
}

/// 把 gRPC 命令还原为本地 `WorkerCommand`。
pub fn proto_to_command(cmd: pb::Command) -> Option<WorkerCommand> {
    let kind = cmd.kind?;
    Some(match kind {
        pb::command::Kind::AddRemoteCandidate(m) => WorkerCommand::AddRemoteCandidate {
            participant_id: m.participant_id,
            candidate: m.candidate,
            sdp_mid: m.sdp_mid,
        },
        pb::command::Kind::RemoveParticipant(m) => {
            WorkerCommand::RemoveParticipant {
                participant_id: m.participant_id,
            }
        }
        pb::command::Kind::Subscribe(m) => WorkerCommand::Subscribe {
            room_id: m.room_id,
            subscriber_id: m.subscriber_id,
            publisher_id: m.publisher_id,
            publisher_mid: m.publisher_mid,
            subscriber_mid: m.subscriber_mid,
            publisher_worker: m.publisher_worker as usize,
        },
        pb::command::Kind::Unsubscribe(m) => WorkerCommand::Unsubscribe {
            subscriber_id: m.subscriber_id,
            publisher_id: m.publisher_id,
            publisher_mid: m.publisher_mid,
            publisher_worker: m.publisher_worker as usize,
        },
        pb::command::Kind::RegisterRelaySubscriber(m) => WorkerCommand::RegisterRelaySubscriber {
            publisher_id: m.publisher_id,
            publisher_mid: m.publisher_mid,
            subscriber_worker: m.subscriber_worker as usize,
            subscriber_id: m.subscriber_id,
            subscriber_mid: m.subscriber_mid,
        },
        pb::command::Kind::UnregisterRelaySubscriber(m) => {
            WorkerCommand::UnregisterRelaySubscriber {
                publisher_id: m.publisher_id,
                publisher_mid: m.publisher_mid,
                subscriber_id: m.subscriber_id,
            }
        }
        pb::command::Kind::RequestKeyframe(m) => WorkerCommand::RequestKeyframe {
            publisher_id: m.publisher_id,
            publisher_mid: m.publisher_mid,
            rid: m.rid.map(|r| Rid::from(r.as_str())),
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_roundtrip() {
        let cmd = WorkerCommand::RegisterRelaySubscriber {
            publisher_id: "bob".into(),
            publisher_mid: "1".into(),
            subscriber_worker: 3,
            subscriber_id: "alice-recv-bob".into(),
            subscriber_mid: "0".into(),
        };
        let proto = command_to_proto(cmd).unwrap();
        let back = proto_to_command(proto).unwrap();
        match back {
            WorkerCommand::RegisterRelaySubscriber {
                subscriber_worker,
                subscriber_id,
                subscriber_mid,
                ..
            } => {
                assert_eq!(subscriber_worker, 3);
                assert_eq!(subscriber_id, "alice-recv-bob");
                assert_eq!(subscriber_mid, "0");
            }
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn request_keyframe_rid_roundtrip() {
        let cmd = WorkerCommand::RequestKeyframe {
            publisher_id: "p".into(),
            publisher_mid: "0".into(),
            rid: Some(Rid::from("h")),
        };
        let back = proto_to_command(command_to_proto(cmd).unwrap()).unwrap();
        match back {
            WorkerCommand::RequestKeyframe { rid, .. } => {
                assert_eq!(rid, Some(Rid::from("h")));
            }
            _ => panic!("unexpected variant"),
        }
    }

    #[test]
    fn create_and_shutdown_not_serializable() {
        let (tx, _rx) = crossbeam_channel::bounded(1);
        assert!(command_to_proto(WorkerCommand::CreateParticipant {
            room_id: "r".into(),
            participant_id: "p".into(),
            sdp_offer: "v=0".into(),
            reply_tx: tx,
        })
        .is_none());
    }
}
