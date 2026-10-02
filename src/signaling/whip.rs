//! WHIP 推流接入 (WebRTC-HTTP Ingestion Protocol)
//!
//! 让 FFmpeg / GStreamer / OBS 等直接以 WebRTC 推流进 SFU:
//!
//! ```text
//!   ffmpeg ... -f whip http://host:5180/whip?room=demo
//!        │  POST application/sdp (offer, 含 ICE 候选)
//!        ▼
//!   本模块 ── create_participant ──► Worker (Rtp publish, 复用现有链路)
//!        │  201 + application/sdp (answer, 注入 SFU host 候选)
//!        ▼
//!   ffmpeg 自行完成 ICE + DTLS + SRTP, RTP 到达 worker 媒体端口
//! ```
//!
//! 复用现有发布者路径: WHIP 推流者在 Router 里就是一个普通 publisher,
//! 房间内 WebSocket 订阅者照常 `subscribe` 即可。
//!
//! 支持: `OPTIONS`(CORS) / `POST /whip?room=...`(建流) /
//! `PATCH /whip/{id}`(trickle ICE) / `DELETE /whip/{id}`(停止)。

use crate::signaling::auth::extract_token;
use crate::signaling::SignalingState;
use crate::worker::WorkerCommand;
use ahash::AHashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

use super::http::{
    inject_candidates, parse_candidates, parse_head, query_param, read_request, write_response,
};

struct WhipSession {
    participant_id: String,
    room_id: String,
    worker_idx: usize,
}

/// 生成一个不会与 WS 侧冲突的 participant id。
fn new_id(seq: u64) -> String {
    format!("whip-{}", seq)
}

/// 启动 WHIP ingrest HTTP 服务。
pub async fn run_server(addr: std::net::SocketAddr, state: Arc<SignalingState>) -> io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("WHIP 推流接入监听: http://{}", addr);
    let sessions: Arc<parking_lot::Mutex<AHashMap<String, WhipSession>>> =
        Arc::new(parking_lot::Mutex::new(AHashMap::new()));
    let seq = Arc::new(AtomicU64::new(1));

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                warn!("WHIP accept 失败: {}", e);
                continue;
            }
        };
        let state = state.clone();
        let sessions = sessions.clone();
        let seq = seq.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(stream, state, sessions, seq).await {
                tracing::debug!("WHIP 连接结束: {} ({})", peer, e);
            }
        });
    }
}

async fn handle(
    mut stream: TcpStream,
    state: Arc<SignalingState>,
    sessions: Arc<parking_lot::Mutex<AHashMap<String, WhipSession>>>,
    seq: Arc<AtomicU64>,
) -> io::Result<()> {
    let req = match read_request(&mut stream).await? {
        Some(r) => r,
        None => return Ok(()),
    };
    let method = req.method.clone();
    let (path, query) = parse_head(&req);

    // OPTIONS 为 CORS 预检, 不携带业务鉴权信息, 放行。
    if method == "OPTIONS" {
        return write_response(&mut stream, "204 No Content", "text/plain", &[], b"").await;
    }

    // 启用鉴权时, POST/PATCH/DELETE 均需携带有效 JWT。
    let auth = state.auth.clone();
    if auth.is_enabled() {
        let token = extract_token(req.header("authorization"), Some(query.as_str()));
        let verified = token.as_deref().map(|t| auth.verify(t));
        match verified {
            Some(Ok(_)) => {}
            Some(Err(e)) => {
                warn!("WHIP 鉴权失败: {}", e);
                return write_response(
                    &mut stream,
                    "401 Unauthorized",
                    "text/plain",
                    &[],
                    e.as_bytes(),
                )
                .await;
            }
            None if auth.is_required() => {
                return write_response(
                    &mut stream,
                    "401 Unauthorized",
                    "text/plain",
                    &[],
                    b"missing bearer token",
                )
                .await;
            }
            None => {}
        }
    }

    match method.as_str() {
        "POST" if path == "/whip" => {
            let room = query_param(&query, "room").unwrap_or_else(|| "default".to_string());
            let sdp_offer = String::from_utf8_lossy(&req.body).to_string();
            if sdp_offer.trim().is_empty() {
                return write_response(
                    &mut stream,
                    "400 Bad Request",
                    "text/plain",
                    &[],
                    b"empty sdp",
                )
                .await;
            }

            let id = new_id(seq.fetch_add(1, Ordering::Relaxed));
            let worker_idx = state.assign_worker(&room, &id);
            // 提取推流轨道列表 (供订阅者逐 track 订阅)。
            let tracks: Vec<crate::signaling::protocol::TrackInfo> =
                crate::signaling::http::offer_mids(&sdp_offer)
                    .into_iter()
                    .map(|(kind, mid)| crate::signaling::protocol::TrackInfo { mid, kind })
                    .collect();
            // create_participant 可能阻塞等待 worker 回复 (最长 10s)，放到 blocking 线程池。
            let state_for_task = state.clone();
            let room_for_task = room.clone();
            let id_for_task = id.clone();
            let reply = tokio::task::spawn_blocking(move || {
                state_for_task.create_participant(worker_idx, room_for_task, id_for_task, sdp_offer)
            })
            .await
            .unwrap_or(None);

            match reply {
                Some(crate::worker::WorkerReply::SdpAnswer {
                    sdp_answer,
                    local_candidates,
                    ..
                }) => {
                    // 作为发布者加入房间 (使 WS 订阅者可发现)
                    state.join_room(&room, &id, tracks.clone());
                    state.notify_room(
                        &room,
                        &id,
                        &serde_json::to_string(
                            &crate::signaling::protocol::SignalingResponse::ParticipantJoined {
                                participant_id: id.clone(),
                                tracks: tracks.clone(),
                            },
                        )
                        .unwrap(),
                    );
                    sessions.lock().insert(
                        id.clone(),
                        WhipSession {
                            participant_id: id.clone(),
                            room_id: room.clone(),
                            worker_idx,
                        },
                    );
                    let answer = inject_candidates(&sdp_answer, &local_candidates);
                    info!(participant = %id, room = %room, "WHIP 推流建立, worker={}", worker_idx);
                    let location = format!("/whip/{}", id);
                    write_response(
                        &mut stream,
                        "201 Created",
                        "application/sdp",
                        &[
                            ("Location", location.as_str()),
                            ("X-Participant-Id", id.as_str()),
                        ],
                        answer.as_bytes(),
                    )
                    .await
                }
                Some(crate::worker::WorkerReply::Error { error, .. }) => {
                    warn!("WHIP 建流失败: {}", error);
                    write_response(
                        &mut stream,
                        "500 Internal Server Error",
                        "text/plain",
                        &[],
                        error.as_bytes(),
                    )
                    .await
                }
                Some(_) => {
                    write_response(
                        &mut stream,
                        "500 Internal Server Error",
                        "text/plain",
                        &[],
                        b"unexpected worker reply",
                    )
                    .await
                }
                None => {
                    write_response(
                        &mut stream,
                        "503 Service Unavailable",
                        "text/plain",
                        &[],
                        b"worker unavailable",
                    )
                    .await
                }
            }
        }
        "PATCH" if path.starts_with("/whip/") => {
            let id = path.trim_start_matches("/whip/").to_string();
            let session = sessions
                .lock()
                .get(&id)
                .map(|s| (s.participant_id.clone(), s.worker_idx));
            let Some((participant_id, worker_idx)) = session else {
                return write_response(
                    &mut stream,
                    "404 Not Found",
                    "text/plain",
                    &[],
                    b"no such session",
                )
                .await;
            };
            let frag = String::from_utf8_lossy(&req.body);
            for (mid, candidate) in parse_candidates(&frag) {
                state.send_worker(
                    worker_idx,
                    WorkerCommand::AddRemoteCandidate {
                        participant_id: participant_id.clone(),
                        candidate,
                        sdp_mid: mid,
                    },
                );
            }
            write_response(&mut stream, "204 No Content", "text/plain", &[], b"").await
        }
        "DELETE" if path.starts_with("/whip/") => {
            let id = path.trim_start_matches("/whip/").to_string();
            let session = sessions.lock().remove(&id);
            if let Some(s) = session {
                state.leave_room(&s.room_id, &s.participant_id);
                state.release_worker(&s.room_id, &s.participant_id);
                let notify = serde_json::to_string(
                    &crate::signaling::protocol::SignalingResponse::ParticipantLeft {
                        participant_id: s.participant_id.clone(),
                    },
                )
                .unwrap();
                state.notify_room(&s.room_id, &s.participant_id, &notify);
                state.send_worker(
                    s.worker_idx,
                    WorkerCommand::RemoveParticipant {
                        participant_id: s.participant_id.clone(),
                    },
                );
                info!(participant = %s.participant_id, "WHIP 推流停止");
            }
            write_response(&mut stream, "200 OK", "text/plain", &[], b"").await
        }
        _ => {
            write_response(
                &mut stream,
                "404 Not Found",
                "text/plain",
                &[],
                b"not found",
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inject_candidates_after_ice_pwd() {
        let sdp = "v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=mid:0\r\na=ice-ufrag:xx\r\na=ice-pwd:yy\r\n";
        let out = inject_candidates(sdp, &["candidate:1 1 udp 2 127.0.0.1 5175 typ host".into()]);
        assert!(out.contains("a=ice-pwd:yy\r\na=candidate:1 1 udp 2 127.0.0.1 5175 typ host\r\n"));
    }

    #[test]
    fn parse_trickle_fragment() {
        let frag = "a=ice-ufrag:xx\r\na=mid:0\r\na=candidate:1 1 udp 2 1.2.3.4 9 typ host\r\n";
        let c = parse_candidates(frag);
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].0.as_deref(), Some("0"));
        assert!(c[0].1.starts_with("candidate:"));
    }

    #[test]
    fn query_parsing() {
        assert_eq!(
            query_param("room=demo&x=1", "room").as_deref(),
            Some("demo")
        );
        assert_eq!(query_param("room=demo", "missing"), None);
    }
}
