//! WHEP 拉流接入 (WebRTC-HTTP Egress Protocol)
//!
//! 让浏览器 / 播放器以标准 WHEP 从 SFU 拉取某发布者的音视频:
//!
//! ```text
//!   player ── POST /whep?room=demo&publisher=alice (SDP offer, recvonly) ──►
//!          ◄── 201 application/sdp (answer, 注入 SFU host 候选) ───────────
//!   本模块 ── create_participant(subscriber) + GetPublisherTracks ──► Worker
//!          ── Subscribe(publisher_mid → subscriber_mid) ──► Worker
//! ```
//!
//! WHEP 是 WHIP 的对称能力: WHIP 负责推流(ingest), WHEP 负责拉流(egress)。
//! 这里把 WHEP 客户端当作一个普通**订阅者**: 创建 recvonly 的 Rtc, 再按
//! 客户端 offer 的 m-section 与发布者实际轨道按 kind 匹配后发起 subscribe。
//!
//! 支持: `OPTIONS`(CORS) / `POST /whep?room=...&publisher=...` /
//! `PATCH /whep/{id}`(trickle ICE) / `DELETE /whep/{id}`(停止)。

use super::http::{
    inject_candidates, offer_mids, parse_candidates, parse_head, query_param, read_request,
    write_response,
};
use crate::signaling::auth::extract_token;
use crate::signaling::SignalingState;
use crate::worker::WorkerCommand;
use ahash::AHashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

struct WhepSession {
    participant_id: String,
    room_id: String,
    worker_idx: usize,
}

/// 生成一个不会与其它信令侧冲突的 participant id。
fn new_id(seq: u64) -> String {
    format!("whep-{}", seq)
}

/// 按 kind 把发布者轨道匹配到订阅者 offer 里的 recv mid。
///
/// `recv` 为 offer 中按出现顺序排列的 (kind, mid); 返回 (publisher_mid, subscriber_mid) 列表。
fn match_tracks(
    publisher_tracks: &[(String, String)],
    recv: &[(String, String)],
) -> Vec<(String, String)> {
    let mut remaining: Vec<(String, String)> = recv.to_vec();
    let mut out = Vec::new();
    for (pub_mid, kind) in publisher_tracks {
        if let Some(pos) = remaining.iter().position(|(k, _)| k == kind) {
            let (_, sub_mid) = remaining.remove(pos);
            out.push((pub_mid.clone(), sub_mid));
        }
    }
    out
}

/// 启动 WHEP 拉流 HTTP 服务。
pub async fn run_server(addr: std::net::SocketAddr, state: Arc<SignalingState>) -> io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("WHEP 拉流接入监听: http://{}", addr);
    let sessions: Arc<parking_lot::Mutex<AHashMap<String, WhepSession>>> =
        Arc::new(parking_lot::Mutex::new(AHashMap::new()));
    let seq = Arc::new(AtomicU64::new(1));

    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                warn!("WHEP accept 失败: {}", e);
                continue;
            }
        };
        let state = state.clone();
        let sessions = sessions.clone();
        let seq = seq.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(stream, state, sessions, seq).await {
                tracing::debug!("WHEP 连接结束: {} ({})", peer, e);
            }
        });
    }
}

async fn handle(
    mut stream: TcpStream,
    state: Arc<SignalingState>,
    sessions: Arc<parking_lot::Mutex<AHashMap<String, WhepSession>>>,
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
                warn!("WHEP 鉴权失败: {}", e);
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
        "POST" if path == "/whep" => {
            let room = query_param(&query, "room").unwrap_or_else(|| "default".to_string());
            let Some(publisher) = query_param(&query, "publisher").filter(|p| !p.is_empty()) else {
                return write_response(
                    &mut stream,
                    "400 Bad Request",
                    "text/plain",
                    &[],
                    b"missing publisher query param",
                )
                .await;
            };
            let sdp_offer = String::from_utf8_lossy(&req.body).to_string();
            if sdp_offer.trim().is_empty() {
                return write_response(&mut stream, "400 Bad Request", "text/plain", &[], b"empty sdp")
                    .await;
            }
            let recv = offer_mids(&sdp_offer);
            if recv.is_empty() {
                return write_response(
                    &mut stream,
                    "400 Bad Request",
                    "text/plain",
                    &[],
                    b"offer has no audio/video m-line",
                )
                .await;
            }

            // 发布者所在 worker (全局编号), 用于跨 worker / 跨主机订阅。
            let publisher_worker = state.worker_of(&publisher);
            let publisher_tracks = state.publisher_tracks(publisher_worker, publisher.clone());
            if publisher_tracks.is_empty() {
                return write_response(
                    &mut stream,
                    "404 Not Found",
                    "text/plain",
                    &[],
                    b"publisher not found or has no media",
                )
                .await;
            }
            let subs = match_tracks(&publisher_tracks, &recv);

            let id = new_id(seq.fetch_add(1, Ordering::Relaxed));
            let worker_idx = state.assign_worker(&room, &id);
            let state_for_task = state.clone();
            let room_for_task = room.clone();
            let id_for_task = id.clone();
            let reply = tokio::task::spawn_blocking(move || {
                state_for_task.create_participant(
                    worker_idx,
                    room_for_task,
                    id_for_task,
                    sdp_offer,
                )
            })
            .await
            .unwrap_or(None);

            match reply {
                Some(crate::worker::WorkerReply::SdpAnswer {
                    sdp_answer,
                    local_candidates,
                    ..
                }) => {
                    // 作为订阅者: 对每个匹配轨道发起 subscribe。
                    for (publisher_mid, subscriber_mid) in &subs {
                        state.send_worker(
                            worker_idx,
                            WorkerCommand::Subscribe {
                                room_id: room.clone(),
                                subscriber_id: id.clone(),
                                publisher_id: publisher.clone(),
                                publisher_mid: publisher_mid.clone(),
                                subscriber_mid: subscriber_mid.clone(),
                                publisher_worker,
                            },
                        );
                    }
                    sessions.lock().insert(
                        id.clone(),
                        WhepSession {
                            participant_id: id.clone(),
                            room_id: room.clone(),
                            worker_idx,
                        },
                    );
                    let answer = inject_candidates(&sdp_answer, &local_candidates);
                    info!(
                        participant = %id, room = %room, publisher = %publisher,
                        tracks = subs.len(), "WHEP 拉流建立, worker={}", worker_idx
                    );
                    let location = format!("/whep/{}", id);
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
                    warn!("WHEP 建流失败: {}", error);
                    write_response(
                        &mut stream,
                        "500 Internal Server Error",
                        "text/plain",
                        &[],
                        error.as_bytes(),
                    )
                    .await
                }
                _ => {
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
        "PATCH" if path.starts_with("/whep/") => {
            let id = path.trim_start_matches("/whep/").to_string();
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
        "DELETE" if path.starts_with("/whep/") => {
            let id = path.trim_start_matches("/whep/").to_string();
            let session = sessions.lock().remove(&id);
            if let Some(s) = session {
                // 订阅者未加入房间, 只需释放 worker 绑定并移除 Rtc。
                state.release_worker(&s.room_id, &s.participant_id);
                state.send_worker(
                    s.worker_idx,
                    WorkerCommand::RemoveParticipant {
                        participant_id: s.participant_id.clone(),
                    },
                );
                info!(participant = %s.participant_id, "WHEP 拉流停止");
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
    fn match_by_kind() {
        let publisher = vec![
            ("5Ye".to_string(), "audio".to_string()),
            ("KZQ".to_string(), "video".to_string()),
        ];
        let recv = vec![
            ("audio".to_string(), "0".to_string()),
            ("video".to_string(), "1".to_string()),
        ];
        let subs = match_tracks(&publisher, &recv);
        assert_eq!(
            subs,
            vec![
                ("5Ye".to_string(), "0".to_string()),
                ("KZQ".to_string(), "1".to_string())
            ]
        );
    }

    #[test]
    fn match_skips_missing_kind() {
        let publisher = vec![
            ("a".to_string(), "audio".to_string()),
            ("v".to_string(), "video".to_string()),
        ];
        let recv = vec![("audio".to_string(), "0".to_string())];
        let subs = match_tracks(&publisher, &recv);
        assert_eq!(subs, vec![("a".to_string(), "0".to_string())]);
    }
}
