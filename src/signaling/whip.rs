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

use crate::signaling::SignalingState;
use crate::worker::WorkerCommand;
use ahash::AHashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{info, warn};

struct HttpRequest {
    method: String,
    target: String,
    body: Vec<u8>,
}

struct WhipSession {
    participant_id: String,
    room_id: String,
    worker_idx: usize,
}

/// 生成一个不会与 WS 侧冲突的 participant id。
fn new_id(seq: u64) -> String {
    format!("whip-{}", seq)
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

async fn read_request(stream: &mut TcpStream) -> io::Result<Option<HttpRequest>> {
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos + 4;
        }
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            if buf.is_empty() {
                return Ok(None);
            }
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "no header end",
            ));
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > 1 << 20 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "header too large",
            ));
        }
    };

    let head = String::from_utf8_lossy(&buf[..header_end]);
    let mut lines = head.split("\r\n");
    let request_line = lines.next().unwrap_or_default();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let target = parts.next().unwrap_or_default().to_string();

    let mut content_length = 0usize;
    for l in lines {
        if let Some(v) = l
            .split_once(':')
            .filter(|(k, _)| k.eq_ignore_ascii_case("content-length"))
            .map(|(_, v)| v)
        {
            content_length = v.trim().parse().unwrap_or(0);
        }
    }

    let mut body = buf[header_end..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut tmp).await?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(content_length);

    Ok(Some(HttpRequest {
        method,
        target,
        body,
    }))
}

fn parse_head(req: &HttpRequest) -> (String, String) {
    // 返回 (path, query)
    let (path, query) = match req.target.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (req.target.clone(), String::new()),
    };
    (path, query)
}

fn query_param(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key {
                return Some(percent_decode(v));
            }
        }
    }
    None
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(b) = u8::from_str_radix(&s[i + 1..i + 3], 16) {
                out.push(b);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// 把 SFU 的 ICE 候选注入 SDP answer (WHIP 非 trickle 场景)。
fn inject_candidates(sdp: &str, candidates: &[String]) -> String {
    let mut out = String::with_capacity(sdp.len() + 128);
    for line in sdp.lines() {
        out.push_str(line);
        out.push_str("\r\n");
        if line.starts_with("a=ice-pwd:") {
            for c in candidates {
                out.push_str("a=");
                out.push_str(c);
                out.push_str("\r\n");
            }
        }
    }
    out
}

async fn write_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    extra: &[(&str, &str)],
    body: &[u8],
) -> io::Result<()> {
    let mut head = format!(
        "HTTP/1.1 {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        status,
        content_type,
        body.len()
    );
    // CORS (浏览器 WHIP 客户端)
    head.push_str("Access-Control-Allow-Origin: *\r\n");
    head.push_str("Access-Control-Allow-Methods: POST, PATCH, DELETE, OPTIONS\r\n");
    head.push_str("Access-Control-Allow-Headers: Content-Type, Authorization\r\n");
    for (k, v) in extra {
        head.push_str(k);
        head.push_str(": ");
        head.push_str(v);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await
}

/// 从 trickle-ice SDP fragment 中提取 (mid, candidate)。
fn parse_candidates(frag: &str) -> Vec<(Option<String>, String)> {
    let mut out = Vec::new();
    let mut cur_mid: Option<String> = None;
    for line in frag.lines() {
        if let Some(m) = line.strip_prefix("a=mid:") {
            cur_mid = Some(m.trim().to_string());
        } else if let Some(c) = line.strip_prefix("a=candidate:") {
            out.push((cur_mid.clone(), format!("candidate:{}", c.trim())));
        }
    }
    out
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

    match method.as_str() {
        "OPTIONS" => write_response(&mut stream, "204 No Content", "text/plain", &[], b"").await,
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
                    state.join_room(&room, &id);
                    state.notify_room(
                        &room,
                        &id,
                        &serde_json::to_string(
                            &crate::signaling::protocol::SignalingResponse::ParticipantJoined {
                                participant_id: id.clone(),
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
