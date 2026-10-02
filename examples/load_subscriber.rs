//! 最小压测订阅器 — 以 str0m 作为客户端从 SFU 拉流
//!
//! 与 `load_publisher` 配套使用，用于压测 SFU 的媒体转发热路径 (即「一路发布、
//! 多路拉流」的 fanout 场景)。每个订阅者拥有独立的 UDP socket 与 WebSocket
//! 信令连接，完整走 ICE + DTLS + SRTP 握手，然后持续接收并统计媒体数据。
//!
//! 用法:
//! ```bash
//! # 1) 先启动发布者，记下它打印的 track mid
//! cargo run --release --example load_publisher -- \
//!     --count 1 --duration 60 --advertise 192.168.0.101
//! # 输出形如: [mids] load-0 AnS ct1
//!
//! # 2) 启动 100 路订阅者订阅该发布者
//! cargo run --release --example load_subscriber -- \
//!     --url ws://192.168.0.101:5174 --room load --count 100 \
//!     --duration 60 --advertise 192.168.0.101 --publisher load-0:AnS:ct1
//! ```
//!
//! 参数:
//!   --url        WebSocket 信令地址 (默认 ws://127.0.0.1:5174)
//!   --room       房间号, 必须与发布者一致 (默认 load)
//!   --count      订阅者数量 (默认 10)
//!   --duration   持续秒数 (默认 20)
//!   --advertise  本端对外公布的 IP (默认 127.0.0.1)
//!   --publisher  <id>:<audio_mid>:<video_mid>，可重复，指定要订阅的发布者 track
//!   --tracks     订阅范围: both(默认) / audio(仅音频) / video(仅视频)
//!   --id-prefix  participant_id 前缀 (多进程压测时避免跨进程 ID 冲突)
//!
//! 说明: 每个订阅者用一个 Rtc 承载全部 m-line (每个发布者一对 audio/video,
//! 均为 recvonly)，并显式上报自身 mid，使 SFU 不再依赖「发布/订阅 mid 相同」的
//! 浏览器约定。
//!
//! 注意: 每个订阅者占用 1 个 UDP fd + 1 个 TCP fd，大规模压测前请调高
//! `ulimit -n`。压测机本身会消耗 CPU 做 SRTP 解密，测 SFU 单核能力时建议
//! 让压测进程独占其它核心、或放到另一台机器。

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use str0m::change::{SdpAnswer, SdpPendingOffer};
use str0m::media::{Direction, MediaKind, Mid};
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, Input, Output, Rtc};
use tokio::net::UdpSocket;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

#[derive(Default)]
struct Stats {
    connected: AtomicI64,
    /// 收到的媒体帧数 (MediaData 事件)
    rx_frames: AtomicU64,
    /// 收到的媒体字节数
    rx_bytes: AtomicU64,
    /// 发出的信令/RTCP/STUN 包数
    tx_packets: AtomicU64,
}

/// 一个待订阅的发布者及其 track mid
#[derive(Clone)]
struct PublisherTracks {
    id: String,
    audio_mid: String,
    video_mid: String,
    /// 可选的第 2 路视频 (屏幕共享) mid。
    screen_mid: Option<String>,
}

struct Config {
    url: String,
    room: String,
    count: usize,
    duration: Duration,
    advertise_ip: IpAddr,
    /// participant_id 前缀 (多进程压测时避免进程间 ID 冲突)。
    id_prefix: String,
    /// 订阅哪些 track: (audio, video)。默认都订阅。
    sub_audio: bool,
    sub_video: bool,
    publishers: Vec<PublisherTracks>,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut cfg = Config {
            url: "ws://127.0.0.1:5174".into(),
            room: "load".into(),
            count: 10,
            duration: Duration::from_secs(20),
            advertise_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            id_prefix: String::new(),
            sub_audio: true,
            sub_video: true,
            publishers: Vec::new(),
        };

        let mut args = std::env::args().skip(1);
        while let Some(key) = args.next() {
            let mut val = || args.next().unwrap_or_default();
            match key.as_str() {
                "--url" => cfg.url = val(),
                "--room" => cfg.room = val(),
                "--count" => cfg.count = val().parse().unwrap_or(cfg.count),
                "--duration" => {
                    cfg.duration = Duration::from_secs(val().parse().unwrap_or(20))
                }
                "--advertise" => {
                    if let Ok(ip) = val().parse() {
                        cfg.advertise_ip = ip;
                    }
                }
                "--id-prefix" => {
                    let p = val();
                    // 加分隔符避免与自动编号粘连 (如 "a" + "sub-0" → "a-sub-0")。
                    cfg.id_prefix = if p.is_empty() {
                        String::new()
                    } else {
                        format!("{}-sub", p.trim_end_matches('-'))
                    };
                }
                "--tracks" => match val().as_str() {
                    "both" => { cfg.sub_audio = true; cfg.sub_video = true; }
                    "audio" => { cfg.sub_audio = true; cfg.sub_video = false; }
                    "video" => { cfg.sub_audio = false; cfg.sub_video = true; }
                    other => return Err(format!("--tracks 需为 both/audio/video, 收到: {}", other)),
                },
                "--publisher" => {
                    let spec = val();
                    let parts: Vec<&str> = spec.split(':').collect();
                    if parts.len() != 3 && parts.len() != 4 {
                        return Err(format!(
                            "--publisher 需要 <id>:<audio_mid>:<video_mid>[:<screen_mid>] 格式, 收到: {}",
                            spec
                        ));
                    }
                    cfg.publishers.push(PublisherTracks {
                        id: parts[0].to_string(),
                        audio_mid: parts[1].to_string(),
                        video_mid: parts[2].to_string(),
                        screen_mid: parts.get(3).map(|s| s.to_string()),
                    });
                }
                "--help" | "-h" => {
                    println!("{}", include_str!("load_subscriber.rs"));
                    std::process::exit(0);
                }
                other => eprintln!("忽略未知参数: {}", other),
            }
        }

        if cfg.publishers.is_empty() {
            return Err("至少需要指定一个 --publisher <id>:<audio_mid>:<video_mid>".into());
        }
        Ok(cfg)
    }
}

#[tokio::main]
async fn main() {
    let cfg = Arc::new(match Config::parse() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("参数错误: {}", e);
            std::process::exit(1);
        }
    });
    let stats = Arc::new(Stats::default());

    println!(
        "压测订阅器: count={} room={} url={} 订阅 {} 个发布者: {}",
        cfg.count,
        cfg.room,
        cfg.url,
        cfg.publishers.len(),
        cfg.publishers
            .iter()
            .map(|p| p.id.as_str())
            .collect::<Vec<_>>()
            .join(","),
    );

    let s = stats.clone();
    tokio::spawn(async move {
        let mut last_bytes = 0u64;
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            let bytes = s.rx_bytes.load(Ordering::Relaxed);
            let mbps = (bytes - last_bytes) as f64 * 8.0 / 2.0 / 1e6;
            last_bytes = bytes;
            println!(
                "[stats] connected={}  收流={:.1} Mbps  (累计 {} 帧, {:.1} MB)  tx={} 包",
                s.connected.load(Ordering::Relaxed),
                mbps,
                s.rx_frames.load(Ordering::Relaxed),
                bytes as f64 / 1e6,
                s.tx_packets.load(Ordering::Relaxed),
            );
        }
    });

    let deadline = Instant::now() + cfg.duration + Duration::from_secs(5);
    let mut handles = Vec::with_capacity(cfg.count);
    for idx in 0..cfg.count {
        let cfg = cfg.clone();
        let stats = stats.clone();
        handles.push(tokio::spawn(async move {
            if let Err(e) = subscriber(idx, cfg, stats).await {
                eprintln!("[sub-{}] 退出: {}", idx, e);
            }
        }));
    }

    for (idx, h) in handles.into_iter().enumerate() {
        if tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), h)
            .await
            .is_err()
        {
            eprintln!("[sub-{}] 超时未结束", idx);
        }
    }

    println!(
        "共接收 {} 帧 / {:.1} MB",
        stats.rx_frames.load(Ordering::Relaxed),
        stats.rx_bytes.load(Ordering::Relaxed) as f64 / 1e6
    );
}

async fn subscriber(
    idx: usize,
    cfg: Arc<Config>,
    stats: Arc<Stats>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let participant_id = if cfg.id_prefix.is_empty() {
        format!("sub-{}", idx)
    } else {
        format!("{}-{}", cfg.id_prefix, idx)
    };

    // ── 1. 独立 UDP socket ──
    let udp = UdpSocket::bind("0.0.0.0:0").await?;
    enlarge_udp_buffers(&udp);
    let local_port = udp.local_addr()?.port();
    let advertised = SocketAddr::new(cfg.advertise_ip, local_port);

    // ── 2. Rtc + 本地候选 ──
    let mut rtc = Rtc::builder().build(Instant::now());
    let local_candidate = Candidate::host(advertised, Protocol::Udp)?;
    rtc.add_local_candidate(local_candidate.clone());

    // ── 3. 为每个发布者添加 recvonly audio/video，并记录自身 mid ──
    let mut own_mids: Vec<(Mid, Mid, Option<Mid>)> = Vec::with_capacity(cfg.publishers.len());
    let (offer, pending) = {
        let mut api = rtc.sdp_api();
        for pubr in &cfg.publishers {
            let a = api.add_media(MediaKind::Audio, Direction::RecvOnly, None, None, None);
            let v = api.add_media(MediaKind::Video, Direction::RecvOnly, None, None, None);
            // 发布者有屏幕共享时, 追加一条 recvonly video。
            let s = pubr
                .screen_mid
                .is_some()
                .then(|| api.add_media(MediaKind::Video, Direction::RecvOnly, None, None, None));
            own_mids.push((a, v, s));
        }
        api.apply().ok_or("生成 SDP offer 失败")?
    };
    let mut pending: Option<SdpPendingOffer> = Some(pending);

    // ── 4. WebSocket 信令 ──
    let (ws, _resp) = connect_async(cfg.url.as_str()).await?;
    let (mut ws_tx, mut ws_rx) = ws.split();

    ws_tx
        .send(Message::text(
            json!({
                "type": "join",
                "pc_id": "sub",
                "room_id": cfg.room,
                "participant_id": participant_id,
                "sdp": offer.to_sdp_string(),
                "role": "subscriber",
            })
            .to_string(),
        ))
        .await?;

    ws_tx
        .send(Message::text(
            json!({
                "type": "ice_candidate",
                "pc_id": "sub",
                "candidate": local_candidate.to_sdp_string(),
                "sdp_mid": "0",
                "sdp_mline_index": 0,
            })
            .to_string(),
        ))
        .await?;

    let mut buf = vec![0u8; 2000];
    let mut next_deadline = Instant::now();
    let mut connected = false;
    let deadline = Instant::now() + cfg.duration;

    while Instant::now() < deadline {
        drain(&mut rtc, &udp, &mut next_deadline, &stats);

        let now_connected = rtc.is_connected();
        if now_connected != connected {
            stats
                .connected
                .fetch_add(if now_connected { 1 } else { -1 }, Ordering::Relaxed);
            connected = now_connected;
        }

        enum Act {
            Ws(Message),
            WsClosed,
            Recv(SocketAddr, usize),
            Timeout,
        }

        let act = tokio::select! {
            msg = ws_rx.next() => match msg {
                Some(Ok(m)) => Act::Ws(m),
                _ => Act::WsClosed,
            },
            r = udp.recv_from(&mut buf) => match r {
                Ok((n, src)) => Act::Recv(src, n),
                Err(_) => Act::Recv(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0), 0),
            },
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(next_deadline)) => {
                Act::Timeout
            }
        };

        match act {
            Act::WsClosed => break,
            Act::Ws(msg) => {
                let Message::Text(text) = msg else { continue };
                if let Some(answer_sdp) = extract_sdp_answer(&text) {
                    let answer = match SdpAnswer::from_sdp_string(&answer_sdp) {
                        Ok(a) => a,
                        Err(e) => {
                            eprintln!("[{}] 解析 SDP answer 失败: {}", participant_id, e);
                            continue;
                        }
                    };
                    if let Some(p) = pending.take() {
                        if let Err(e) = rtc.sdp_api().accept_answer(p, answer) {
                            eprintln!("[{}] accept_answer 失败: {}", participant_id, e);
                            continue;
                        }
                    }
                    add_candidates_from_answer(&text, &mut rtc);

                    // SDP 完成后按选择为每个发布者 track 发送 subscribe (携带自身 mid)
                    let mut n_sub = 0usize;
                    for (i, pubr) in cfg.publishers.iter().enumerate() {
                        let (own_a, own_v, own_s) = own_mids[i];
                        if cfg.sub_audio {
                            subscribe(&mut ws_tx, &pubr.id, &pubr.audio_mid, &own_a.to_string())
                                .await?;
                            n_sub += 1;
                        }
                        if cfg.sub_video {
                            subscribe(&mut ws_tx, &pubr.id, &pubr.video_mid, &own_v.to_string())
                                .await?;
                            n_sub += 1;
                        }
                        if let (Some(smid), Some(own_s)) = (&pubr.screen_mid, own_s) {
                            subscribe(&mut ws_tx, &pubr.id, smid, &own_s.to_string()).await?;
                            n_sub += 1;
                        }
                    }
                    println!("[{}] SDP 完成, 已发送 {} 条 subscribe", participant_id, n_sub);
                } else if is_ice_candidate(&text) {
                    add_candidate_message(&text, &mut rtc);
                }
            }
            Act::Recv(src, n) if n > 0 => {
                let local = udp.local_addr().unwrap_or(src);
                if let Ok(recv) = Receive::new(Protocol::Udp, src, local, &buf[..n]) {
                    let _ = rtc.handle_input(Input::Receive(Instant::now(), recv));
                }
            }
            Act::Recv(..) => {}
            Act::Timeout => {
                let _ = rtc.handle_input(Input::Timeout(Instant::now()));
            }
        }
    }

    let _ = ws_tx
        .send(Message::text(
            json!({"type": "leave", "pc_id": "sub"}).to_string(),
        ))
        .await;
    if connected {
        stats.connected.fetch_sub(1, Ordering::Relaxed);
    }

    Ok(())
}

/// 把 str0m 的输出 drain 到 `Timeout`，统计收到的媒体帧。
fn drain(rtc: &mut Rtc, udp: &UdpSocket, next_deadline: &mut Instant, stats: &Stats) {
    loop {
        match rtc.poll_output() {
            Ok(Output::Transmit(t)) => {
                stats.tx_packets.fetch_add(1, Ordering::Relaxed);
                let _ = udp.try_send_to(&t.contents, t.destination);
            }
            Ok(Output::Event(Event::MediaData(md))) => {
                stats.rx_frames.fetch_add(1, Ordering::Relaxed);
                stats.rx_bytes.fetch_add(md.data.len() as u64, Ordering::Relaxed);
            }
            Ok(Output::Event(_)) => {}
            Ok(Output::Timeout(t)) => {
                *next_deadline = t;
                break;
            }
            Err(_) => break,
        }
    }
}

/// 发送一条 subscribe 消息 (track_mid = 发布者 mid, subscriber_mid = 自身 mid)
async fn subscribe<S>(
    ws_tx: &mut S,
    publisher_id: &str,
    track_mid: &str,
    subscriber_mid: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    S: futures_util::Sink<Message> + Unpin,
    S::Error: std::error::Error + Send + Sync + 'static,
{
    ws_tx
        .send(Message::text(
            json!({
                "type": "subscribe",
                "pc_id": "sub",
                "publisher_id": publisher_id,
                "track_mid": track_mid,
                "subscriber_mid": subscriber_mid,
            })
            .to_string(),
        ))
        .await?;
    Ok(())
}

/// 从 sdp_answer 消息中取出 sdp 字段
fn extract_sdp_answer(text: &str) -> Option<String> {
    let v: serde_json::Value = serde_json::from_str(text).ok()?;
    if v["type"].as_str() != Some("sdp_answer") {
        return None;
    }
    Some(v["sdp"].as_str()?.to_string())
}

/// 处理 sdp_answer 内联的候选
fn add_candidates_from_answer(text: &str, rtc: &mut Rtc) {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return;
    };
    if let Some(cands) = v["ice_candidates"].as_array() {
        for c in cands.iter().filter_map(|c| c.as_str()) {
            if let Ok(cand) = Candidate::from_sdp_string(c) {
                rtc.add_remote_candidate(cand);
            }
        }
    }
}

/// 调大压测客户端 UDP socket 收发缓冲区 (默认仅 ~208KB，高路数下易丢包)。
/// 实际生效值受内核 `rmem_max`/`wmem_max` 限制。
fn enlarge_udp_buffers(udp: &UdpSocket) {
    use std::os::fd::AsRawFd;
    let bytes: i32 = 8 * 1024 * 1024;
    unsafe {
        let fd = udp.as_raw_fd();
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &bytes as *const _ as *const _,
            std::mem::size_of::<i32>() as u32,
        );
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &bytes as *const _ as *const _,
            std::mem::size_of::<i32>() as u32,
        );
    }
}

fn is_ice_candidate(text: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(text)
        .map(|v| v["type"].as_str() == Some("ice_candidate"))
        .unwrap_or(false)
}

/// 处理 trickle ice_candidate 消息
fn add_candidate_message(text: &str, rtc: &mut Rtc) {
    let Ok(v) = serde_json::from_str::<serde_json::Value>(text) else {
        return;
    };
    if let Some(cs) = v["candidate"].as_str() {
        if let Ok(cand) = Candidate::from_sdp_string(cs) {
            rtc.add_remote_candidate(cand);
        }
    }
}
