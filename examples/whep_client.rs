//! 最小 WHEP 客户端 — 通过 WHEP (WebRTC-HTTP Egress Protocol) 从 SFU 拉流
//!
//! 用于验证 `SFU_WHEP_ADDR` 拉流接入: POST offer → 201 answer → trickle PATCH
//! → ICE/DTLS/SRTP → 统计接收媒体。
//!
//! ```bash
//! # 先用 load_publisher 在 room=demo 发布一路流
//! cargo run --example whep_client -- \
//!     --url http://127.0.0.1:5181 --room demo --publisher load-0 --duration 15
//! ```
//!
//! 参数:
//!   --url        WHEP 服务地址 (默认 http://127.0.0.1:5181)
//!   --room       房间号 (默认 demo)
//!   --publisher  要拉取的发布者 id (默认 load-0)
//!   --advertise  本端对外公布的 IP (默认 127.0.0.1)
//!   --duration   持续秒数 (默认 15)

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use str0m::change::SdpAnswer;
use str0m::media::{Direction, MediaKind};
use str0m::net::{Protocol, Receive};
use str0m::{Candidate, Event, Input, Output, Rtc};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};

struct Config {
    url: String,
    room: String,
    publisher: String,
    advertise_ip: IpAddr,
    duration: Duration,
}

impl Config {
    fn parse() -> Self {
        let mut cfg = Config {
            url: "http://127.0.0.1:5181".into(),
            room: "demo".into(),
            publisher: "load-0".into(),
            advertise_ip: IpAddr::V4(Ipv4Addr::LOCALHOST),
            duration: Duration::from_secs(15),
        };
        let mut args = std::env::args().skip(1);
        while let Some(key) = args.next() {
            let mut val = || args.next().unwrap_or_default();
            match key.as_str() {
                "--url" => cfg.url = val(),
                "--room" => cfg.room = val(),
                "--publisher" => cfg.publisher = val(),
                "--advertise" => {
                    if let Ok(ip) = val().parse() {
                        cfg.advertise_ip = ip;
                    }
                }
                "--duration" => cfg.duration = Duration::from_secs(val().parse().unwrap_or(15)),
                "--help" | "-h" => {
                    println!("{}", include_str!("whep_client.rs"));
                    std::process::exit(0);
                }
                other => eprintln!("忽略未知参数: {}", other),
            }
        }
        cfg
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cfg = Config::parse();
    let host = cfg
        .url
        .strip_prefix("http://")
        .unwrap_or(&cfg.url)
        .trim_end_matches('/')
        .to_string();

    // 1) 本地 UDP + Rtc + recvonly audio/video
    let udp = UdpSocket::bind("0.0.0.0:0").await?;
    let local_port = udp.local_addr()?.port();
    let advertised = SocketAddr::new(cfg.advertise_ip, local_port);
    let mut rtc = Rtc::builder().build(Instant::now());
    let local_candidate = Candidate::host(advertised, Protocol::Udp)?;
    rtc.add_local_candidate(local_candidate.clone());

    let (offer, pending) = {
        let mut api = rtc.sdp_api();
        api.add_media(MediaKind::Audio, Direction::RecvOnly, None, None, None);
        api.add_media(MediaKind::Video, Direction::RecvOnly, None, None, None);
        api.apply().ok_or("生成 SDP offer 失败")?
    };
    let mut pending = Some(pending);

    // 2) POST /whep?room=&publisher=
    let path = format!(
        "/whep?room={}&publisher={}",
        urlencode(&cfg.room),
        urlencode(&cfg.publisher)
    );
    let (status, headers, body) =
        http_request(&host, "POST", &path, "application/sdp", offer.to_sdp_string().as_bytes())
            .await?;
    if status != 201 {
        return Err(format!(
            "WHEP POST 失败: status={} body={}",
            status,
            String::from_utf8_lossy(&body)
        )
        .into());
    }
    let location = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("location"))
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| "/whep/unknown".into());
    let answer_sdp = String::from_utf8_lossy(&body).to_string();

    // 3) accept answer
    let answer = SdpAnswer::from_sdp_string(&answer_sdp)?;
    if let Some(p) = pending.take() {
        rtc.sdp_api().accept_answer(p, answer)?;
    }
    // 服务端内联的 host 候选
    for line in answer_sdp.lines() {
        if let Some(c) = line.trim().strip_prefix("a=candidate:") {
            if let Ok(cand) = Candidate::from_sdp_string(&format!("candidate:{}", c)) {
                rtc.add_remote_candidate(cand);
            }
        }
    }

    // 4) trickle 本端候选 (PATCH)
    let frag = format!(
        "a=mid:0\r\na={}\r\n",
        local_candidate.to_sdp_string()
    );
    let (status, _, _) =
        http_request(&host, "PATCH", &location, "application/trickle-ice-sdpfrag", frag.as_bytes())
            .await?;
    if status != 204 {
        eprintln!("警告: WHEP PATCH 返回 status={}", status);
    }

    println!(
        "[whep] 已建立 (Location={}), 拉取 room={} publisher={}",
        location, cfg.room, cfg.publisher
    );

    // 5) 媒体循环
    let mut buf = vec![0u8; 2000];
    let mut next_deadline = Instant::now();
    let mut rx_frames = 0u64;
    let mut rx_bytes = 0u64;
    let start = Instant::now();
    let deadline = start + cfg.duration;

    while Instant::now() < deadline {
        drain(&mut rtc, &udp, &mut next_deadline, &mut rx_frames, &mut rx_bytes);

        enum Act {
            Recv(SocketAddr, usize),
            Timeout,
        }
        let act = tokio::select! {
            r = udp.recv_from(&mut buf) => match r {
                Ok((n, src)) => Act::Recv(src, n),
                Err(_) => Act::Recv(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0), 0),
            },
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(next_deadline)) => {
                Act::Timeout
            }
        };
        match act {
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

    // 6) DELETE
    let _ = http_request(&host, "DELETE", &location, "text/plain", b"").await;

    println!(
        "[whep] 结束: 接收 {} 帧 / {:.1} MB, 收流 {:.2} Mbps",
        rx_frames,
        rx_bytes as f64 / 1e6,
        rx_bytes as f64 * 8.0 / cfg.duration.as_secs_f64() / 1e6
    );
    Ok(())
}

fn drain(
    rtc: &mut Rtc,
    udp: &UdpSocket,
    next_deadline: &mut Instant,
    rx_frames: &mut u64,
    rx_bytes: &mut u64,
) {
    loop {
        match rtc.poll_output() {
            Ok(Output::Transmit(t)) => {
                let _ = udp.try_send_to(&t.contents, t.destination);
            }
            Ok(Output::Event(Event::MediaData(md))) => {
                *rx_frames += 1;
                *rx_bytes += md.data.len() as u64;
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

fn urlencode(s: &str) -> String {
    s.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (b as char).to_string()
            }
            _ => format!("%{:02X}", b),
        })
        .collect()
}

/// 极简 HTTP/1.1 客户端 (服务端 `Connection: close`, 读到 EOF 即完整响应)。
async fn http_request(
    host: &str,
    method: &str,
    path: &str,
    content_type: &str,
    body: &[u8],
) -> Result<(u16, Vec<(String, String)>, Vec<u8>), Box<dyn std::error::Error + Send + Sync>> {
    let mut stream = TcpStream::connect(host).await?;
    let head = format!(
        "{} {} HTTP/1.1\r\nHost: {}\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        method,
        path,
        host,
        content_type,
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.flush().await?;

    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await?;
    let pos = resp
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("响应缺少 header 结束符")?;
    let head = String::from_utf8_lossy(&resp[..pos]);
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let headers = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        .collect();
    Ok((status, headers, resp[pos + 4..].to_vec()))
}
