//! QUIC 信令传输层 (基于 quinn)
//!
//! 与 WebSocket 分支完全对称: 把 QUIC 双向流的字节流与传输无关的
//! `SignalingSession` 对接。二者共用同一套 JSON 协议与房间/订阅逻辑。
//!
//! ## 为什么用 QUIC 做信令
//!
//! QUIC 连接由 **Connection ID** 标识, 而不是四元组 (ip:port)。移动终端从
//! WiFi 切到蜂窝 (或反之) 时本机 IP 改变, TCP/WebSocket 连接必然断开并需要
//! 重建 (重新握手、重新加入房间、重新协商 SDP); 而 QUIC 连接可以在新路径上
//! 继续, 只需一次路径验证 (PATH_CHALLENGE/RESPONSE), 信令会话状态不丢失,
//! 媒体协商结果也无需重做。
//!
//! ## 帧格式
//!
//! 单条双向流上承载多条消息, 采用长度前缀分帧:
//!
//! ```text
//!   ┌──────────────┬───────────────────────┐
//!   │ u32 BE len   │ JSON 负载 (len 字节)   │
//!   └──────────────┴───────────────────────┘
//! ```
//!
//! 相比换行分隔, 长度前缀不依赖 JSON 内部不含换行的约定, 更健壮。

use crate::signaling::{session, SignalingState};
use quinn::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

/// 单帧最大长度 (1 MiB), 防御异常长度前缀
const MAX_FRAME: usize = 1 << 20;

/// QUIC 所需的 TLS 材料
pub struct QuicTls {
    pub cert: CertificateDer<'static>,
    pub key: PrivateKeyDer<'static>,
}

impl QuicTls {
    /// 生成本地自签名证书 (适合开发/内网; 生产应换成正式证书)
    pub fn self_signed(hostname: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let certified = rcgen::generate_simple_self_signed(vec![hostname.to_string()])?;
        let cert = certified.cert.der().clone();
        let key = PrivateKeyDer::from(PrivatePkcs8KeyDer::from(
            certified.signing_key.serialize_der(),
        ));
        Ok(Self { cert, key })
    }

    /// 证书 DER 的 SHA-256 指纹 (十六进制), 供客户端固定校验 / 浏览器
    /// WebTransport 的 `serverCertificateHashes` 使用
    pub fn fingerprint_sha256(&self) -> String {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(self.cert.as_ref());
        let digest = hasher.finalize();
        digest.iter().map(|b| format!("{:02x}", b)).collect()
    }
}

/// 运行 QUIC 信令服务器
pub async fn run_server(
    addr: SocketAddr,
    state: Arc<SignalingState>,
    tls: QuicTls,
) -> io::Result<()> {
    let mut server_config =
        quinn::ServerConfig::with_single_cert(vec![tls.cert], tls.key).map_err(io::Error::other)?;

    // 连接迁移依赖 keep-alive 让对端在地址变化后尽快验证新路径；
    // idle timeout 不宜过短, 否则移动端切换网络期间空窗会被判定超时。
    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(
        Duration::from_secs(30)
            .try_into()
            .map_err(io::Error::other)?,
    ));
    transport.keep_alive_interval(Some(Duration::from_secs(5)));
    server_config.transport_config(Arc::new(transport));

    let endpoint = quinn::Endpoint::server(server_config, addr)?;
    info!("QUIC 信令服务器启动: quic://{}", addr);

    while let Some(incoming) = endpoint.accept().await {
        let state = state.clone();
        tokio::spawn(async move {
            match incoming.await {
                Ok(conn) => {
                    let peer = conn.remote_address();
                    if let Err(e) = handle_connection(conn, state, peer).await {
                        debug!("QUIC 连接结束: {} ({})", peer, e);
                    }
                }
                Err(e) => warn!("QUIC 握手失败: {}", e),
            }
        });
    }

    Ok(())
}

/// 处理单条 QUIC 连接
///
/// 服务端接受客户端打开的第一条双向流作为信令流。连接迁移对上层透明:
/// 即使 `conn.remote_address()` 在中途变化, 本函数也不依赖它做路由。
async fn handle_connection(
    conn: quinn::Connection,
    state: Arc<SignalingState>,
    peer: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("QUIC 信令连接: {}", peer);

    let (mut send, mut recv) = conn.accept_bi().await?;

    // 输出通道: SignalingSession 写入, writer 任务消费并发送
    let (msg_tx, mut msg_rx) = mpsc::channel::<String>(crate::signaling::SIGNALING_CHANNEL_CAP);
    // 输入通道: reader 写入, SignalingSession 消费
    let (inbound_tx, inbound_rx) = mpsc::channel::<String>(crate::signaling::SIGNALING_CHANNEL_CAP);

    // 发送任务
    let writer = tokio::spawn(async move {
        while let Some(msg) = msg_rx.recv().await {
            if let Err(e) = write_frame(&mut send, msg.as_bytes()).await {
                debug!("QUIC 写帧失败: {}", e);
                break;
            }
        }
        let _ = send.finish();
    });

    // 会话任务 (传输无关)
    let session_state = state.clone();
    let peer_str = peer.to_string();
    let session_task = tokio::spawn(async move {
        session::run_session(session_state, msg_tx, inbound_rx, peer_str, "quic").await;
    });

    // 接收循环
    loop {
        match read_frame(&mut recv).await {
            Ok(Some(frame)) => match String::from_utf8(frame) {
                Ok(text) => {
                    // 会话消费过慢时关闭连接，避免输入队列无界增长。
                    if inbound_tx.try_send(text).is_err() {
                        break;
                    }
                }
                Err(_) => warn!("QUIC 收到非 UTF-8 帧, 已丢弃"),
            },
            Ok(None) => break, // 流被对端关闭
            Err(e) => {
                debug!("QUIC 读帧失败: {}", e);
                break;
            }
        }
    }

    // 关闭输入通道 → session 任务退出并清理
    drop(inbound_tx);
    let _ = session_task.await;
    writer.abort();
    Ok(())
}

/// 写入一个长度前缀帧
async fn write_frame(send: &mut quinn::SendStream, data: &[u8]) -> io::Result<()> {
    send.write_all(&(data.len() as u32).to_be_bytes())
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))?;
    send.write_all(data)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::BrokenPipe, e))?;
    Ok(())
}

/// 读取一个长度前缀帧; 返回 `Ok(None)` 表示流已正常结束
async fn read_frame(recv: &mut quinn::RecvStream) -> io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match recv.read_exact(&mut len_buf).await {
        Ok(()) => {}
        Err(quinn::ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(quinn::ReadExactError::ReadError(e)) => {
            return Err(io::Error::new(io::ErrorKind::ConnectionAborted, e))
        }
    }

    let len = u32::from_be_bytes(len_buf) as usize;
    if len == 0 || len > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("非法帧长度: {}", len),
        ));
    }

    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| io::Error::new(io::ErrorKind::ConnectionAborted, e))?;
    Ok(Some(buf))
}
