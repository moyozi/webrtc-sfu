//! WebSocket 信令传输层
//!
//! 只负责把 WebSocket 的字节流与传输无关的 `SignalingSession` 对接:
//!   - 读: WebSocket Text 帧 → `inbound` 通道
//!   - 写: `msg_tx` 通道 → WebSocket Text 帧
//!
//! 协议解析与房间/订阅/Worker 交互全部在 `session` 模块完成, 与 QUIC 共用。

use crate::signaling::auth::{extract_token, Claims};
use crate::signaling::{session, SignalingState};
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

/// 运行 WebSocket 信令服务器
pub async fn run_server(addr: SocketAddr, state: Arc<SignalingState>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("WebSocket 信令服务器启动: ws://{}", addr);

    loop {
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection(stream, peer_addr, state).await {
                        let msg = e.to_string();
                        // 分配器健康检查等 TCP 探针会在握手完成前断开连接, 属预期行为,
                        // 降为 debug 避免刷屏; 其余错误仍按 warn 输出。
                        if msg.contains("Handshake not finished")
                            || msg.contains("Connection reset")
                            || msg.contains("connection reset")
                            || msg.contains("Unexpected EOF")
                        {
                            tracing::debug!("WebSocket 连接提前结束 ({}): {}", peer_addr, msg);
                        } else {
                            warn!("WebSocket 连接错误 ({}): {}", peer_addr, msg);
                        }
                    }
                });
            }
            Err(e) => {
                error!("接受 WebSocket 连接失败: {}", e);
            }
        }
    }
}

/// 构造一个 401 响应, 用于在 WebSocket 握手段拒绝未通过鉴权的连接。
fn unauthorized(msg: &str) -> ErrorResponse {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .body(Some(msg.to_string()))
        .expect("build 401 response")
}

/// 处理单个 WebSocket 连接
///
/// 握手阶段从 `Authorization: Bearer` 头或 `?token=` 查询参数提取 JWT 并校验;
/// 强制鉴权模式下校验失败直接返回 401, 不升级为 WebSocket。
// `ErrorResponse` 由 tungstenite `Callback` trait 强制要求, 无法收窄。
#[allow(clippy::result_large_err)]
async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    state: Arc<SignalingState>,
) -> Result<(), Box<dyn std::error::Error>> {
    let auth = state.auth.clone();
    let captured: Arc<Mutex<Option<Result<Claims, String>>>> = Arc::new(Mutex::new(None));
    let cap = captured.clone();

    let callback = move |req: &Request, response: Response| -> Result<Response, ErrorResponse> {
        let header = req
            .headers()
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok());
        let result = match extract_token(header, req.uri().query()) {
            Some(token) => auth.verify(&token),
            None => Err("缺少鉴权 token (JWT)".to_string()),
        };
        if auth.is_required() {
            if let Err(e) = &result {
                warn!("WebSocket 鉴权失败 ({}): {}", peer_addr, e);
                return Err(unauthorized(e));
            }
        }
        *cap.lock().expect("auth capture lock") = Some(result);
        Ok(response)
    };

    let ws_stream = tokio_tungstenite::accept_hdr_async(stream, callback).await?;
    let claims = captured
        .lock()
        .expect("auth capture lock")
        .take()
        .and_then(|r| r.ok());
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    info!("WebSocket 连接: {}", peer_addr);

    // 输出通道: SignalingSession 写入, 本函数下方的 send_task 消费并发送
    let (msg_tx, mut msg_rx) = mpsc::channel::<String>(crate::signaling::SIGNALING_CHANNEL_CAP);
    // 输入通道: 本函数读取 WebSocket 后写入, SignalingSession 消费
    let (inbound_tx, inbound_rx) = mpsc::channel::<String>(crate::signaling::SIGNALING_CHANNEL_CAP);

    // 发送任务
    let send_task = tokio::spawn(async move {
        while let Some(msg) = msg_rx.recv().await {
            if ws_sender.send(Message::text(msg)).await.is_err() {
                break;
            }
        }
    });

    // 会话任务 (传输无关)
    let session_state = state.clone();
    let peer = peer_addr.to_string();
    let session_task = tokio::spawn(async move {
        session::run_session(session_state, msg_tx, inbound_rx, peer, "websocket", claims).await;
    });

    // 接收循环
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                // 会话消费过慢时关闭连接，避免输入队列无界增长。
                if inbound_tx.try_send(text.to_string()).is_err() {
                    break;
                }
            }
            Ok(Message::Close(_)) => {
                info!("WebSocket 关闭: {}", peer_addr);
                break;
            }
            Ok(_) => continue,
            Err(e) => {
                warn!("WebSocket 错误: {}", e);
                break;
            }
        }
    }

    // 关闭输入通道 → session 任务退出并清理
    drop(inbound_tx);
    let _ = session_task.await;
    send_task.abort();
    Ok(())
}
