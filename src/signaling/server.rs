//! WebSocket 信令传输层
//!
//! 只负责把 WebSocket 的字节流与传输无关的 `SignalingSession` 对接:
//!   - 读: WebSocket Text 帧 → `inbound` 通道
//!   - 写: `msg_tx` 通道 → WebSocket Text 帧
//!
//! 协议解析与房间/订阅/Worker 交互全部在 `session` 模块完成, 与 QUIC 共用。

use crate::signaling::{session, SignalingState};
use futures_util::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
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
                        warn!("WebSocket 连接错误: {}", e);
                    }
                });
            }
            Err(e) => {
                error!("接受 WebSocket 连接失败: {}", e);
            }
        }
    }
}

/// 处理单个 WebSocket 连接
async fn handle_connection(
    stream: TcpStream,
    peer_addr: SocketAddr,
    state: Arc<SignalingState>,
) -> Result<(), Box<dyn std::error::Error>> {
    let ws_stream = tokio_tungstenite::accept_async(stream).await?;
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    info!("WebSocket 连接: {}", peer_addr);

    // 输出通道: SignalingSession 写入, 本函数下方的 send_task 消费并发送
    let (msg_tx, mut msg_rx) = mpsc::unbounded_channel::<String>();
    // 输入通道: 本函数读取 WebSocket 后写入, SignalingSession 消费
    let (inbound_tx, inbound_rx) = mpsc::unbounded_channel::<String>();

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
        session::run_session(session_state, msg_tx, inbound_rx, peer, "websocket").await;
    });

    // 接收循环
    while let Some(msg) = ws_receiver.next().await {
        match msg {
            Ok(Message::Text(text)) => {
                if inbound_tx.send(text.to_string()).is_err() {
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
