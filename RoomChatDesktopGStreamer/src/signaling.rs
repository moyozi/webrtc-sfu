//! 信令客户端 — WebSocket (tokio-tungstenite)。
//!
//! 设计: 本结构持有一个专用 tokio 运行时; 一个异步任务负责:
//!   · 建立 WS 连接
//!   · 把 `tx` 里待发送的 JSON 文本写入 socket (writer 子任务)
//!   · 读取 socket 消息, 反序列化后经 crossbeam channel 交给 WebRTC worker
//!
//! WebRTC worker 线程是同步阻塞的, 因此不把 reactor-webrtc 调用放进 tokio
//! 任务里; 两者只通过 channel 交互。

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use crossbeam_channel::Sender;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc::UnboundedSender;
use tokio_tungstenite::tungstenite::Message;

use crate::protocol::{ClientMessage, ServerMessage};

/// 等待 WebSocket 握手完成的最长时间
const CONNECT_TIMEOUT: Duration = Duration::from_secs(8);

pub struct Signaling {
    /// 待发送的 JSON 文本
    tx: UnboundedSender<String>,
    /// 主动关闭标记: 置位后后台任务不再上报"连接已断开"
    closed: Arc<AtomicBool>,
    /// 持有运行时, drop 时关闭连接与后台任务
    _rt: tokio::runtime::Runtime,
}

impl Drop for Signaling {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::SeqCst);
    }
}

impl Signaling {
    /// 连接到 `url` (如 `ws://127.0.0.1:5174`)。
    ///
    /// 阻塞等待握手结果 (最多 [`CONNECT_TIMEOUT`]), 以便加入失败时立即报错。
    /// 之后的断开仍以 `ServerMessage::Error` 的形式投递到 `msg_tx`。
    pub fn connect(url: &str, msg_tx: Sender<ServerMessage>) -> Result<Self, String> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("signaling-rt")
            .enable_all()
            .build()
            .map_err(|e| format!("创建 tokio 运行时失败: {e}"))?;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
        let (conn_tx, conn_rx) = mpsc::channel::<Result<(), String>>();
        let url = url.to_string();
        let closed = Arc::new(AtomicBool::new(false));
        let closed_task = closed.clone();

        rt.spawn(async move {
            let ws = match tokio_tungstenite::connect_async(&url).await {
                Ok((ws, _resp)) => ws,
                Err(e) => {
                    let _ = conn_tx.send(Err(format!("WebSocket 连接失败: {e}")));
                    return;
                }
            };
            let _ = conn_tx.send(Ok(()));

            let (mut sink, mut stream) = ws.split();

            // writer: 从 rx 取文本写入 socket
            let writer = tokio::spawn(async move {
                while let Some(text) = rx.recv().await {
                    if sink.send(Message::text(text)).await.is_err() {
                        break;
                    }
                }
                let _ = sink.close().await;
            });

            // reader
            while let Some(item) = stream.next().await {
                match item {
                    Ok(Message::Text(text)) => {
                        match serde_json::from_str::<ServerMessage>(&text) {
                            Ok(m) => {
                                if msg_tx.send(m).is_err() {
                                    break; // worker 已退出
                                }
                            }
                            Err(e) => {
                                let _ = msg_tx.send(ServerMessage::Error {
                                    pc_id: None,
                                    error: format!("信令消息解析失败: {e}"),
                                });
                            }
                        }
                    }
                    Ok(Message::Close(_)) => break,
                    Ok(_) => {}
                    Err(e) => {
                        let _ = msg_tx.send(ServerMessage::Error {
                            pc_id: None,
                            error: format!("信令读取错误: {e}"),
                        });
                        break;
                    }
                }
            }

            writer.abort();
            // 主动关闭 (leave) 时不上报断开, 避免污染下一次会话
            if !closed_task.load(Ordering::SeqCst) {
                let _ = msg_tx.send(ServerMessage::Error {
                    pc_id: None,
                    error: "信令连接已断开".to_string(),
                });
            }
        });

        match conn_rx.recv_timeout(CONNECT_TIMEOUT) {
            Ok(Ok(())) => Ok(Self {
                tx,
                closed,
                _rt: rt,
            }),
            Ok(Err(e)) => Err(e),
            Err(_) => Err("连接信令服务器超时".to_string()),
        }
    }

    /// 异步发送一条信令消息 (实际写出由后台 writer 任务完成)。
    pub fn send(&self, msg: ClientMessage) {
        let _ = self.tx.send(msg.to_json());
    }
}
