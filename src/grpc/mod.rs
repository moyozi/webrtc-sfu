//! 信令 → 媒体服务的 gRPC 控制面。
//!
//! ```text
//!   signaling ──gRPC MediaControl──► media (workers)
//!   signaling/media ←──媒体 RTP: UDP (点对点中继) ──► media
//! ```
//!
//! 所有 RPC 定义见 `proto/sfu.proto`。仅替换「信令对远端 worker 的命令通道」
//! (原自研 TCP 控制面), 媒体面保持 UDP。

pub mod client;
pub mod convert;
pub mod server;

pub use client::GrpcControl;

/// prost/tonic 生成的类型。
pub mod pb {
    tonic::include_proto!("sfu.control.v1");
}
