//! QUIC 信令客户端参考实现 (含连接迁移)
//!
//! 运行:
//! ```bash
//! # 默认连接 127.0.0.1:5173, 跳过证书校验 (仅开发)
//! cargo run --example quic_signaling_client -- 192.168.31.112:5173
//!
//! # 用服务端启动日志打印的 SHA-256 指纹固定证书
//! cargo run --example quic_signaling_client -- 192.168.31.112:5173 <指纹hex>
//! ```
//!
//! 交互命令:
//!   <任意一行 JSON>   作为信令消息发送
//!   /migrate [ip]     把底层 UDP socket 重新绑定到新地址 (默认 0.0.0.0:0),
//!                     演示移动端 WiFi/蜂窝切换后 QUIC 连接不中断
//!   /quit             退出
//!
//! 说明: 这里只演示 QUIC 信令的传输层。真实的 join 需要先由 str0m/浏览器生成
//! SDP offer, 本示例聚焦于「换网后信令连接仍在」这一能力。

use quinn::crypto::rustls::QuicClientConfig;
use quinn::rustls;
use quinn::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use quinn::{ClientConfig, Endpoint};
use sha2::{Digest, Sha256};
use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};

const MAX_FRAME: usize = 1 << 20;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let server: SocketAddr = args
        .next()
        .unwrap_or_else(|| "127.0.0.1:5173".to_string())
        .parse()?;
    let fingerprint = args.next();

    // 客户端本地绑定 0.0.0.0:0; 迁移时会 rebind 到这个 socket。
    let mut endpoint = Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))?;
    endpoint.set_default_client_config(make_client_config(fingerprint)?);

    let conn = endpoint.connect(server, "localhost")?.await?;
    println!(
        "[quic] 已连接: server={} local={}",
        conn.remote_address(),
        endpoint.local_addr()?
    );

    // 打开一条双向流承载信令 (长度前缀分帧)
    let (mut send, mut recv) = conn.open_bi().await?;

    // 接收任务: 打印服务端消息
    let recv_task = tokio::spawn(async move {
        loop {
            match read_frame(&mut recv).await {
                Ok(Some(frame)) => match String::from_utf8(frame) {
                    Ok(text) => println!("[recv] {}", text),
                    Err(_) => println!("[recv] <非 UTF-8 帧>"),
                },
                Ok(None) => {
                    println!("[quic] 服务端关闭了信令流");
                    break;
                }
                Err(e) => {
                    println!("[quic] 读帧失败: {}", e);
                    break;
                }
            }
        }
    });

    // 主循环: 读取 stdin
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();
    print!("> ");
    loop {
        let line = match lines.next_line().await? {
            Some(l) => l,
            None => break,
        };
        let line = line.trim();
        if line.is_empty() {
            print!("> ");
            continue;
        }

        if line == "/quit" {
            break;
        }

        if let Some(rest) = line.strip_prefix("/migrate") {
            let ip: IpAddr = rest
                .trim()
                .parse()
                .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED));
            match migrate(&endpoint, ip) {
                Ok(local) => println!("[quic] 已迁移到本地地址 {} (连接保持)", local),
                Err(e) => println!("[quic] 迁移失败: {}", e),
            }
            print!("> ");
            continue;
        }

        if let Err(e) = write_frame(&mut send, line.as_bytes()).await {
            println!("[quic] 发送失败: {}", e);
            break;
        }
        print!("> ");
    }

    let _ = send.finish();
    recv_task.abort();
    endpoint.wait_idle().await;
    Ok(())
}

/// 重新绑定底层 UDP socket, 触发 QUIC 路径迁移。
///
/// 真实移动场景中, iOS/Android 网络切换后旧 socket 失效, 上层只需用新接口
/// 地址重建 socket 并调用 `rebind`, 已有 QUIC 连接 (含信令流) 会迁移到新路径,
/// 无需重新握手或重新加入房间。
fn migrate(endpoint: &Endpoint, ip: IpAddr) -> std::io::Result<SocketAddr> {
    let socket = std::net::UdpSocket::bind(SocketAddr::new(ip, 0))?;
    endpoint.rebind(socket)?;
    endpoint.local_addr()
}

/// 构造 QUIC 客户端配置。若给定指纹则固定校验, 否则跳过校验 (仅开发)。
fn make_client_config(
    fingerprint: Option<String>,
) -> Result<ClientConfig, Box<dyn Error + Send + Sync>> {
    let crypto = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(PinnedVerifier::new(fingerprint))
        .with_no_client_auth();
    Ok(ClientConfig::new(Arc::new(QuicClientConfig::try_from(
        crypto,
    )?)))
}

/// 证书校验器: 支持按 SHA-256 指纹固定; 未提供指纹时接受任意证书 (仅开发)。
#[derive(Debug)]
struct PinnedVerifier {
    expected: Option<[u8; 32]>,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl PinnedVerifier {
    fn new(fingerprint: Option<String>) -> Arc<Self> {
        let expected = fingerprint.and_then(|hex| {
            let bytes = (0..hex.len() / 2)
                .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok())
                .collect::<Option<Vec<u8>>>();
            bytes.and_then(|b| <[u8; 32]>::try_from(b).ok())
        });
        if expected.is_none() {
            eprintln!("[warn] 未提供证书指纹, 跳过服务端证书校验 (仅限开发环境)");
        }
        Arc::new(Self {
            expected,
            provider: Arc::new(rustls::crypto::ring::default_provider()),
        })
    }
}

impl rustls::client::danger::ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        if let Some(expected) = &self.expected {
            let mut hasher = Sha256::new();
            hasher.update(end_entity.as_ref());
            let actual = hasher.finalize();
            if actual.as_slice() != expected.as_slice() {
                return Err(rustls::Error::General(
                    "服务端证书指纹不匹配".to_string(),
                ));
            }
        }
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

async fn write_frame(send: &mut quinn::SendStream, data: &[u8]) -> std::io::Result<()> {
    send.write_all(&(data.len() as u32).to_be_bytes())
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::BrokenPipe, e))?;
    send.write_all(data)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::BrokenPipe, e))?;
    Ok(())
}

async fn read_frame(recv: &mut quinn::RecvStream) -> std::io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match recv.read_exact(&mut len_buf).await {
        Ok(()) => {}
        Err(quinn::ReadExactError::FinishedEarly(_)) => return Ok(None),
        Err(quinn::ReadExactError::ReadError(e)) => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionAborted,
                e,
            ))
        }
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    if len == 0 || len > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("非法帧长度: {}", len),
        ));
    }
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf)
        .await
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::ConnectionAborted, e))?;
    Ok(Some(buf))
}
