//! 编译 gRPC proto (服务间控制面)。

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["proto/sfu.proto"], &["proto"])?;
    Ok(())
}
