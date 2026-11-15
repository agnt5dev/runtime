fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use protobuf-src's bundled protoc and includes
    std::env::set_var("PROTOC", protobuf_src::protoc());
    std::env::set_var("PROTOC_INCLUDE", protobuf_src::include());

    // The public worker contract, which the OSS runtime is built from.
    let mut protos = vec![
        "../../protos/proto/api/v1/common.proto",
        "../../protos/proto/api/v1/worker_coordinator.proto",
        "../../protos/proto/api/v1/worker_routing.proto",
        "../../protos/proto/api/v1/engine.proto",
        "../../protos/proto/api/v1/execution_engine.proto",
    ];
    // Contracts between nodes of a hosted runtime cluster. They stay out of
    // the OSS build so it never depends on them.
    if std::env::var_os("CARGO_FEATURE_MANAGED").is_some() {
        protos.extend([
            "../../protos/proto/api/v1/coordinator_gossip.proto",
            "../../protos/proto/api/v1/runtime_dispatch.proto",
            "../../protos/proto/api/v1/replication.proto",
        ]);
    }

    // Re-run build script when proto files change
    for proto in &protos {
        println!("cargo:rerun-if-changed={proto}");
    }

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(
            std::path::PathBuf::from(std::env::var("OUT_DIR")?).join("engine_descriptor.bin"),
        )
        .compile_protos(&protos, &["../../protos/proto"])?;
    Ok(())
}
