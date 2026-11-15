fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use protobuf-src's bundled protoc and includes
    std::env::set_var("PROTOC", protobuf_src::protoc());
    std::env::set_var("PROTOC_INCLUDE", protobuf_src::include());

    // The public worker contract.
    let protos = vec![
        "../../protos/proto/api/v1/common.proto",
        "../../protos/proto/api/v1/worker_coordinator.proto",
        "../../protos/proto/api/v1/worker_routing.proto",
        "../../protos/proto/api/v1/engine.proto",
    ];
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
