fn main() -> Result<(), Box<dyn std::error::Error>> {
    std::env::set_var("PROTOC", protobuf_src::protoc());

    // Keep every source inside this crate root. Published crate tarballs and
    // downstream SDK builds must not depend on the runtime workspace layout.
    let mut protos = vec![
        "agnt5/protocol/v2/common.proto",
        "agnt5/protocol/v2/errors.proto",
        "agnt5/protocol/v2/capabilities.proto",
        "agnt5/protocol/v2/execution_options.proto",
        "agnt5/protocol/v2/run_policy.proto",
        "agnt5/protocol/v2/trigger.proto",
        "agnt5/protocol/v2/component.proto",
        "agnt5/protocol/v2/state.proto",
        "agnt5/protocol/v2/dispatch.proto",
        "agnt5/protocol/v2/execution.proto",
        "agnt5/protocol/v2/durable.proto",
        "agnt5/protocol/v2/worker.proto",
        "agnt5/protocol/v2/endpoint.proto",
    ];

    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_LEGACY_API");
    if std::env::var_os("CARGO_FEATURE_LEGACY_API").is_some() {
        protos.extend(["api/v1/engine.proto", "api/v1/runtime.proto"]);
    }

    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_RUNTIME_API");
    if std::env::var_os("CARGO_FEATURE_RUNTIME_API").is_some() {
        protos.extend([
            "agnt5/runtime/v1/admin.proto",
            "agnt5/runtime/v1/query.proto",
        ]);
    }

    for proto in &protos {
        println!("cargo:rerun-if-changed={proto}");
    }

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .boxed(".agnt5.protocol.v2.PollRunResponse.result.execute")
        .compile_protos(&protos, &["."])?;
    Ok(())
}
