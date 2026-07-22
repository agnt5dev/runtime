# AGNT5 protobuf modules

This directory is also the package root for the published `agnt5-proto` Rust
crate. Keeping the crate wrapper, build script, and source IDL together ensures
that a packaged artifact never depends on paths elsewhere in the runtime
workspace.

The crate's default feature set generates and exports only
`agnt5.protocol.v2`, including client and server stubs. `legacy-api` enables the
temporary `api.v1` worker bridge, while `runtime-api` enables local query and
admin bindings. SDK repositories use neither feature.

This workspace contains two independently checked modules:

- `agnt5/protocol/v2` is the published SDK/runtime compatibility contract.
- `agnt5/runtime/v1` is the community runtime's local query and admin API.

The dependency direction is one-way: runtime APIs may import protocol types;
protocol files must not import runtime or managed packages. Managed schemas live
in the managed AGNT5 repository and consume a released protocol artifact.

Run validation from this directory:

```sh
buf format -d --exit-code
buf lint
buf build
buf generate --template buf.gen.go.yaml
(cd ../gen/go && go mod tidy && go test ./...)
```

SDK descriptor generation must select only `agnt5/protocol/v2`. The community
runtime binary may compile both modules.

The `api/v1` directory is a frozen transition-only worker contract compiled by
the Rust crate while the runtime migrates. It is excluded from the public Buf
modules and must not gain new behavior.

## Go bindings

`buf.gen.go.yaml` generates the public protocol into the independently
versioned `github.com/agnt5dev/runtime/gen/go` module. Every public v2 source
file declares the same stable Go import path. Generation deliberately excludes
`agnt5.runtime.v1` and transition-only `api.v1`.

The generated module is a release projection, not another schema owner. CI
regenerates it with pinned plugin versions and rejects drift.
