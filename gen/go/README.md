# AGNT5 protocol bindings for Go

This module contains generated Go bindings for the public
`agnt5.protocol.v2` SDK/runtime contract.

The canonical schema lives in `../../proto/agnt5/protocol/v2`. Do not edit
`.pb.go` files by hand. Regenerate them from `runtime/proto`:

```sh
buf generate --template buf.gen.go.yaml
```

The module is released immutably and independently from the Go SDK. Consumers
pin a released version of `github.com/agnt5dev/runtime/gen/go`; they do not
copy the source protos or depend on the Rust `agnt5-proto` crate. SDK update
pull requests also commit the protocol dependency lock from the matching
GitHub release so descriptor and conformance-fixture digests remain auditable.
