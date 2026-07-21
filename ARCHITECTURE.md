# Architecture

## Invariants

1. The distribution contains one AGNT5 binary.
2. PostgreSQL is the only durable source of truth.
3. A database permits one active runtime unless ownership is introduced by a
   future, explicit design.
4. Acknowledged journal appends survive runtime process replacement according
   to PostgreSQL's configured durability policy.
5. Materialized state can be rebuilt from the journal.
6. JWT authenticates callers; the community runtime does not implement RBAC.
7. The configured project is a deployment property, not a caller-controlled
   authorization claim.
8. SDK-visible behavior is verified through shared conformance tests.

## Process composition

```text
agnt5-runtime
  Gateway (HTTP + gRPC + JWT verification)
    Coordinator (workers + leases)
      Processor (journal tail + projections)
        PostgreSQL (journal + materialized state)
```

Each component runs as a task inside the same process. Crate boundaries exist
to keep contracts narrow and to allow the managed AGNT5 runtime to reuse public
behavior without forcing community users to operate multiple services.

## Dependency direction

```text
core <- postgres
core <- processor
core <- coordinator
core <- gateway
core <- telemetry

all public libraries <- runtime binary
```

Storage implementations depend on the public contracts. Core contracts must
never depend on a concrete database, gateway, or managed capability.

