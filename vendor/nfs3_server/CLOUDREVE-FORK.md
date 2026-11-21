# nfs3_server — Cloudreve fork

Upstream: [nfs3_server 0.11.0](https://crates.io/crates/nfs3_server/0.11.0)
(`github.com/Vaiz/nfs3`, commit `1c0681fae059ac544f2e4a7756051b2c261f60df`),
BSD-3-Clause — see `LICENSE`. Imported unchanged in the commit
`chore: vendor nfs3_server 0.11.0 ...`; `git log -p vendor/nfs3_server` shows
every change since. Wired in through `[patch.crates-io]` in the workspace
`Cargo.toml`. Every changed spot is marked `Cloudreve fork`.

Why: the client serves each on-demand drive over NFSv3 on `127.0.0.1` and
mounts it with `mount_nfs`. Loopback keeps the network out, not other local
processes (another account, a sandboxed app with `network.client`).

| Change | Where |
|---|---|
| `CallerPolicy` (opt-in via `NFSTcpListener::set_caller_policy`): refuse calls whose AUTH_UNIX uid is not allowed, or with no AUTH_UNIX at all (NULL ping excepted) — `NFS3ERR_ACCES` with an empty `resfail` per procedure, `MNT3ERR_ACCES` for MNT | `src/caller_policy.rs`, `src/rpcwire.rs` |
| `CallerPolicy::single_mount`: only the first successful MNT gets the root handle; MNT offers AUTH_UNIX only under a policy | `src/mount_handlers.rs` |
| Accept loop survives accept errors (EMFILE, ECONNABORTED) with a backoff instead of returning; no panic when the peer is gone before `peer_addr` | `src/tcp.rs` |
| Connections stop when their listener is dropped (its task aborted) instead of serving forever from detached tasks | `src/tcp.rs` |
| `Invalid Mount Version` logged at debug (mount_nfs probes MOUNT v1 on every mount); `Accepting connection` at debug | `src/mount_handlers.rs`, `src/tcp.rs` |

The random handle generation uses upstream's own `bind_with_generation`.
