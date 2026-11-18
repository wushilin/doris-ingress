# Vendored copy

Source: https://github.com/wushilin/doris_rust_stream_load at commit 428748e (version 0.2.0).

The only change is in `Cargo.toml`: three upper version bounds are removed, because they conflict
with this project's dependencies (DataFusion needs `url` 2.5.7 or newer; this project uses `tokio`
1.54):

| Dependency | Upstream | Here |
|---|---|---|
| `url` | `>=2, <2.5` | `2` |
| `tokio` | `>=1.0, <1.52` | `1` |
| `indexmap` | `>=2, <2.8` | `2` |

Once upstream relaxes these bounds, replace this directory with a git or crates.io dependency.
