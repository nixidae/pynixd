# pynixd-store

The store engine of pynixd: a local Nix store without a Nix daemon. It owns
the store's file system, its database, its locks and its roots, and it has
Nix's method names (`add_to_store`, `register_valid_paths`,
`query_path_info`).

It imports no part of the daemon frontend. It takes its data types (store
paths, path information, derivations) from `nix-daemon-protocol`, and no
request or response model. `docs/design/store-engine.md` in pynixd gives the
layers and the boundary.
