# The store model: what pynixd takes from Nix's store classes

Status: accepted by User, 2026-11-04, at nixidae ca0d513c. Commits 1–4 are
implemented. It prepares `ssh-ng://` substituters for nixidae/nixidae#1155
(`daemonless.md`). Nix 2.35.2 is the reference.

## What Nix does

- Every store type is a pair: `FooConfig : virtual StoreConfig` holds the
  settings, and `FooConfig::openStore()` makes the `Foo : virtual Store`
  (`store-api.hh:206`).
- `StoreConfig` gives **every** store `trusted` (default false), `priority`
  (default 0) and `want-mass-query` (default false) (`store-api.hh:275-300`).
  A binary cache takes the default of the last two from `nix-cache-info`; a URL
  parameter still wins (`binary-cache-store.cc:72`).
- A registry maps a URL scheme to a config (`store-registration.hh:54`).
  `getDefaultSubstituters` opens each URL, drops one that fails with a warning,
  and sorts by `priority` (`store-registration.cc:114`).
- A substitution goal asks any store the same three things: `queryPathInfo`,
  `narFromPath` (through `copyStorePath`) and `queryRealisation`. It checks
  signatures unless `sub->config.isTrusted` (`substitution-goal.cc:116`, `:236`).
- Optional abilities are mixins: `GcStore`, `LogStore`, `IndirectRootStore`.

## What pynixd has already

- `StoreSpecBase` is Nix's `StoreConfig`, and `Spec.to_store()` is
  `openStore()`. This part does not change: no class renames, no new hierarchy.
- The roles (`scheduleable`, `no_schedule`, `gc_enabled`, `last_resort`) are
  settings on the spec, as Nix's are. They stay settings.
- A substituter is a `no_schedule` store (`SubstitutionQueue.substituter_stores`).
  A builder is not one. A build output comes back through `store/transfer.py`,
  not through the substituter trust check.

## The proposal

1. **One registry.** `pynixd/store_url.py` maps a scheme to a function from a
   parsed URL to a spec, or `None`. `None` means the daemon answers for that
   substituter. An unknown query parameter also gives `None`, so pynixd never
   ignores a setting the user wrote. It replaces the two parsers:
   `substituters.py` (client substituters) and `instance.py:190-250` (the
   daemon's `nix.conf`). The registry also gives the key that tells two
   spellings of one URL apart (Nix's `getReference`).
2. **`trusted` and `priority` on every spec.** The registry sets both from the
   URL, with Nix's defaults: `trusted=false`, `priority=0`, and a cache's
   `nix-cache-info` for the cache default. A spec in the configuration file
   keeps today's rule: a non-cache peer is trusted (nixidae/nixidae#1008).
   `_trusted_source` becomes `store.trusted`.
   This changes one answer. Today pynixd trusts a `unix://` substituter
   without `?trusted=1`. After the change it checks signatures, as Nix does.
3. **Three read methods on `Store`, as Nix's virtual methods.** A path's info
   is already op 26 on every store. `download_size(path)` (0 when unknown),
   `read_nar(path, nar_size)` and `find_realisation(key, drv_path, by_path=)`
   are new. The base reads over the worker protocol, so `DaemonStore` and
   `LocalDBStore` need no override; `BinaryCacheStore` overrides all three.
   No new base class: `Store` is Nix's `Store`. Nix's mixins (`GcStore`,
   `LogStore`) become Protocols when one is needed; no multiple inheritance.

Each `is_binary_cache` site, and what it becomes:

| site | use | becomes |
| --- | --- | --- |
| `substitution_queue.py:101` | trust | `store.trusted` |
| `substitution_queue.py:562` | download size | `Store.download_size` |
| `substitution_queue.py:636` | NAR stream | `Store.read_nar` |
| `goals/realisations.py:156` | realisation | `Store.find_realisation` |
| `gc.py:517` | `WantMassQuery` | `store.want_mass_query`, every store |
| `substituters.py:133`, `:135` | dedup key | the registry key |
| `instance.py:198`, `:230` | dedup key | the registry key |
| `substitution_queue.py:383`, `:454`, `:471`, `:485` | narinfo race | stays: only a cache serves `.narinfo` |

Nine sites go and four stay. The narinfo race is a cache ability, as Nix's
`BinaryCacheStore` is a specialisation.

## What pynixd does not take

- `ssh://` (`LegacySSHStore`), `mounted-ssh-ng://`, `s3://`, the overlay store.
  User's decision, 2026-11-04. Each stays on the daemon.
- The `Config`/`Store` class pair: the spec already is the config.
- Virtual multiple inheritance.

## Where `ssh-ng://` fits

`ssh-ng://[user@]host[:port]` becomes an `SSHSubprocessStoreSpec` with
`no_schedule`, not `scheduleable`, no GC, no probe builds, `monitor=False` and
no agent upload. The parameters it reads: `ssh-key`,
`base64-ssh-public-host-key`, `compress`, `remote-program`, `remote-store`,
`trusted`, `priority`. Any other gives `None`.

- Host keys: `base64-ssh-public-host-key` is the only key accepted. Without
  it, `known_hosts` lists OpenSSH's four default files and pynixd reads the
  ones that exist. No file accepts no key. `None` ("accept any key") is never
  used. asyncssh alone reads only `~/.ssh/known_hosts` (nixidae/nixidae#1172).
- The command is Nix's: `remote-program --stdio`, then `--store remote-store`
  when given (`ssh-store.cc:227`).
- A connect gives up after 10 s. A client's substituter whose start fails
  stays with the daemon, and the client op goes on.

A bare `ssh-ng://` has priority 0, so it ranks before `cache.nixos.org` (40).
Nix ranks it the same way.

## Commits

1. The registry, for `http`, `https`, `file` and `unix`. No answer changes.
2. `trusted` and `priority` on every spec; `_trusted_source` reads `trusted`.
3. The three read methods on `Store`, and `want_mass_query`.
4. The `ssh-ng` scheme.
5. A functional test with an `sshd` in the guest.
