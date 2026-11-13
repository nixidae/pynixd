# Requests & Responses

All Nix daemon protocol operations, organized by op code. Each operation module defines a `*Request` (subclass of `WireRequest`) and a `*Response` (subclass of `WireResponse`).

The standard operations below live in `nix_daemon_protocol`. Operations in the `pynixd.daemon_extensions` namespace are private pynixd additions beyond the standard Nix daemon protocol. Forwarding and extension fallback are pynixd daemon-dispatch policy, not codec behavior.

---

## Standard Operations

### Op 1 — IsValidPath

```{eval-rst}
.. automodule:: nix_daemon_protocol.is_valid_path
   :members:
```

---

### Op 6 — QueryReferrers

```{eval-rst}
.. automodule:: nix_daemon_protocol.query_referrers
   :members:
```

---

### Op 7 — AddToStore (streaming request)

```{eval-rst}
.. automodule:: nix_daemon_protocol.add_to_store
   :members:
```

---

### Op 9 — BuildPaths

`forward = False`

```{eval-rst}
.. automodule:: nix_daemon_protocol.build_paths
   :members:
```

---

### Op 10 — EnsurePath

```{eval-rst}
.. automodule:: nix_daemon_protocol.ensure_path
   :members:
```

---

### Op 11 — AddTempRoot

```{eval-rst}
.. automodule:: nix_daemon_protocol.add_temp_root
   :members:
```

---

### Op 12 — AddIndirectRoot

```{eval-rst}
.. automodule:: nix_daemon_protocol.add_indirect_root
   :members:
```

---

### Op 14 — FindRoots

```{eval-rst}
.. automodule:: nix_daemon_protocol.find_roots
   :members:
```

---

### Op 19 — SetOptions

```{eval-rst}
.. automodule:: nix_daemon_protocol.set_options
   :members:
```

---

### Op 20 — CollectGarbage

```{eval-rst}
.. automodule:: nix_daemon_protocol.collect_garbage
   :members:
```

---

### Op 23 — QueryAllValidPaths

```{eval-rst}
.. automodule:: nix_daemon_protocol.query_all_valid_paths
   :members:
```

---

### Op 26 — QueryPathInfo

```{eval-rst}
.. automodule:: nix_daemon_protocol.query_path_info
   :members:
```

---

### Op 29 — QueryPathFromHashPart

```{eval-rst}
.. automodule:: nix_daemon_protocol.query_path_from_hash_part
   :members:
```

---

### Op 31 — QueryValidPaths

```{eval-rst}
.. automodule:: nix_daemon_protocol.query_valid_paths
   :members:
```

---

### Op 32 — QuerySubstitutablePaths

```{eval-rst}
.. automodule:: nix_daemon_protocol.query_substitutable_paths
   :members:
```

---

### Op 33 — QueryValidDerivers

```{eval-rst}
.. automodule:: nix_daemon_protocol.query_valid_derivers
   :members:
```

---

### Op 34 — OptimiseStore

```{eval-rst}
.. automodule:: nix_daemon_protocol.optimise_store
   :members:
```

---

### Op 35 — VerifyStore

```{eval-rst}
.. automodule:: nix_daemon_protocol.verify_store
   :members:
```

---

### Op 36 — BuildDerivation

`forward = False`

```{eval-rst}
.. automodule:: nix_daemon_protocol.build_derivation
   :members:
```

---

### Op 37 — AddSignatures

```{eval-rst}
.. automodule:: nix_daemon_protocol.add_signatures
   :members:
```

---

### Op 38 — NarFromPath (streaming response)

```{eval-rst}
.. automodule:: nix_daemon_protocol.nar_from_path
   :members:
```

---

### Op 39 — AddToStoreNar (streaming request)

```{eval-rst}
.. automodule:: nix_daemon_protocol.add_to_store_nar
   :members:
```

---

### Op 40 — QueryMissing

```{eval-rst}
.. automodule:: nix_daemon_protocol.query_missing
   :members:
```

---

### Op 41 — QueryDerivationOutputMap

```{eval-rst}
.. automodule:: nix_daemon_protocol.query_derivation_output_map
   :members:
```

---

### Op 42 — RegisterDrvOutput

```{eval-rst}
.. automodule:: nix_daemon_protocol.register_drv_output
   :members:
```

---

### Op 43 — QueryRealisation

```{eval-rst}
.. automodule:: nix_daemon_protocol.query_realisation
   :members:
```

---

### Op 44 — AddMultipleToStore (streaming request)

```{eval-rst}
.. automodule:: nix_daemon_protocol.add_multiple_to_store
   :members:
```

---

### Op 45 — AddBuildLog

```{eval-rst}
.. automodule:: nix_daemon_protocol.add_build_log
   :members:
```

---

### Op 46 — BuildPathsWithResults

`forward = False`

```{eval-rst}
.. automodule:: nix_daemon_protocol.build_paths_with_results
   :members:
```

---

### Op 47 — AddPermRoot

```{eval-rst}
.. automodule:: nix_daemon_protocol.add_perm_root
   :members:
```

---

## Pynixd Extensions

### Op 101 — PynixdCollectGarbage

```{eval-rst}
.. automodule:: pynixd.daemon_extensions.pynixd_collect_garbage
   :members:
```

---

### Op 104 — QueryClosure

```{eval-rst}
.. automodule:: pynixd.daemon_extensions.query_closure
   :members:
```

---

### Op 105 — QueryClosureWithInfo

```{eval-rst}
.. automodule:: pynixd.daemon_extensions.query_closure_with_info
   :members:
```

---

### Op 106 — QueryDerivationOutputMapBatch

```{eval-rst}
.. automodule:: pynixd.daemon_extensions.query_derivation_output_map_batch
   :members:
```

---

### Op 107 — SignPathInfo

```{eval-rst}
.. automodule:: pynixd.daemon_extensions.sign_path_info
   :members:
```

---

### Op 108 — ProbeSystems

```{eval-rst}
.. automodule:: pynixd.daemon_extensions.probe_systems
   :members:
```

---

### Op 109 — ProbeFeatures

```{eval-rst}
.. automodule:: pynixd.daemon_extensions.probe_features
   :members:
```
