# Daemon Protocol Implementation

The reusable `nix_daemon_protocol` package implements the [Nix daemon wire protocol][nix-protocol] using a Pydantic-based serialization framework. Every protocol message (request and response) is a Pydantic model that auto-generates its own binary encoder and decoder from Python type annotations. `pynixd` consumes this package for daemon transport and execution.

## Supported protocol versions

The codec package supports the contiguous **1.35 through 1.38** interval. 1.35
is the floor for a client, a store and a builder alike: Lix and nixbuild.net
speak it. Fast in-memory compatibility tests run every protocol boundary in
that interval. Real-daemon tests run the current Nix protocol, and no
real-daemon test runs 1.35. Requests also declare their introduction
version and reject attempts to send an operation to an older negotiated daemon.

## Deserialization diagnostics

The protocol package reports malformed wire data through an optional logging
adapter. It uses structlog when the embedding environment has it installed and
falls back to the standard-library `nix_daemon_protocol` logger otherwise. The
package never configures either logging system. Consumers can also provide a
logger in `ReadContext`.

Recursive decode failures produce one `daemon_deserialization_failed` event at
the outermost message boundary, then re-raise the original exception. Events
contain message type, protocol version, operation (when known), reader ID, and
exception type; they never include raw wire payload data.

[nix-protocol]: https://github.com/NixOS/nix/blob/master/src/libstore/build/worker-protocol.hh

## How it works

You write a normal Pydantic model:

```python
class IsValidPathResponse(WireResponse):
    """IsValidPath response — valid bool as uint64 on wire."""

    valid: bool
```

And it automatically knows how to:
- Write `valid` as a `uint64` (0 or 1) on the wire
- Read a `uint64` back from the wire into a `bool`
- Render a human-readable `__init__` signature for IDE autocompletion
- Serialize to/from JSON for debugging

No manual `read_uint64` / `write_uint64` calls. Types are inferred from annotations.

## Base classes

### `WireModel` — the engine

`WireModel` is an abstract base class inheriting from Pydantic's `BaseModel`. It introspects the model's type annotations and synthesizes two methods:

| Method | Purpose |
|---|---|
| `to_writer(ctx)` | Iterates fields in declaration order, writing each to the wire |
| `from_reader(ctx)` | Classmethod — reads fields from the wire in declaration order |

Class variables (`ClassVar[T]`) are **skipped** by default — they hold protocol constants (like op codes) that are handled by the transport layer, not the message body.

Nested `WireModel` fields are serialized **inline** — the child's fields are written directly into the parent's wire stream with no length prefix. This matches the Nix daemon's flat wire format.

### `WireString` — single string on the wire

Some types are represented as a **single length-prefixed UTF-8 string** on the wire — `StorePath` (`"/nix/store/hash-name"`), `Signature` (`"name:base64sig"`), `DerivedPath` (`"drv!out"`), etc. These inherit from `WireString`, which overrides `to_writer`/`from_reader` to read/write the whole object as one string.

Multi-field `WireString` subclasses (like `Signature` and `DrvOutput`) override `from_str`/`to_str` to parse/format with delimiters.

### `WireRequest` / `WireResponse` — operation envelopes

`WireRequest` adds:
- An **op code** (`uint64`) written before the message body
- Auto-registration in `WIRE_REGISTRY` (op code → request class)
- ClassVars for `response_type` and the operation code. The base model also
  carries neutral capability metadata used by consumers; daemon dispatch policy
  belongs to consumers such as `pynixd`, not the reusable codec package.

`WireResponse` adds:
- A `logs: WireLogs` field — the daemon's stderr stream, written/read before body fields

Here's a complete operation pair:

```python
class BuildDerivationRequest(WireRequest):
    op: ClassVar[int] = 36
    response_type = BuildDerivationResponse
    forward: ClassVar[bool] = False  # never forwarded — handled by pynixd

    drv_path: StorePath
    derivation: BasicDerivation
    build_mode: int


class BuildDerivationResponse(WireResponse):
    result: BuildResult
```

## Wire type dispatch

The serialization engine maps Python types to wire primitives:

| Python type | Wire format |
|---|---|
| `int` | `uint64` |
| `IntEnum` | `uint64` (value) |
| `str` | length-prefixed UTF-8 |
| `bool` | `uint64` (0 or 1) |
| `bytes` | length-prefixed bytes |
| `list[T]` | `uint64` count, then N elements |
| `set[T]` | `uint64` count, then N elements |
| `dict[K, V]` | `uint64` count, then N key-value pairs |
| `Optional[T]` | same as `T` (absent/none is context-dependent) |
| `WireString` subclass | single length-prefixed string |
| `WireModel` subclass | inline field-by-field |

Special case: `Optional[StorePath]` writes `""` for `None` — this is Nix's convention for an absent deriver field.

## Version constraints

The Nix daemon protocol evolved across versions. Some fields only exist on the wire at certain protocol versions. `WireField` supports this via `min_version` and `max_version`:

```python
times_built: int | None = WireField(default=None, min_version=proto(1, 29))
cpu_user: OptMicroseconds = WireField(default_factory=OptMicroseconds, min_version=proto(1, 37))
```

Fields outside the negotiated version range are skipped during both serialization and deserialization.

## Conditional fields

Some fields depend on the **value of another field** at runtime — they only appear on the wire under certain conditions. `WireField` supports this via `wire_depends_on`, a predicate that receives `self`:

```python
info: UnkeyedValidPathInfo | None = WireField(
    default=None,
    wire_depends_on=lambda self: self.valid,
)
```

During deserialization, `valid` is read first. If it's `False`, `info` is skipped — the field stays at its default `None`.

## Custom serialization

Some types override `from_reader`/`to_writer` entirely, bypassing field-by-field serialization:

| Type | Reason | Wire format |
|---|---|---|
| `Realisation` | JSON-encoded on wire | length-prefixed UTF-8 containing JSON |
| `Time` / `TimeSpan` | Single `uint64` — faster | raw `uint64` |
| `WireLogs` | Tagged-union stream | `[uint64 code][body]...[STDERR_LAST]` |

## Stderr stream

Every response carries a **stderr stream** (`WireLogs`) — a sequence of tagged-union messages:

```
[uint64 STDERR_NEXT][length-prefixed string]
[uint64 STDERR_START_ACTIVITY][activity fields...]
[uint64 STDERR_STOP_ACTIVITY][activity id]
[uint64 STDERR_RESULT][result fields...]
[uint64 STDERR_ERROR][error fields...]    ← terminates the stream, no STDERR_LAST after
[uint64 STDERR_LAST]                       ← normal end of stream
```

`WireLogs.from_reader` reads messages until it hits `STDERR_LAST` or a `LogError`. `WireLogs.to_writer` writes each message followed by `STDERR_LAST`.

## Type reference

The following pages catalogue every type, generated from source code:

```{toctree}
:maxdepth: 1

requests
reusable
infrastructure
```
