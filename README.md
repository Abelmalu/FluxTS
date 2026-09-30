# FluxTS

A single-node time-series database written in Go, focused on the internals:
bounded concurrency, backpressure, durability, and sharding.

## What FluxTS does today

- Accepts batches of samples over a bidirectional gRPC stream.
- Groups them by series identity (metric name + canonicalized labels).
- Stores them in-memory behind an `RWMutex`-guarded map.
- Serves time-range queries with `[start, end)` semantics.
- Ships a load generator that opens N concurrent streams and produces
  simulated CPU / memory / connection samples.

There is no disk persistence, no retention, and no aggregation yet. An ack
currently means "received by the handler", not "durably stored."

## Repository layout

```
cmd/                    Server entrypoint (loads env, starts gRPC server).
config/                 Env-driven Config (GRPC_PORT, ...).
errors/                 Shared sentinel errors.
platform/               Small zap wrapper.
proto/
  fluxts.proto          Wire schema (Write stream + QueryRange).
  pb/                   Generated Go code.
server/                 gRPC server lifecycle: listen, serve, graceful stop.
internal/
  model/                Sample, Label, Series, Batch, SeriesKey.
  memstore/             Concurrent in-memory store (map + RWMutex).
  handler/              FluxHandler — implements the gRPC service.
  service/              Domain services.
  repository/           Persistence layer.
  interfaces/           Domain interfaces.
producer/               Load generator: N concurrent streams, per-series
                        buffers, bounded inflight window, flag-driven config.
```

## Prerequisites

- Go 1.26 or newer (module targets `go 1.26.3`).
- `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` if you regenerate the
  gRPC stubs. The checked-in `proto/pb` is enough to build without them.

## Configuration

The server reads its config from environment variables. A `.env` file at the
repo root is loaded automatically at startup.

| Variable   | Required | Example  | Purpose                          |
| ---------- | -------- | -------- | -------------------------------- |
| `GRPC_PORT`| yes      | `50051`  | Port for the gRPC server         |

Minimal `.env`:

```
GRPC_PORT=50051
```

## Running

### Server

```bash
go run ./cmd
```

You should see:

```
INFO   Starting server on port:   {"port": ":50051"}
```

### Load generator

```bash
go run ./producer \
  -addr localhost:50051 \
  -producers 4 \
  -sps 20 \
  -batch-size 50 \
  -flush-interval 200ms \
  -duration 30s
```

Run `go run ./producer -h` for the full flag list.

## Testing

```bash
# All packages, race detector on.
go test -race ./...

# Just the storage engine.
go test -race ./internal/memstore/...
```

The storage engine has coverage for label canonicalization, range
boundaries, sort ordering, aliasing, and concurrent access under `-race`.

## Wire format

Defined in [proto/fluxts.proto](proto/fluxts.proto):

- `rpc Write(stream Batch) returns (stream Ack)` — bidirectional stream for
  ingestion. One `Batch` carries one `Series` plus its samples.
- `rpc QueryRange(QueryRangeRequest) returns (QueryRangeResponse)` — unary
  time-range query.

Samples are `(google.protobuf.Timestamp timestamp, double value)`. Values
must be finite; NaN / ±Inf are rejected at the storage layer.

## Design decisions

- Timestamps: Unix milliseconds, `int64`.
- Values: finite `float64`.
- Series identity: metric name + labels, sorted before hashing (label order
  does not create new series).
- Label matching: exact only.
- Query boundaries: `[start, end)` — start inclusive, end exclusive.
- Duplicate samples are preserved.
- Delivery semantics: at-least-once.
- Query consistency: per-series snapshot, not global.

## License

See [LICENSE](LICENSE).
