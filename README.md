# I3D: Increasing Integer Identifier service

[![ci](https://github.com/pengubco/i3d/actions/workflows/ci.yml/badge.svg)](https://github.com/pengubco/i3d/actions/workflows/ci.yml)

I3D issues globally unique 64-bit integer IDs over gRPC. Each node runs the
[snowflake](https://en.wikipedia.org/wiki/Snowflake_ID) algorithm, and nodes coordinate
through [etcd](https://etcd.io/) so that no two nodes ever share a worker ID or reuse a
timestamp range.

- **Unique** across nodes and restarts, even with clock skew, network partitions or
  paused processes. See [the design](docs/DESIGN.md).
- **Increasing** per node. Roughly time-ordered across nodes.
- **Scalable** to 1024 nodes, each issuing up to 4096 IDs per millisecond.
- **Stateless nodes.** All coordination state lives in etcd. A node keeps serving for a few
  seconds if etcd is briefly unreachable.
- **Consistent over available.** When a node can't guarantee uniqueness it returns
  `UNAVAILABLE`, and clients retry on another node.

## ID layout

```
| 0 | 41 bits: ms since 2025-01-01 UTC | 10 bits: worker ID | 12 bits: sequence |
```

IDs are positive `int64`s and the timestamps last until 2094.

## API

[`api/i3d/v1/i3d.proto`](api/i3d/v1/i3d.proto):

```proto
service IDService {
  rpc NextID(NextIDRequest) returns (NextIDResponse);     // one ID
  rpc NextIDs(NextIDsRequest) returns (NextIDsResponse);  // 1..4096 increasing IDs
}
```

Each node also serves the standard `grpc.health.v1.Health` service and gRPC reflection. On
its HTTP port it serves `/metrics` (Prometheus), `/livez` and `/readyz`.

## Quick start

You need Go 1.27+ and [etcd](https://etcd.io/docs/latest/install/) 3.5+.

```bash
# 1. Start etcd and create a user that can read and write the /i3d/ prefix (see Appendix).
# 2. Build and run one node. All listeners bind to 127.0.0.1 by default.
make build
export I3D_ETCD_PASSWORD='<the i3d user password>'
./build/i3d -config example/config/localhost.json

# 3. Get IDs, e.g. with grpcurl (https://github.com/fullstorydev/grpcurl).
grpcurl -plaintext 127.0.0.1:7669 i3d.v1.IDService/NextID
grpcurl -plaintext -d '{"count": 3}' 127.0.0.1:7669 i3d.v1.IDService/NextIDs
```

### Docker

```bash
make docker
# Publish to 127.0.0.1 only. The container listens on 0.0.0.0 inside its own network.
docker run --rm --name i3d \
  -e I3D_ETCD_PASSWORD \
  --mount type=bind,source="$(pwd)"/example/config,target=/config,readonly \
  -p 127.0.0.1:7669:7669 -p 127.0.0.1:8001:8001 \
  i3d -config /config/docker.json
```

### Kubernetes (demo)

The manifests in [`example/k8s`](example/k8s) run a throwaway single-node etcd and five
i3d replicas. Both Services are `ClusterIP`, so nothing is reachable from outside the
cluster.

```bash
make docker            # then load the image into your cluster, e.g. `kind load docker-image i3d:latest`
kubectl create namespace i3d
kubectl apply -f example/k8s/etcd.yaml
# Set up users and auth inside the etcd pod (see Appendix), using `kubectl -n i3d exec -it etcd -- sh`.
kubectl -n i3d create secret generic i3d-etcd --from-literal=password='<the i3d user password>'
kubectl apply -f example/k8s/i3d.yaml

kubectl -n i3d port-forward svc/i3d 7669:7669 &
grpcurl -plaintext 127.0.0.1:7669 i3d.v1.IDService/NextID
```

The demo etcd keeps its data in an `emptyDir`. In production, run a durable etcd cluster.
I3D's uniqueness guarantee depends on the data stored there.

## Configuration

`i3d -config <file.json>`. Unknown fields are rejected.

| Field                  | Default          | Description |
|------------------------|------------------|-------------|
| `grpc_addr`            | `127.0.0.1:7669` | gRPC listen address. |
| `grpc_tls`             | off              | `cert_file`, `key_file`, and optional `client_ca_file` to require client certificates (mutual TLS). |
| `allow_insecure_grpc`  | `false`          | Required to serve plaintext gRPC on a non-loopback address. Only use it on a private network. |
| `http_addr`            | `127.0.0.1:8001` | `/metrics`, `/livez`, `/readyz`. |
| `debug_addr`           | off              | Serves `/debug/pprof`. Must be loopback. |
| `log_level`            | `info`           | `debug`, `info`, `warn`, `error`. Logs are JSON. |
| `max_workers`          | `1024`           | Worker IDs shared by the cluster, 1–1024. Must match on every node. |
| `etcd.endpoints`       | required         | etcd client endpoints. |
| `etcd.prefix`          | required         | Key prefix. One prefix is one cluster. |
| `etcd.username`        |                  | etcd user. |
| `etcd.password_file`   |                  | File containing the password. `$I3D_ETCD_PASSWORD` takes precedence. Passwords are never read from the JSON file. |
| `etcd.tls`             | off              | `ca_file`, and `cert_file` + `key_file` for client certificate auth. |

### Security notes

- **Bind addresses.** Everything binds to loopback by default. Plaintext gRPC on another
  address must be enabled explicitly with `allow_insecure_grpc`. For callers outside a
  private network, enable `grpc_tls` with `client_ca_file`.
- **HTTP port.** `/metrics` and the health endpoints are unauthenticated. Keep that port on a
  private network.
- **etcd access.** The etcd user needs read/write access to its prefix only.

## Development

```bash
make test    # unit and integration tests (embedded etcd, no external services), with -race
make lint    # golangci-lint v2
make vuln    # govulncheck
make proto   # regenerate api/i3d/v1 (requires protoc; Go plugins are pinned in go.mod)
```

Layout:

| Path                  | Contents |
|-----------------------|----------|
| `cmd/i3d`             | Binary: config, listeners, graceful shutdown. |
| `api/i3d/v1`          | Protobuf API and generated code. |
| `internal/snowflake`  | ID generator: layout, clock handling, reservation limit. |
| `internal/worker`     | etcd coordination: claiming worker IDs, reservations, leases. |
| `internal/server`     | gRPC service and error mapping. |
| `internal/config`     | Config loading and validation. |

## Migrating from v1

This release is not compatible with v1. Don't run v1 and v2 nodes against the same etcd prefix.

- IDs count milliseconds from 2025-01-01 instead of 1970, so they are smaller than v1 IDs.
- The proto package is now `i3d.v1`, with service `IDService` and methods `NextID` and
  `NextIDs`. `NextIDs` is unary and returns a list of IDs instead of a stream.
- Config: the `-config_from_file` flag is now `-config`, `max_server_cnt` is now
  `max_workers`, and the `etcd_*` fields moved under `etcd`. Passwords are read from
  `$I3D_ETCD_PASSWORD` or `etcd.password_file`.
- etcd keys: `workers/last_reported_time/<id>` is replaced by `workers/reserved_until/<id>`.
- HTTP: `/ready` is now `/readyz` (returns 503 when not ready), and `/livez` was added. pprof
  moved to the opt-in, loopback-only `debug_addr`.

## Appendix: etcd user setup

Run these with `etcdctl` against your etcd (inside the pod for the Kubernetes demo).
Choose your own passwords. `etcdctl` prompts for them when you leave out `--new-user-password`.

```bash
etcdctl user add root
etcdctl user grant-role root root
etcdctl user add i3d
etcdctl role add i3d
etcdctl role grant-permission --prefix=true i3d readwrite /i3d/
etcdctl user grant-role i3d i3d
etcdctl auth enable

# verify
etcdctl --user i3d put /i3d/check ok && etcdctl --user i3d del /i3d/check
```

## License

[MIT](LICENSE.md)
