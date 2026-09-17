# OccasionallyConnected.Collaboration.Server

This example is a small ASP.NET Core host for the occasionally-connected HTTP protocol. It binds to loopback by default, stores the server journal in SQLite, and maps caller-supplied development tokens to explicit tenant/client identities before the portable `HttpServerEndpoint` sees a request.

The token header is `X-OC-Demo-Token`. The server does not use a request `TenantHint` as identity; the trusted `ServerAuthenticatedClient` comes only from `OC_DEMO_CREDENTIALS`.

## Run

From `src`:

```powershell
$env:OC_DEMO_CREDENTIALS = "local-client-a-token:tenant-dev:client-a;local-client-b-token:tenant-dev:client-b"
$env:OC_SERVER_DATABASE = "$PWD\.local\oc-server\journal.db"
dotnet run --project examples/OccasionallyConnected.Collaboration.Server/OccasionallyConnected.Collaboration.Server.csproj --framework net8.0
```

The default address is `http://127.0.0.1:5088`. Override it with `OC_SERVER_URL` or `--url`, keeping it on loopback for this development host. Supported command-line switches are `--url`, `--database`, `--credentials`, and `--path-base`; unknown switches and switches without values are rejected.

The runnable host declares batch push, cursor resume, receive acknowledgements, server idempotency, and atomic apply-and-acknowledge for clients that negotiate exactly-once delivery.

## Served Streams

- `collaboration/activity`: custom activity stream implemented by the example. It accepts bounded JSON payloads with `status`, optional `title`, and optional `details`, then writes canonical JSON with server-owned acceptance metadata.
- `collaboration/crdt/g-counter`: built-in grow-only counter CRDT.
- `collaboration/crdt/pn-counter`: built-in positive-negative counter CRDT.
- `collaboration/crdt/or-set`: built-in observed-remove set CRDT.
- `collaboration/crdt/lww-register`: built-in last-writer-wins register CRDT.

## Safety Defaults

The example sets finite request, payload, receive, concurrency, journal and retention bounds. Size limits are converted from KiB with checked arithmetic. The SQLite journal is persistent and is never silently deleted by the application.

The built-in token mapping is deliberately scoped to local development: it listens on loopback by default and accepts `X-OC-Demo-Token` values from `OC_DEMO_CREDENTIALS` or `--credentials`. Production hosts should terminate TLS, authenticate the caller with the service's normal identity system, and create the trusted `ServerAuthenticatedClient` from that authenticated identity before dispatching to the portable endpoint.

## Verification Commands

Run these from `src`:

```powershell
dotnet build tests/ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests/ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests.csproj -c Release -f net8.0 -m:1 --disable-build-servers
dotnet tests/ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests/bin/Release/net8.0/ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests.dll --progress off
```
