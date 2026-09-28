# OccasionallyConnected Collaboration Server

This example hosts the OccasionallyConnected HTTP protocol in ASP.NET Core. It binds to loopback by default and stores its journal in SQLite. The host maps local development tokens to tenant and client identities before passing requests to the portable `HttpServerEndpoint`.

The token header is `X-OC-Demo-Token`. A request's `TenantHint` does not establish identity. The server creates the trusted `ServerAuthenticatedClient` from the configured development credentials.

These examples accompany the first v1 release of OccasionallyConnected. The feature has no earlier released version, so end users do not need a migration.

## Run the server

Run these commands from `src` with the .NET 8 SDK or later:

```powershell
$env:OC_DEMO_CREDENTIALS = "token-a:tenant-a:client-a;token-b:tenant-a:client-b"
$env:OC_SERVER_DATABASE = "$PWD\.local\oc-server\journal.db"
dotnet run --project examples/OccasionallyConnected.Collaboration.Server/OccasionallyConnected.Collaboration.Server.csproj --framework net8.0
```

The server listens at `http://127.0.0.1:5088`. The parent directory for the database is created as needed. The database is persistent and the application does not delete it. The host also accepts `--url`, `--database`, `--credentials`, and `--path-base`; each switch takes a value. The URL must remain a loopback HTTP address for this development host.

The host declares batch push, cursor resume, receive acknowledgements, server idempotency, and atomic apply-and-acknowledge for clients that negotiate exactly-once delivery.

## Served streams

- `collaboration/activity` accepts bounded JSON updates with `status` and optional `title` and `details`. The server stores canonical JSON with server-owned acceptance metadata.
- `collaboration/crdt/g-counter` is a grow-only counter.
- `collaboration/crdt/pn-counter` is a positive-negative counter.
- `collaboration/crdt/or-set` is an observed-remove set.
- `collaboration/crdt/lww-register` is a last-writer-wins register.

## Development safety

The host sets finite request, payload, receive, concurrency, journal, and retention limits. Its token mapping is for local development only. A production host should terminate TLS, authenticate callers through its identity system, and construct `ServerAuthenticatedClient` from that trusted identity before dispatching requests.

## Test the server example

Run from `src`:

```powershell
dotnet test tests/ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests/ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Server.Tests.csproj -c Release -f net8.0
```
