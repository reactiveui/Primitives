# OccasionallyConnected Collaboration Client

This console example pairs with the `OccasionallyConnected.Collaboration.Server` example. It uses a stable client id, a local SQLite database, HTTP transport, and the activity payload contract shared with the server.

These examples accompany the first v1 release of OccasionallyConnected. The feature has no earlier released version, so end users do not need a migration.

## Run the examples

Open two terminals at the repository root. Start the server in the first terminal:

```powershell
$env:OC_DEMO_CREDENTIALS = "token-a:tenant-a:client-a;token-b:tenant-a:client-b"
dotnet run --project src/examples/OccasionallyConnected.Collaboration.Server/OccasionallyConnected.Collaboration.Server.csproj --framework net8.0
```

The server listens at `http://127.0.0.1:5088` and creates its default journal under its application directory. In the second terminal, publish an activity as client A:

```powershell
dotnet run --project src/examples/OccasionallyConnected.Collaboration.Client/OccasionallyConnected.Collaboration.Client.csproj --framework net8.0 -- publish --server http://127.0.0.1:5088 --database .\client-a.db --token token-a --client client-a --status active --title "Launch checklist" --details "Client A created the item"
```

Start a watcher as client B in a third terminal to see activity updates:

```powershell
dotnet run --project src/examples/OccasionallyConnected.Collaboration.Client/OccasionallyConnected.Collaboration.Client.csproj --framework net8.0 -- watch --server http://127.0.0.1:5088 --database .\client-b.db --token token-b --client client-b
```

Press Ctrl+C to stop `watch`. The client closes its subscriptions and SQLite store when canceled.

To queue an update while the server is unavailable, run this command with client A's database, token, and client id:

```powershell
dotnet run --project src/examples/OccasionallyConnected.Collaboration.Client/OccasionallyConnected.Collaboration.Client.csproj --framework net8.0 -- publish --offline --server http://127.0.0.1:5088 --database .\client-a.db --token token-a --client client-a --status draft
```

The command persists the operation and prints its id, then exits without starting synchronization. Once the server is available, start `watch` with the same database, token, and client id. The context starts synchronization and retries the saved operation with its original id. Keep `watch` running until the operation synchronizes. Running `publish` again creates a separate operation.

The client token must appear in the server's `OC_DEMO_CREDENTIALS` mapping with the matching tenant and client ids.

## Test the client example

Run from `src`:

```powershell
dotnet test tests/ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client.Tests/ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client.Tests.csproj -c Release -f net8.0
```
