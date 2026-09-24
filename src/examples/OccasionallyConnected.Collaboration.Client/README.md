# Occasionally connected collaboration client

This example is the matching client for `OccasionallyConnected.Collaboration.Server`. It uses a stable client id, a local SQLite database, the HTTP transport, and a custom serializer that emits the same activity payload contract as the server.

Start the server first:

```bash
dotnet run --project src/examples/OccasionallyConnected.Collaboration.Server -- --url http://127.0.0.1:5088 --database server.db --credentials "token-a:tenant-a:client-a;token-b:tenant-a:client-b"
```

Publish from client A:

```bash
dotnet run --project src/examples/OccasionallyConnected.Collaboration.Client -- publish --server http://127.0.0.1:5088 --database client-a.db --token token-a --client client-a --status active --title "Launch checklist" --details "Client A created the item"
```

Watch from client B:

```bash
dotnet run --project src/examples/OccasionallyConnected.Collaboration.Client -- watch --server http://127.0.0.1:5088 --database client-b.db --token token-b --client client-b
```

To queue work while offline, run `publish --offline` with client A's database, token, and client id. The command prints the saved operation id. When the server is available, run `watch` with the same database, token, and client id. It reconnects, sends the saved operation with its original id, and resumes the persisted subscription. Running `publish` again would create another operation.

Press Ctrl+C to stop `watch`. The client cancels the command, closes its subscriptions and SQLite store, and exits with the cancellation code. The client accepts its token through `--token`; the server's `--credentials` mapping must include that token and client id.
