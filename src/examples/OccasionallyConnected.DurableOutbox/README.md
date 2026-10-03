# OccasionallyConnected Durable Outbox Example

This console example shows the durable local storage workflow used by adapter authors. It stores readings and operation state in SQLite through `SqliteLocalStoreAdapter`. It also shows JSON payload registration, durable receipts, subscription identity, leases, and attempt barriers.

The example has no server. `simulate-attempt` records local durable transitions around a simulated network attempt. It does not send a request or prove a remote delivery guarantee.

These examples accompany the first v1 release of OccasionallyConnected. The feature has no earlier released version, so end users do not need a migration.

## Run the example

Run these commands from `src` with the .NET 10 SDK or later. The first command appends a reading to `outbox.db` in the current directory.

```powershell
$append = dotnet run --project examples/OccasionallyConnected.DurableOutbox --framework net10.0 -- append-reading --database .\outbox.db --device device-a --value 21.5 --guarantee at-least-once
$append
$operationId = ($append | Where-Object { $_ -like 'operation: *' } | Select-Object -First 1).Substring('operation: '.Length)
dotnet run --project examples/OccasionallyConnected.DurableOutbox --framework net10.0 -- status --database .\outbox.db
dotnet run --project examples/OccasionallyConnected.DurableOutbox --framework net10.0 -- status --database .\outbox.db --operation $operationId
dotnet run --project examples/OccasionallyConnected.DurableOutbox --framework net10.0 -- simulate-attempt --database .\outbox.db --outcome lost-response
dotnet run --project examples/OccasionallyConnected.DurableOutbox --framework net10.0 -- pending --database .\outbox.db
dotnet run --project examples/OccasionallyConnected.DurableOutbox --framework net10.0 -- subscription --database .\outbox.db
dotnet run --project examples/OccasionallyConnected.DurableOutbox --framework net10.0 -- subscribe --database .\outbox.db --take 2
dotnet run --project examples/OccasionallyConnected.DurableOutbox --framework net10.0 -- guarantees
dotnet run --project examples/OccasionallyConnected.DurableOutbox --framework net10.0 -- --demo
```

`append-reading` prints the operation id, which the commands above pass to `status --operation`. `--demo` runs in an owned temporary directory and removes that directory when it finishes. The named database commands keep their database so you can inspect it across runs.

The sample limits the pending outbox to four operations and SQLite worker input to 1 MB. `at-most-once` and `at-least-once` can be demonstrated locally. `effectively-once` is rejected because this example has no server idempotency ledger, atomic server apply-and-acknowledge, or negotiated retention window.

## Test the example

Run from `src`:

```powershell
dotnet test tests/ReactiveUI.Primitives.OccasionallyConnected.Examples.Tests/ReactiveUI.Primitives.OccasionallyConnected.Examples.Tests.csproj -c Release -f net10.0
```
