# OccasionallyConnected Durable Outbox Example

This console app teaches the public low-level durable storage workflow used by adapter authors.
It uses `SqliteLocalStoreAdapter`, `JsonPayloadSerializer`, source-generated `JsonTypeInfo`,
durable receipts, persisted subscription identity, leases, and attempt barriers against a real
SQLite database chosen by the user.

It intentionally does not pretend to have a server. `simulate-attempt` is explicitly local: it
records the same durable transitions an engine would need around network I/O, then prints whether a
local simulated acknowledgement was recorded.

Run from `src`:

```powershell
dotnet run --project examples/OccasionallyConnected.DurableOutbox --framework net10.0 -- `
  append-reading --database .\outbox.db --device device-a --value 21.5 --guarantee at-least-once
dotnet run --project examples/OccasionallyConnected.DurableOutbox --framework net10.0 -- status --database .\outbox.db
dotnet run --project examples/OccasionallyConnected.DurableOutbox --framework net10.0 -- status --database .\outbox.db --operation <operation-guid>
dotnet run --project examples/OccasionallyConnected.DurableOutbox --framework net10.0 -- simulate-attempt --database .\outbox.db --outcome lost-response
dotnet run --project examples/OccasionallyConnected.DurableOutbox --framework net10.0 -- pending --database .\outbox.db
dotnet run --project examples/OccasionallyConnected.DurableOutbox --framework net10.0 -- subscription --database .\outbox.db
dotnet run --project examples/OccasionallyConnected.DurableOutbox --framework net10.0 -- subscribe --database .\outbox.db --take 2
dotnet run --project examples/OccasionallyConnected.DurableOutbox --framework net10.0 -- guarantees
dotnet run --project examples/OccasionallyConnected.DurableOutbox --framework net10.0 -- --demo
```

Delivery guarantee examples:

- `at-most-once` writes a durable local audit record and records one attempt barrier. A lost response becomes ambiguous and is not retried automatically.
- `at-least-once` keeps the same `OperationId` and client sequence in SQLite so an idempotent server can deduplicate retries.
- `effectively-once` is rejected because this app has no server idempotency ledger, atomic server apply-plus-ack, or negotiated retention window.

The sample has a finite pending capacity of four operations and a 1 MB SQLite worker input budget.
`--demo` creates an owned temporary directory, exercises append/reopen/attempt ambiguity, and cleans
only that marked directory before returning.
