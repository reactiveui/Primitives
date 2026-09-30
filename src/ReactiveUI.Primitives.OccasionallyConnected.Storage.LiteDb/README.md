# ReactiveUI.Primitives.OccasionallyConnected.Storage.LiteDb

This package stores local occasionally connected state in a LiteDB file.
`LiteDbLocalStoreAdapter` keeps subscription identity, snapshots, pending
operations, retry state, inbox entries, leases, and dead letters in one
transactional document.

## Install

```bash
dotnet add package ReactiveUI.Primitives.OccasionallyConnected.Storage.LiteDb
```

The package targets `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`,
`net472`, `net48`, and `net481`. It depends on the core contracts and the
`LiteDB` package.

## Use

```csharp
using ReactiveUI.Primitives.OccasionallyConnected.Storage.LiteDb;

await using var store = new LiteDbLocalStoreAdapter("client-store.db");
```

Initialize the adapter before you use it. Dispose it when the owning scope
stops. The adapter writes one durable store document inside the LiteDB file and
updates that document inside a transaction for each committed change.

The adapter supports one schema version today. It does not support authenticated
encryption at rest, so initialization rejects that requirement.
