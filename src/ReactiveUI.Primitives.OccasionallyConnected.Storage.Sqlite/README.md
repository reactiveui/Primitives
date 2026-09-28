# ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite

SQLite implementation of the local durable store contracts. `SqliteLocalStoreAdapter` persists stream state, queued operations, receipts, subscriptions, and recovery data so a client can resume after process restart.

## Install

```bash
dotnet add package ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite
```

The package targets `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, and `net481`. It depends on the core contracts, Microsoft.Data.Sqlite, and the SQLite native bundle.

## Use

```csharp
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

var store = new SqliteLocalStoreAdapter("client.db");
```

Pass the adapter to `OccasionallyConnectedBuilder` or register it for the dependency-injection integration. Dispose the adapter when its owning context is shut down. See the [Durable Outbox example](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.DurableOutbox/README.md) for persistence and recovery, and the [collaboration client example](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.Collaboration.Client/README.md) for end-to-end use.

New stores use the complete schema version 1. The package has no older released database format to upgrade. When you provide an encryption key, the adapter protects stored records and rejects an invalid key or altered protected state. AES-GCM protection requires .NET 8 or later; the .NET Framework targets can use the plaintext store.
