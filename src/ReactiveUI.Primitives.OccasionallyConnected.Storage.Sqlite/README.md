# ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite

SQLite implementation of the local durable store contracts. `SqliteLocalStoreAdapter` persists stream state, queued operations, receipts, subscriptions, and recovery data so a client can resume after process restart.

## Install

```bash
dotnet add package ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite
```

The package targets `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, and `net481`.
It depends on the core contracts, `SQLitePCLRaw.core`, and `SQLite3MC.PCLRaw.bundle`.
The bundle supplies the SQLite3 Multiple Ciphers native library. Do not add another SQLite native bundle.

## Use

```csharp
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

var store = new SqliteLocalStoreAdapter("client.db");
```

Pass the adapter to `OccasionallyConnectedBuilder` or register it for the dependency-injection integration. Dispose the adapter when its owning context is shut down. See the [Durable Outbox example](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.DurableOutbox/README.md) for persistence and recovery, and the [collaboration client example](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.Collaboration.Client/README.md) for end-to-end use.

New stores use the complete schema version 1. The package has no older released database format to upgrade. When you provide an encryption key, the adapter protects stored records and rejects an invalid key or altered protected state. AES-GCM protection requires .NET 8 or later; the .NET Framework targets can use the plaintext store.

## Native database access

The adapter calls SQLite directly through internal database, statement, transaction, and BLOB owners.
BLOB owners read large payloads in small chunks for integrity checks.
The adapter closes each native handle when its operation ends. It does not pool connections.
Writer transactions take the database lock before changing data.
Lock waits have a timeout. Cancellation interrupts lock polling and long native queries.
The adapter stops observing cancellation after a commit succeeds.
It finishes postcommit maintenance before reporting success.

The store uses WAL journaling and FULL synchronous writes for durable commits.
Record encryption and key rotation keep their authenticated, atomic transaction boundaries.
The adapter's public options do not accept a database passphrase.

Native failures use `SqliteDatabaseException` from the core contracts.
Its `SqliteErrorCode` and `SqliteExtendedErrorCode` properties preserve SQLite's result codes.
Storage-full and I/O failures still use `DurableStorageException`.
