# ReactiveUI.Primitives.OccasionallyConnected.Storage.BliteDb

BLite implementation of the local durable store contracts. `BliteDbLocalStoreAdapter`
stores subscription identity, snapshots, queued operations, retry state, leases,
remote inbox entries, and dead letters in one BLite database file.

## Install

```bash
dotnet add package ReactiveUI.Primitives.OccasionallyConnected.Storage.BliteDb
```

The package targets `net8.0`, `net9.0`, `net10.0`, and `net11.0`. It depends on
the core occasionally connected contracts and the `BLite` package.

## Use

```csharp
using ReactiveUI.Primitives.OccasionallyConnected.Storage.BliteDb;

var store = new BliteDbLocalStoreAdapter("client-store.blite");
```

Pass the adapter to your occasionally connected builder or register it in your own
composition root. Dispose the adapter when the owning app shuts down.

The adapter keeps the full store state in one durable BLite document. Each public
write method validates its preconditions, writes the replacement state inside one
BLite transaction, and then returns the committed result. The adapter does not
enable BLite encryption in this package, so initialization rejects
authenticated-encryption-at-rest requirements.
