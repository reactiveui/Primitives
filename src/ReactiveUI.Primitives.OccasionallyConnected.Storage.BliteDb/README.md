# ReactiveUI.Primitives.OccasionallyConnected.Storage.BliteDb

BLite implementation of the local durable store contracts. `BliteDbLocalStoreAdapter`
stores subscription identity, snapshots, queued operations, retry state, leases,
remote inbox entries, and dead letters in one BLite database file.

## Install

```bash
dotnet add package ReactiveUI.Primitives.OccasionallyConnected.Storage.BliteDb
```

Source builds target `net8.0`, `net9.0`, `net10.0`, and `net11.0`. The package depends on
the core occasionally connected contracts and the `BLite` package.

## Release assets

A package asset is a library built for one target framework.
Stable package versions omit .NET 11 preview assets and their dependency groups.
A .NET 11 app that installs a stable version uses the compatible .NET 10 asset.
Prerelease versions include .NET 11 preview assets from the source targets you build.

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

The shared local store conformance suite checks the adapter's advertised capabilities.
It covers failed commits, canceled writes, cursor fences, duplicate inbox events, leases and client binding.
Native checks cover reopen, acknowledged commits after process termination and corrupt database headers.
Reinitialization cannot change the bound client or store identity.
The first remote apply uses revision zero when no snapshot exists.
