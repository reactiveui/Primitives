# ReactiveUI.Primitives.OccasionallyConnected.Storage.LiteDb

This package stores local occasionally connected state in a LiteDB file.
`LiteDbLocalStoreAdapter` keeps subscription identity, snapshots, pending
operations, retry state, inbox entries, leases, and dead letters in one
transactional document.

## Install

```bash
dotnet add package ReactiveUI.Primitives.OccasionallyConnected.Storage.LiteDb
```

Source builds target `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`,
`net472`, `net48`, and `net481`. The package depends on the core contracts and the
`LiteDB` package.

## Release assets

A package asset is a library built for one target framework.
Stable package versions omit .NET 11 preview assets and their dependency groups.
A .NET 11 app that installs a stable version uses the compatible .NET 10 asset.
Prerelease versions include .NET 11 preview assets from the source targets you build.

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

The shared local store conformance suite checks the adapter's advertised capabilities.
It covers failed commits, canceled writes, cursor fences, duplicate inbox events, leases and client binding.
Native checks cover reopen, acknowledged commits after process termination and corrupt database headers.
Reinitialization cannot change the bound client or store identity.
The first remote apply uses revision zero when no snapshot exists.
