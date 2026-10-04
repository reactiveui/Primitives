# ReactiveUI.Primitives.OccasionallyConnected.Core

Core contracts and value types for applications that must keep working while a remote service is unavailable. This package defines the local store, remote transport, stream, synchronization, conflict, delivery, and recovery contracts. It also contains the CRDT value types and helpers used by the feature.

## Install

```bash
dotnet add package ReactiveUI.Primitives.OccasionallyConnected.Core
```

Source builds target `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, and `net481`. The package depends on `ReactiveUI.Primitives.Core`. It supplies contracts and algorithms; you must choose implementations for local storage and remote transport.

## Release assets

A package asset is a library built for one target framework.
Stable package versions omit .NET 11 preview assets and their dependency groups.
A .NET 11 app that installs a stable version uses the compatible .NET 10 asset.
Prerelease versions include .NET 11 preview assets from the source targets you build.

## Use

Create a CRDT state and apply local input with the public core helpers:

```csharp
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

var state = CrdtFunctions.Empty(CrdtKind.GCounter);
var input = CrdtInput.ForMutation(CrdtMutation.GCounterSet("device-a", 1));
var next = CrdtFunctions.ApplyLocal(state, input, "device-a", clientSequence: 1);
```

## CRDT merge and limits

Register merges compare server write stamps first. Equal stamps, including two missing stamps, use the larger byte value in lexicographic order. This compares bytes from left to right. A longer value wins when both values share the same prefix.

`CrdtBounds.MaximumElements` limits distinct active OR-set values. Removed values and repeated values do not count. The default element limit matches the 16,384 dot-binding limit. You can lower each limit. A merge that exceeds a configured limit fails instead of silently losing data.

## OR-set checkpoints

An OR-set retains removed dots until you supply a checkpoint. A dot identifies an add by client id and durable stream sequence. A checkpoint records a **frontier**: each client's sequence through which you have applied every operation in that stream.

Call `CheckpointORSet` only after your durable ingestion path proves those complete prefixes. The largest received sequence is not enough. A missing earlier operation makes that prefix unsafe.

```csharp
var checkpoint = CrdtFunctions.CheckpointORSet(
    setState,
    new Dictionary<string, long> { ["device-a"] = fullyAppliedSequence });
var checkpointBytes = CrdtCodec.EncodeState(checkpoint);
```

Persist the whole checkpoint atomically. Keep its active bindings and `ORSetFrontier` together. Synchronize the whole state with other replicas. A missing binding below the frontier stays removed when a delayed replica or replayed add arrives. Adds above the frontier and adds from other clients still survive.

The frontier never expires. Never drop it, decrease it, or reset a client's sequence under the same client id. Its client count uses `MaximumCounterComponents`. Checkpoint before the retained-dot or tombstone limit is exhausted. Without a proven prefix, the library keeps exact tombstones and fails at the limit.

States without a frontier and all mutation inputs keep the existing version-one binary encoding. Checkpoint states and their authoritative inputs use binary version two. The decoder reads both versions. The outer payload contract and store schema stay unchanged. Upgrade every reader before sharing checkpoints. Older readers reject version two rather than lose removal knowledge.

For a full durable workflow, see the [Durable Outbox example](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.DurableOutbox/README.md). For transport and server integration, see the [collaboration client](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.Collaboration.Client/README.md) and [ResilienceLab](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.ResilienceLab/README.md).

This is the first V1 release of the feature. There is no earlier OccasionallyConnected package version to migrate from.

## Local store conformance kit

The package includes `conformance/LocalStoreConformance.cs`.
Copy that file into a .NET 8 or later TUnit test project that references this package.
The kit uses composition. You supply a fresh initialized `ILocalStoreAdapter` to each check.
It does not require a test base class or add testing dependencies to your app.

```csharp
using ReactiveUI.Primitives.OccasionallyConnected.Testing;

[Test]
public async Task LocalCommitIsAtomic()
{
    await using var store = CreateFreshStore();
    await store.InitializeAsync(
        new(LocalStoreConformance.Identity, 1, false)
        {
            ClientId = LocalStoreConformance.Client,
        },
        CancellationToken.None);
    await LocalStoreConformance.AtomicLocalCommitAsync(store);
}
```

Run the local commit, cancellation, remote apply, lease and client identity checks on every store.
Run snapshot recovery checks only when your store advertises `AtomicSnapshotRecovery`.
Use `SeedDurableAsync` and `AssertDurableAsync` to test closing and reopening the same storage.
Also kill a writer process without disposing its adapter, then check its acknowledged commits.
Test your backend's transaction failure points, corruption handling and competing writers separately.
A clean close or an in-memory fake alone does not prove physical durability.

The repository runs the same assertions on in-memory, SQLite, encrypted SQLite, FileSystem, LiteDB,
BLite and IndexedDB adapters. Native providers also run process-kill and corruption checks.
IndexedDB runs both the C# interop contract checks and the shipped JavaScript module in a real browser.
The browser checks transaction completion, conflicting writes, new pages and browser process restart.
Browser persistence still depends on the browser's storage and eviction policy.
None of these checks establish whole-file rollback protection.
