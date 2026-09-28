# ReactiveUI.Primitives.OccasionallyConnected.Core

Core contracts and value types for applications that must keep working while a remote service is unavailable. This package defines the local store, remote transport, stream, synchronization, conflict, delivery, and recovery contracts. It also contains the CRDT value types and helpers used by the feature.

## Install

```bash
dotnet add package ReactiveUI.Primitives.OccasionallyConnected.Core
```

The package targets `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, and `net481`. It depends on `ReactiveUI.Primitives.Core`. It supplies contracts and algorithms; you must choose implementations for local storage and remote transport.

## Use

Create a CRDT state and apply local input with the public core helpers:

```csharp
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

var state = CrdtFunctions.Empty(CrdtKind.GCounter);
var input = CrdtInput.ForMutation(CrdtMutation.GCounterSet("device-a", 1));
var next = CrdtFunctions.ApplyLocal(state, input, "device-a", clientSequence: 1);
```

For a full durable workflow, see the [Durable Outbox example](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.DurableOutbox/README.md). For transport and server integration, see the [collaboration client](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.Collaboration.Client/README.md) and [ResilienceLab](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.ResilienceLab/README.md).

This is the first V1 release of the feature. There is no earlier OccasionallyConnected package version to migrate from.
