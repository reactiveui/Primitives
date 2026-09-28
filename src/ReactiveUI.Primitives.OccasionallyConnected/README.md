# ReactiveUI.Primitives.OccasionallyConnected

The client runtime for durable reactive streams that continue to accept local changes while disconnected. It coordinates local projections, queued operations, retry and synchronization state, and remote delivery through the storage and transport adapters you configure.

## Install

```bash
dotnet add package ReactiveUI.Primitives.OccasionallyConnected
```

The package targets `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, and `net481`. NuGet brings in the core contracts and ReactiveUI primitives it needs. The runtime does not select a durable store or HTTP endpoint for you; configure those dependencies for your application.

## Use

Build a context with `OccasionallyConnectedBuilder`, supplying a client identity, options, store, serializer, and remote transport. Then obtain an `IOccasionallyConnectedStream<TState, TInput>` from the context to publish local input and observe local state and synchronization status.

The [collaboration client example](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.Collaboration.Client/README.md) shows a complete client using SQLite storage and HTTP transport. The [ResilienceLab](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.ResilienceLab/README.md) covers retries, backpressure, recovery, and disconnected operation.

This is the first V1 release of the feature. There is no earlier OccasionallyConnected package version to migrate from.
