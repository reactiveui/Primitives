# ReactiveUI.Primitives.OccasionallyConnected.Hosting

Host lifecycle and health-check integration for an OccasionallyConnected client. It starts the registered context with the .NET host, stops it during graceful shutdown, and exposes its synchronization health through Microsoft.Extensions health checks.

## Install

```bash
dotnet add package ReactiveUI.Primitives.OccasionallyConnected.Hosting
```

The package targets `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, and `net481`. It depends on the dependency-injection integration and Microsoft.Extensions hosting and health-check abstractions.

## Use

After registering the client with `AddOccasionallyConnected(...)`, call `IServiceCollection.AddOccasionallyConnectedHosting()` to bind startup and shutdown to the host. Add `IHealthChecksBuilder.AddOccasionallyConnected()` to report the context's health.

See the [collaboration client example](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.Collaboration.Client/README.md) and [ResilienceLab](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.ResilienceLab/README.md) for client setup and runtime behavior.

This is the first V1 release of the feature. There is no earlier OccasionallyConnected package version to migrate from.
