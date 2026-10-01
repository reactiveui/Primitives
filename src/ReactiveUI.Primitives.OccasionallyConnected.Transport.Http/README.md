# ReactiveUI.Primitives.OccasionallyConnected.Transport.Http

HTTP client and server endpoint support for OccasionallyConnected synchronization. `HttpRemoteTransportAdapter` implements the remote transport contract, while the endpoint types expose the protocol to a server application. Requests, payloads, and replay-protection data are bounded by configurable options.

## Install

```bash
dotnet add package ReactiveUI.Primitives.OccasionallyConnected.Transport.Http
```

Source builds target `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, and `net481`. The package depends on the core contracts and System.Text.Json. On .NET Framework it also uses compatibility packages for time, diagnostics, and tuple support.

## Release assets

A package asset is a library built for one target framework.
Stable package versions omit .NET 11 preview assets and their dependency groups.
A .NET 11 app that installs a stable version uses the compatible .NET 10 asset.
Prerelease versions include .NET 11 preview assets from the source targets you build.

## Use

Create an `HttpRemoteTransportAdapter` with `HttpRemoteTransportOptions` and provide it to the client context as its `IRemoteTransportAdapter`. Configure a matching server endpoint, authorization policy, and replay protection on the service.

See the [collaboration client](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.Collaboration.Client/README.md) and [collaboration server](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.Collaboration.Server/README.md) for the protocol working end to end. [ResilienceLab](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.ResilienceLab/README.md) exercises a durable HTTP synchronization scenario.

The adapter implements the feature's HTTP protocol; it does not configure application identity or endpoint security for you. This is the first V1 release of the feature, with no earlier version to migrate from.
