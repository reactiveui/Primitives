# ReactiveUI.Primitives.OccasionallyConnected.Transport.Http

HTTP client and server endpoint support for OccasionallyConnected synchronization. `HttpRemoteTransportAdapter` implements the remote transport contract, while the endpoint types expose the protocol to a server application. Requests, payloads, and replay-protection data are bounded by configurable options.

## Install

```bash
dotnet add package ReactiveUI.Primitives.OccasionallyConnected.Transport.Http
```

The package targets `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, and `net481`. It depends on the core contracts and System.Text.Json. On .NET Framework it also uses compatibility packages for time, diagnostics, and tuple support.

## Use

Create an `HttpRemoteTransportAdapter` with `HttpRemoteTransportOptions` and provide it to the client context as its `IRemoteTransportAdapter`. Configure a matching server endpoint, authorization policy, and replay protection on the service.

See the [collaboration client](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.Collaboration.Client/README.md) and [collaboration server](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.Collaboration.Server/README.md) for the protocol working end to end. [ResilienceLab](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.ResilienceLab/README.md) exercises a durable HTTP synchronization scenario.

The adapter implements the feature's HTTP protocol; it does not configure application identity or endpoint security for you. This is the first V1 release of the feature, with no earlier version to migrate from.
