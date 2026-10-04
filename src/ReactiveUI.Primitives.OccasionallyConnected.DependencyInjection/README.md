# ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection

Microsoft dependency-injection integration for the OccasionallyConnected client runtime. It registers a singleton context and named stream registry, and lets you configure the store, transport, serializer contracts, client identity, and runtime options through services.

## Install

```bash
dotnet add package ReactiveUI.Primitives.OccasionallyConnected.DependencyInjection
```

Source builds target `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, and `net481`. The package depends on `ReactiveUI.Primitives.OccasionallyConnected` and Microsoft.Extensions dependency-injection, logging abstractions, and options packages.

## Release assets

A package asset is a library built for one target framework.
Stable package versions omit .NET 11 preview assets and their dependency groups.
A .NET 11 app that installs a stable version uses the compatible .NET 10 asset.
Prerelease versions include .NET 11 preview assets from the source targets you build.

## Use

Register the feature with `IServiceCollection.AddOccasionallyConnected(...)`. In the configuration callback, use `OccasionallyConnectedDependencyInjectionBuilder` to select singleton store and transport services and set client and stream options. Resolve `IOccasionallyConnectedContext` or `IOccasionallyConnectedStreamRegistry` from the service provider.

See the [collaboration client example](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.Collaboration.Client/README.md) for a working application setup. The [ResilienceLab](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.ResilienceLab/README.md) shows integrated synchronization behavior.

This is the first V1 release of the feature. There is no earlier OccasionallyConnected package version to migrate from.
