# ReactiveUI.Primitives.OccasionallyConnected.Reactive

This package adapts OccasionallyConnected streams to `System.Reactive` types. It is intended for applications that already use `IObservable<T>`, `IObserver<T>`, `Unit`, or Rx schedulers.

Install this package alongside the storage and transport adapters that your application uses. The package depends on the core OccasionallyConnected runtime and `System.Reactive`.

The base `ReactiveUI.Primitives.OccasionallyConnected` package keeps its reactive API independent of `System.Reactive`. Use this package only when your application needs the Rx-facing adapter.

## Release assets

A package asset is a library built for one target framework.
Stable package versions omit .NET 11 preview assets and their dependency groups.
A .NET 11 app that installs a stable version uses the compatible .NET 10 asset.
Prerelease versions include .NET 11 preview assets from the source targets you build.
