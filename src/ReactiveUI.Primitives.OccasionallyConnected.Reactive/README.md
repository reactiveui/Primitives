# ReactiveUI.Primitives.OccasionallyConnected.Reactive

This package adapts OccasionallyConnected streams to `System.Reactive` types. It is intended for applications that already use `IObservable<T>`, `IObserver<T>`, `Unit`, or Rx schedulers.

Install this package alongside the storage and transport adapters that your application uses. The package depends on the core OccasionallyConnected runtime and `System.Reactive`.

The base `ReactiveUI.Primitives.OccasionallyConnected` package keeps its reactive API independent of `System.Reactive`. Use this package only when your application needs the Rx-facing adapter.
