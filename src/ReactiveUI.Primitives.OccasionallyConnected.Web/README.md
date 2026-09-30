# Browser composition

This package uses the existing IndexedDB store and controls a context with browser events.
It supports .NET 10 and .NET 11 browser apps.

1. Create your store with `BrowserLifecycleAdapter.CreateLocalStore(jsRuntime)`.
2. Build your context with that store and your transport, serializer, and stream definitions.
3. Create `BrowserLifecycleAdapter(jsRuntime, context)`.
4. Call `StartAsync(cancellationToken)` after interactive rendering.
5. Dispose the adapter before you dispose the context.

The caller owns the context and store. The adapter stops the context on disposal but does not dispose it.
Do not give another service control of the same context's start and stop lifecycle.
Use `ReactiveUI.Primitives.OccasionallyConnected` for the context implementation.

## Network hints

`ConnectivityHint` reports `Unknown`, `Unavailable`, or `PossiblyAvailable`.
A network hint never means the server is Online. Only the engine's authenticated handshake can establish that state.
A possible network path requests synchronization. An offline hint does not disable local writes or engine retry policy.

## Page lifecycle

Hidden, frozen, and departed pages request `StopAsync`. Visible pages request `StartAsync`.
The adapter cancels active startup or synchronization when a suspension arrives.
One pending state replaces older hints. Lifecycle calls run in order without overlapping.
`StartAsync` installs listeners; it does not wait for the context to connect.
`LastError` reports lifecycle failures. The next browser event can retry. The adapter has no retry loop.

Browsers may terminate a page before asynchronous shutdown finishes.
Only committed IndexedDB records can survive that termination. Browser quota, private mode, and eviction rules still apply.
This package adds no storage or transport capability flags.

The Razor static asset is `_content/ReactiveUI.Primitives.OccasionallyConnected.Web/browserLifecycle.js`.
The IndexedDB package supplies its own static asset. Both assets must be served by your app.

## Tests

The TUnit project tests both target frameworks.
It also runs the shipped JavaScript module with Node.js 18 or later.
The JavaScript test uses DOM event targets and checks each event mapping, bounded delivery, and listener removal.
