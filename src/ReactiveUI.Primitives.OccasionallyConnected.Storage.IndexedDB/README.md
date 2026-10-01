# ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB

This package stores occasionally connected stream state in the browser's
IndexedDB database.

It is for browser-hosted apps, such as Blazor WebAssembly. The adapter uses
`IJSRuntime` and a JavaScript module that ships with the package. It does not
touch the local file system or native libraries.

## Release assets

A package asset is a library built for one target framework.
Stable package versions omit .NET 11 preview assets and their dependency groups.
A .NET 11 app that installs a stable version uses the compatible .NET 10 asset.
Prerelease versions include .NET 11 preview assets from the source targets you build.

## What it stores

The adapter keeps one durable JSON document per initialized store identity. That
document contains:

- subscription ids
- snapshots
- pending operations
- retry state
- remote inbox entries
- lease ownership
- dead letters

Each write goes through an IndexedDB read-write transaction in the JavaScript
module. The C# adapter uses a generation check so concurrent tabs do not
silently overwrite each other.

## Lease behavior

Lease ownership is durable across tabs in the same browser profile because the
adapter stores lease state in IndexedDB with expiry timestamps.

This is not an operating-system process lock. A browser tab can still disappear
without releasing a lease. Another tab must wait for the stored lease to expire
before it can reuse the work.

## Host setup

No manual script tag is required. The adapter imports the package module from
the Razor class library static asset path:

`./_content/ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB/indexedDbInterop.js`

Create the adapter with the app's `IJSRuntime` and call `InitializeAsync`
before any other method.

```csharp
using Microsoft.JSInterop;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.IndexedDB;

var adapter = new IndexedDbLocalStoreAdapter(jsRuntime);

await adapter.InitializeAsync(
    new LocalStoreInitialization("orders", requiredSchemaVersion: 1, requireAuthenticatedEncryptionAtRest: false)
    {
        ClientId = "browser-client",
    },
    CancellationToken.None);
```

## Limits

- The package is browser-focused. It expects IndexedDB and JS interop.
- Authenticated encryption at rest is not available. Initialization rejects that
  requirement.
- Compaction uses the UTF-8 size of the stored JSON document as its byte
  estimate because IndexedDB does not expose physical page usage.
