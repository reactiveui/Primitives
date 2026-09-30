# ReactiveUI.Primitives.OccasionallyConnected.Server

Server-side stream and conflict-resolution primitives for authenticated synchronization. The package includes an in-memory or SQLite-backed stream hub, conflict resolvers, server domain handlers, and durable journal support.

## Install

```bash
dotnet add package ReactiveUI.Primitives.OccasionallyConnected.Server
```

The package targets `net8.0`, `net9.0`, `net10.0`, `net11.0`, `net462`, `net472`, `net48`, and `net481`. It depends on `ReactiveUI.Primitives.OccasionallyConnected.Core`, `Microsoft.Data.Sqlite.Core`, and the SQLite3 Multiple Ciphers native bundle (`SQLite3MC.PCLRaw.bundle`).

## Use

Create a `ServerStreamHub` with `ServerStreamHub.CreateInMemory(options)` for process-local use or `ServerStreamHub.CreateSqlite(databasePath, options)` for a durable server journal. Register stream handlers and authorization policies for the data your service exposes.

The [collaboration server example](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.Collaboration.Server/README.md) shows an HTTP service using a durable hub. [ResilienceLab](https://github.com/reactiveui/Primitives/blob/main/src/examples/OccasionallyConnected.ResilienceLab/README.md) demonstrates retries, duplicate delivery, and conflict behavior.

This package provides server primitives; host endpoints and application authentication are configured by your service. This is the first V1 release of the feature, with no earlier version to migrate from.
