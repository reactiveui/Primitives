# Mobile composition

This package connects mobile app lifecycle to an existing occasionally connected context.
It reuses the core engine and SQLite adapter. It does not implement another engine or storage format.

## Target frameworks

The standard build targets .NET 10 and .NET 11, like the MAUI adapter in this repository.
The .NET 11 and MAUI 11 references are prerelease.
These targets expose MAUI interfaces but do not contain an OS implementation of Essentials.
Pass the platform app's `ISecureStorage`, `IFileSystem` and `IConnectivity` services to the adapters.
Do not use the neutral target's default Essentials services. They throw when the platform is unsupported.

To build native convenience entry points, set `MobilePlatformTargetFrameworks` to your supported MAUI heads.
For example, add `net10.0-android` or `net10.0-windows10.0.19041.0`.
The matching platform SDK, MAUI workload and native SQLite assets must be available.
Platform builds add `MauiMobileServices`, which uses `SecureStorage.Default`,
`FileSystem.Current` and `Connectivity.Current`. Apple heads require their supported build host.

## Lifecycle

1. Attach `MauiWindowLifecycle` to a real MAUI `Window` on the UI thread.
2. Wrap the platform connectivity service in `MauiConnectivityHint`.
3. Create `MobileSyncSession` with your context and both sources.
4. Await `RefreshAsync(CancellationToken.None)` to apply the initial state.
5. Inspect or await `Transition` to detect errors from OS events.
6. Dispose the session before disposing its context. Dispose both event sources on the UI thread.

Disable the context's auto-start option. Let the session own its lifecycle ordering.
Do not start or stop the same context through another host at the same time.

The window adapter observes `Created`, `Resumed`, `Stopped` and `Destroying`.
It describes one window. For multiple windows, provide an app-wide `IMobileLifecycle` policy.
The session coalesces requests to the latest intent. Suspension cancels startup and trigger I/O.
It then awaits the core context's durable stop with no cancellation token.
Cancelling an accepted request cancels your wait, not that drain.
An accepted suspension always drains before a later resume. A failed drain blocks resume until a retry succeeds.
Disposal unsubscribes, drains a final stop, and does not dispose the borrowed context.

Connectivity is only a hint to request another sync attempt.
No hint proves that a server is reachable. Missing internet hints do not block the engine's own retry policy.
The OS may freeze or terminate an app before a stop completes, or without sending an event.
Durable commits rely on SQLite, not on receiving a final lifecycle callback.

## Secure identity and keys

`MobileSecureState.OpenAsync` loads one secure storage entry with a stable random client ID and a random 256-bit key.
Set `allowCreate` to false when reopening an existing database.
Missing or malformed state fails instead of silently replacing its identity or encryption keys.
There is no plaintext storage fallback. Platform exceptions propagate.

`RotateAsync` writes a new current key and retains old keys.
Then call `Store.RotateEncryptionKeyAsync` to rewrite protected SQLite records.
The provider shares its current snapshot with other providers using the same service object and entry name.
It retains at most 32 keys and does not delete keys automatically.

Secure storage must atomically replace one entry. The package serializes access within one process.
You must prevent concurrent access from other processes or different wrappers around the same secure store.
Essentials has no cancellable write API. A write may commit even when your await reports cancellation.
The provider publishes successful writes before reporting cancellation.
Keys stay in memory because the SQLite worker needs synchronous key access.
The core key type owns copies and does not provide guaranteed memory erasure.

Configure Android backup exclusions and Apple keychain entitlements for your app.
Secure storage may be locked, unavailable, reset, or restored separately from SQLite.
This package does not promise hardware-backed keys or recovery after lost keys.
Do not remove or rename the secure entry while the database exists.
Keep tenant routing separate from the device ID. The ID is not authentication.

## SQLite app data

`MobileSqliteStorage.CreateAsync` accepts the platform services, a simple file name, a secure entry name,
SQLite options and a cancellation token. It creates the app-data directory and supplies the secure key provider.
Pass its `Identity` and `Store` into your usual core initialization.
It does not initialize protocol state or start a context for you.

```csharp
await using var local = await MobileSqliteStorage.CreateAsync(
    SecureStorage.Default,
    FileSystem.Current,
    "sync.db",
    "my-app.sync-state",
    new SqliteLocalStoreAdapterOptions(),
    cancellationToken);
```

Run this code in your platform app. Import `Microsoft.Maui.Storage`,
`ReactiveUI.Primitives.OccasionallyConnected.Mobile` and
`ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite`.

The bundle owns the SQLite adapter. Avoid a second owner that disposes it early.
Existing database files require existing secure state.
The adapter provides authenticated record encryption, not encryption of the entire SQLite file.
SQLite still owns its transaction, recovery, durability and capacity rules.
Device backups, filesystem durability, native SQLite availability and app sandbox policy remain host responsibilities.
