# Mobile composition

This package connects mobile app lifecycle to an existing occasionally connected context.
It reuses the core engine and SQLite adapter. It does not implement another engine or storage format.

## Target frameworks

The package includes neutral .NET targets and supported .NET 10 native heads.
These targets expose MAUI interfaces but do not contain an OS implementation of Essentials.
Pass the platform app's `ISecureStorage`, `IFileSystem` and `IConnectivity` services to the adapters.
Do not use the neutral target's default Essentials services. They throw when the platform is unsupported.

Windows builds include Android and Windows heads by default. macOS builds include iOS and Mac Catalyst.
The dedicated Mobile CI workflow builds those heads on their supported hosts.
Its Windows job builds Windows and Android assets. Its macOS job builds Android, iOS and Mac Catalyst assets.
The composition job combines those unsigned assets and symbols into one package.
Every input must have the same package version and source commit.
CI checks the native API baselines and reads the actual package libraries to check `MauiMobileServices`.
Use the `mobile-complete-native-package` artifact for publication.
The release workflow replaces its Windows-built Mobile package with this complete artifact.
It checks all four heads, the release version, commit, and symbols before signing and publication.
Release workflows can supply `sourceRef` and `packageVersion` when calling this workflow.
The workflow does not publish packages by itself.
The matching platform SDK, MAUI workload and native SQLite assets must be available.
The native CI job selects a stable .NET 10 SDK before it installs workloads or builds.
Native builds add `MauiMobileServices`, which uses `SecureStorage.Default`,
`FileSystem.Current` and `Connectivity.Current`. Apple heads require their supported build host.
You do not need to set a build property when consuming a native package.
For a neutral-only source build, pass `-p:MobilePlatformTargetFrameworks=` to MSBuild.
Source builds keep the neutral .NET 10 and .NET 11 preview targets.
A package asset is a library built for one target framework.
Stable package versions omit all .NET 11 preview assets and their dependency groups.
A .NET 11 app that installs a stable version uses the compatible .NET 10 asset.
Prerelease versions include .NET 11 preview assets from the source targets you build.
Override `MobilePlatformTargetFrameworks` to build native preview heads with their matching workloads.
The native package workflow builds supported .NET 10 heads. The shared CI test matrix still includes .NET 11.
CI builds libraries and runs host-based tests. It does not run a device app or prove device permissions work.

### Neutral validation without native workloads

Disable native heads explicitly when you run desktop tests or validation tools.
Disabling the Primitives Android and Apple targets does not disable Mobile's native targets.
Use all three properties when you run a neutral test from `src`:

```powershell
dotnet test --project tests\ReactiveUI.Primitives.OccasionallyConnected.Mobile.Tests\ReactiveUI.Primitives.OccasionallyConnected.Mobile.Tests.csproj `
    -c Release --framework net10.0 `
    -p:AndroidPrimitivesTargetFrameworks= `
    -p:ApplePrimitivesTargetFrameworks= `
    -p:MobilePlatformTargetFrameworks=
```

The feature coverage, package, AOT, supply-chain, and mutation CLI gates pass these neutral properties.
They do not require MAUI workloads.
The dedicated native job does not use this opt-out when it builds the release package.
Release publication still requires its complete four-head Mobile artifact.

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

### Reinstall and backup recovery

The SQLite bundle binds its secure identity to a non-secret `.rxui-installation` file beside the database.
Keep the database and this marker together. Use one secure entry name per database.
The bundle holds a separate exclusive installation handle until disposal.
This prevents another factory call from changing keys before SQLite initialization.
A cancelled factory call may still create the marker and database after a secure write succeeds.
Retry with the same services and file name to reopen that identity.

When the database is absent, the bundle creates a new random identity and key.
It does not reuse keychain state retained after an uninstall.
The new identity prevents a reset client sequence from reusing old operation IDs or shared set element tags.
Deleting only the database also starts a new identity. The bundle reserves the new database file before it returns.
If recovery sidecars remain without the database, the bundle stops and preserves the original keys.
An existing database without a marker fails before provisioning changes.
Restore its original marker, database and secure key ring together.

When you restore a database, restore its marker and original secure identity with the complete key ring.
An existing database with missing keys fails with recovery instructions. A mismatched marker also fails.
The bundle never deletes encrypted data, resets an existing identity, or substitutes plaintext storage.
If the original keys are available, restore them through your trusted platform backup process.
Check the restored database's freshness before resuming the client.
If the keys are lost, keep the database or move it aside before re-enrolling with a new database and entry.
You cannot recover its pending encrypted operations without those keys.
Do not treat a fresh server download as recovery of unsent local work.

A matching marker and key ring do not prove that the database is the latest copy.
A valid older backup can pass these checks and restart at an older client sequence.
Your host needs a trusted checkpoint to resume that identity after a backup restore.
A checkpoint records the latest accepted state outside SQLite.
Check the restored state against that checkpoint before resuming.
If you cannot prove that the restored state is current, keep it aside and re-enroll with a new database and identity.
The bundle does not perform this freshness check for you.

Android Auto Backup does not preserve the Keystore key used by secure storage.
Exclude the SQLite database, its sidecars, and installation marker unless your app can restore the original keys securely.
Test that policy in your own app on a real device.

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
