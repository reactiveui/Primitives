# OccasionallyConnected Packed Sample

This console app exercises the first v1 OccasionallyConnected packages from a clean consumer project. It uses NuGet package references instead of project references and is not part of `src/ReactiveUI.Primitives.slnx`.

OccasionallyConnected has no earlier released version, so end users do not need a migration when adopting v1.

## What the sample demonstrates

The sample hosts an HTTP endpoint and server stream hub in the same process. Each client uses an `OccasionallyConnectedContext`, a SQLite local store, and the HTTP transport. The HTTP client sends requests directly to the endpoint, so the sample does not open a network port.

The run checks that:

1. Startup returns when the connection is refused or a gateway returns `503 Service Unavailable`.
2. `PublishAsync` returns a receipt and updates local state immediately.
3. `stream.Input.OnNext` updates local state without returning a receipt.
4. A client recovers local state, pending work, and a receipt after it reopens its database.
5. `TriggerSyncAsync` sends pending operations after the server starts.
6. `AwaitSynchronizedAsync` completes for a receipt created before the restart.
7. Two offline G-counter updates reconcile to the merged value on both clients.
8. An online write synchronizes and reaches the other client.

The app prints `PASS` or `FAIL` for each check. It exits with code 0 when all checks pass, code 1 when a check fails, and code 2 when the run faults or exceeds five minutes.

## Run the package gate

Run from the repository root with PowerShell 7:

```powershell
pwsh tools/Test-OccasionallyConnectedPackages.ps1
```

The script packs the feature packages and their ReactiveUI dependencies into a local feed with one temporary prerelease version. It repeats the pack and compares the resulting files, checks package frameworks, symbols, and Source Link, then copies this sample into a clean consumer directory. That copy restores only from the local feed and builds and runs for each selected target framework. The script also publishes the sample for `net10.0` as trimmed and NativeAOT apps where the required platform linker is available. It exits with code 1 if a gate fails and writes logs under `artifacts/oc-packages/logs`.

### Options

| Option | Effect |
| --- | --- |
| `-Version <value>` | Sets the package version. By default, the script uses a unique `0.1.0-octest` prerelease version for local verification. |
| `-SampleTargetFrameworks net8.0,net48` | Runs the sample only for the listed target frameworks. |
| `-SkipDeterminism` | Skips the second pack and file comparison. |
| `-SkipSample` | Skips the clean install, sample runs, and publish checks. |
| `-SkipAot` | Skips the trimmed and NativeAOT publish checks. |

NativeAOT requires a platform linker. On Windows, install the Visual C++ build tools workload. If the linker is unavailable, the script reports that gate as skipped and names the missing tool.

## Run the clean sample by hand

After the package gate completes, use the version it printed for the local feed:

```powershell
Set-Location artifacts/oc-packages/clean-sample
$version = Read-Host "Package version from the gate output"
dotnet run --framework net10.0 -p:OccasionallyConnectedPackageVersion=$version
```
