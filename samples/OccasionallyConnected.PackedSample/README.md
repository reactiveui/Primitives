# OccasionallyConnected packed sample

This sample runs the spec section 16 walkthrough against the packed NuGet packages. It does not use project
references. It is not part of `src/ReactiveUI.Primitives.slnx`.

## What the sample checks

The sample hosts a sync server in the same process. The server is a SQLite `ServerStreamHub` behind an
`HttpServerEndpoint`. Each client is an `OccasionallyConnectedContext` with a `SqliteLocalStoreAdapter` and an
`HttpRemoteTransportAdapter`. The HTTP client hands each request straight to the endpoint, so no port is opened.

The sample runs these steps in order:

1. **Offline startup.** The sample checks two kinds of "down":
   - The connection is refused. `StartAsync` must still return.
   - A gateway answers `503 Service Unavailable`. `StartAsync` returns and the context reports `Offline`.
     The later steps use this client.
2. **Optimistic write.** `PublishAsync` returns a receipt. The local state shows the new value right away.
3. **Observer input.** `stream.Input.OnNext` writes a value without a receipt.
4. **Restart recovery.** The sample disposes client A and reopens its database. The local state and the
   pending operations come back.
5. **Reconnect.** The server starts. `TriggerSyncAsync` sends the pending operations.
6. **Operation synchronization.** `AwaitSynchronizedAsync` completes for a receipt issued before the restart.
7. **Conflict reconciliation.** Clients A and B wrote to the same G-counter while offline. Both end with the
   merged value.

Each step prints `PASS` or `FAIL` with the expected and actual values. The process exits with 0 only when every
check passes. It exits with 2 if the run faults or takes longer than 5 minutes.

## How to run it

Run the gate script from any folder with PowerShell 7:

```powershell
pwsh tools/Test-OccasionallyConnectedPackages.ps1
```

The script does the following:

1. Packs the OccasionallyConnected packages and their ReactiveUI dependencies into `artifacts/oc-packages/feed`.
   Every package gets the same unique prerelease version.
2. Rebuilds the OccasionallyConnected projects and packs them again. It compares both packs file by file.
3. Checks each package for the `lib/<tfm>` folders, a `.snupkg` with matching portable PDBs, and Source Link.
4. Copies this sample to `artifacts/oc-packages/clean-sample`. The copy gets an empty `Directory.Build.props`, an
   empty `Directory.Packages.props` and a `nuget.config` that takes `ReactiveUI.Primitives*` packages only from
   the local feed. It restores into a private packages folder, then builds and runs the sample for each framework.
5. Publishes the sample for `net10.0` as a trimmed app and as a NativeAOT app, then runs both. A trim or AOT
   warning from a `ReactiveUI.*` assembly fails the gate.

The script prints a table of gates and exits with 1 when any gate fails. Logs go to `artifacts/oc-packages/logs`.

### Options

| Option | Effect |
| --- | --- |
| `-Version <v>` | Uses this package version instead of `0.1.0-octest.<timestamp>`. |
| `-SampleTargetFrameworks net8.0,net48` | Runs the sample only for these frameworks. |
| `-SkipDeterminism` | Skips the second build and the package comparison. |
| `-SkipSample` | Skips the clean install, the sample runs and the publish tests. |
| `-SkipAot` | Skips the trimmed and NativeAOT publish tests. |

NativeAOT needs the platform linker. On Windows that is the "Desktop development with C++" workload. When the
linker is missing, the script reports the AOT gate as `SKIP` and names the missing tool.

## Run the sample by hand

After the script has packed a version, you can build the clean copy yourself:

```powershell
cd artifacts/oc-packages/clean-sample
dotnet run -f net10.0 -p:OccasionallyConnectedPackageVersion=<version>
```
