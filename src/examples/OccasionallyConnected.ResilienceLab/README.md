# OccasionallyConnected ResilienceLab

This example hosts bounded runnable resilience demonstrations for `ReactiveUI.Primitives.OccasionallyConnected`.

The `crdt-loopback` scenario uses the public in-memory server stream hub and loopback transport with two trusted authenticated client identities. It demonstrates public CRDT behavior for GCounter, PNCounter, ORSet, and LWW register states, including independent client receive checks, authoritative frontier agreement, duplicate operation idempotence, observed OR-set remove behavior, server-stamped LWW ordering, and explicit receive acknowledgement/resume behavior.

The `durable-http-lost-ack` scenario runs a local Kestrel HTTP server with a durable SQLite journal and the built-in CRDT server registration. It drops a successful push response after the server commits, closes the writer, then reopens its SQLite store and retries the same operation. An independent SQLite-backed observer receives the effect. The printed cases report the server journal count, retry identity, pending queue before and after restart, restored client state, cursor, snapshot, and observer inbox count. The clients request `AtLeastOnce` delivery, and the single observed durable server effect comes from server deduplication of the stable operation ID within its configured retention window. This scenario covers one lost-ACK boundary. The other failure points in the resilience matrix remain future work.

The `retry-backoff` scenario exercises the public retry policy with a deterministic jitter source. It verifies that a server retry hint is honored, the next attempt count is persisted, the computed delay stays within configured bounds, and the policy stops after the configured attempt budget.

The `duplicate-reordered-delivery` scenario sends the same operation batch to the in-memory hub twice. The second call returns the original result, and the stream holds one canonical event. The scenario also merges OR-set replica states in forward, reverse and duplicated order. Every order ends with the same elements.

The `capability-downgrade` scenario connects a loopback peer that lacks the features exactly-once delivery needs. A connection that requires `ExactlyOnce` fails with an `InvalidOperationException`, and no session is created. The same peer accepts an `AtLeastOnce` push, and its negotiated features never gain the missing capability.

The `backpressure` scenario builds a context whose outbox holds one operation. A second publish with `BufferStrategy.Reject` fails with `QueueCapacityExceededException`. A publish with `BufferStrategy.Block` waits until synchronization frees the slot, then completes.

The `slow-observers` scenario blocks one `Local` observer and one `SyncStates` observer inside their callbacks. Three publishes still return receipts, and the store holds each operation durably. After the observers are released, both receive the latest state.

The `corruption-quarantine` scenario writes two streams to a SQLite store, then corrupts one stored snapshot row directly in the database file. Recovery of the corrupt stream fails closed and writes a quarantine marker with the stable reason code `sqlite-payload-row-corrupt`. The failure message and reason code do not contain the stored payload text. The healthy stream still recovers its snapshot and pending operation.

The `retention-gap-recovery` scenario moves a manual server clock past the operation retention window. Resuming from the old cursor raises `ServerReceiveRetentionGapException`. The client then asks the hub for a snapshot, gets the latest server state and a new frontier cursor, and resumes receiving from that cursor.

## Project

- App project: `src/examples/OccasionallyConnected.ResilienceLab/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.csproj`
- Dedicated tests: `src/tests/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests.csproj`
- Supported app TFMs from repo props: `net8.0`, `net9.0`, `net10.0`, `net11.0`
- The app project sets `IsPackable=false`, so this runnable demo is not packed as a NuGet artifact.

## Run the demo

From the repository root:

```powershell
dotnet build src/examples/OccasionallyConnected.ResilienceLab/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.csproj -c Release -f net8.0 -m:1 --disable-build-servers
dotnet src/examples/OccasionallyConnected.ResilienceLab/bin/Release/net8.0/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.dll --scenario crdt-loopback
dotnet src/examples/OccasionallyConnected.ResilienceLab/bin/Release/net8.0/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.dll --scenario durable-http-lost-ack
dotnet src/examples/OccasionallyConnected.ResilienceLab/bin/Release/net8.0/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.dll --scenario retry-backoff
dotnet src/examples/OccasionallyConnected.ResilienceLab/bin/Release/net8.0/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.dll --scenario duplicate-reordered-delivery
dotnet src/examples/OccasionallyConnected.ResilienceLab/bin/Release/net8.0/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.dll --scenario capability-downgrade
dotnet src/examples/OccasionallyConnected.ResilienceLab/bin/Release/net8.0/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.dll --scenario backpressure
dotnet src/examples/OccasionallyConnected.ResilienceLab/bin/Release/net8.0/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.dll --scenario slow-observers
dotnet src/examples/OccasionallyConnected.ResilienceLab/bin/Release/net8.0/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.dll --scenario corruption-quarantine
dotnet src/examples/OccasionallyConnected.ResilienceLab/bin/Release/net8.0/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.dll --scenario retention-gap-recovery
```

The process prints each expected/actual case and exits with `0` only when every case passes. Unsupported scenarios print the expected scenario name and return a nonzero exit code.

## Verify the dedicated tests

Run one target framework at a time:

```powershell
dotnet build src/tests/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests.csproj -c Release -f net8.0 -m:1 --disable-build-servers
dotnet src/tests/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests/bin/Release/net8.0/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests.dll --progress off
```

For the other modern targets, replace `net8.0` with `net9.0`, `net10.0`, or `net11.0` in both commands.

## Portable coverage command

This command writes coverage into a fresh target-specific results directory and does not depend on an ignored artifact config file:

```powershell
$tfm = "net8.0"
$project = "src/tests/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests.csproj"
$testAssembly = "src/tests/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests/bin/Release/$tfm/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests.dll"
$results = "artifacts/lab-$tfm-$([Guid]::NewGuid().ToString('N'))"
dotnet build $project -c Release -f $tfm -m:1 --disable-build-servers
if ($LASTEXITCODE -ne 0) { throw "Build failed." }
dotnet $testAssembly --coverage --coverage-output-format cobertura --results-directory $results --progress off
if ($LASTEXITCODE -ne 0) { throw "Tests failed." }
$reports = @(Get-ChildItem -LiteralPath $results -Filter "*.cobertura.xml")
if ($reports.Count -ne 1) { throw "Expected exactly one fresh coverage report." }
& tools/Test-OccasionallyConnectedCoverage.ps1 -ReportPath $reports[0].FullName -PackageNames "ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab"
```

The verifier checks the Cobertura package named `ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab` for 100% line and branch coverage. The command does not add product suppressions or source exclusions.
