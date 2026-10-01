# OccasionallyConnected Resilience Lab

This console app runs bounded demonstrations of OccasionallyConnected behavior. Each scenario prints its expected and actual checks. It exits with code 0 when every check passes and a nonzero code when a check fails or the scenario name is unknown.

These examples accompany the first v1 release of OccasionallyConnected. The feature has no earlier released version, so end users do not need a migration.

## Scenarios

- `crdt-loopback` demonstrates G-counter, PN-counter, OR-set, and last-writer-wins register behavior between trusted clients, including duplicate operation handling and receive acknowledgements. This is the default scenario.
- `durable-http-lost-ack` drops a successful HTTP push response after the server commits it. The client restarts and retries the same operation. A separate SQLite-backed observer checks that server idempotency prevents a duplicate durable effect.
- `retry-backoff` checks retry hints, persisted attempt counts, bounded delays, and the configured attempt limit with deterministic jitter.
- `duplicate-reordered-delivery` sends a batch twice and merges OR-set states in forward, reverse, and duplicate order. It checks that results stay stable.
- `capability-downgrade` checks that a peer missing exactly-once capabilities rejects an exactly-once connection while still accepting an at-least-once push.
- `backpressure` checks that a full outbox rejects a publish with `BufferStrategy.Reject` and releases a blocked publish after synchronization frees capacity.
- `slow-observers` checks that blocked local and sync-state observers do not prevent durable publish receipts and later receive the latest state.
- `corruption-quarantine` corrupts one SQLite snapshot row and checks that recovery quarantines that stream while a healthy stream still recovers.
- `retention-gap-recovery` advances a manual server clock beyond the retention window and checks recovery from a cursor gap by fetching a snapshot and resuming from its frontier.

The lost-ACK lab advances its manual clock while the observer converges.
This lets receive retries wake if a parked HTTP request times out before the host releases it.

## Run a scenario

Run from the repository root with the .NET 8 SDK or later:

```powershell
dotnet run --project src/examples/OccasionallyConnected.ResilienceLab/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.csproj --framework net8.0 -- --scenario crdt-loopback
dotnet run --project src/examples/OccasionallyConnected.ResilienceLab/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.csproj --framework net8.0 -- --scenario durable-http-lost-ack
dotnet run --project src/examples/OccasionallyConnected.ResilienceLab/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.csproj --framework net8.0 -- --scenario retry-backoff
dotnet run --project src/examples/OccasionallyConnected.ResilienceLab/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.csproj --framework net8.0 -- --scenario duplicate-reordered-delivery
dotnet run --project src/examples/OccasionallyConnected.ResilienceLab/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.csproj --framework net8.0 -- --scenario capability-downgrade
dotnet run --project src/examples/OccasionallyConnected.ResilienceLab/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.csproj --framework net8.0 -- --scenario backpressure
dotnet run --project src/examples/OccasionallyConnected.ResilienceLab/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.csproj --framework net8.0 -- --scenario slow-observers
dotnet run --project src/examples/OccasionallyConnected.ResilienceLab/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.csproj --framework net8.0 -- --scenario corruption-quarantine
dotnet run --project src/examples/OccasionallyConnected.ResilienceLab/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.csproj --framework net8.0 -- --scenario retention-gap-recovery
```

## Test the lab

Run from `src`:

```powershell
dotnet test tests/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests/ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests.csproj -c Release -f net8.0
```

The project targets `net8.0`, `net9.0`, and `net10.0`, and also `net11.0` when using the .NET 11 SDK. Replace `net8.0` in the commands with another installed target framework to use it.
