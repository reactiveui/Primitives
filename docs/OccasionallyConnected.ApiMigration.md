# Occasionally connected API migration

The unreleased API uses plain names for mergeable data. Update your source code with the names below.
The runtime keeps the existing binary payload format, contract identifiers, schema version, and enum numbers.

## Mergeable data

| Earlier name | Current name |
| --- | --- |
| `OccasionallyConnected.Crdt` namespace | `OccasionallyConnected.Mergeable` namespace |
| `Crdt` prefix on types and methods | `Mergeable` prefix |
| `CrdtMutation` and `CrdtMutationKind` | `MergeableChange` and `MergeableChangeKind` |
| `CrdtDot` | `ElementTag` |
| `CrdtDotElement` | `TaggedElement` |
| `CrdtInput.Mutation` | `MergeableInput.Value` matched as `MergeableChange` |
| `CrdtInputKind.Mutation` | `MergeableInputKind.Change` |
| `CrdtInput.ForMutation` | `MergeableInput.ForChange` |
| `CrdtDotElement.Dot` | `TaggedElement.Tag` |
| `GCounter` | `IncrementOnlyCounter` |
| `PNCounter` | `Counter` |
| `ORSet` | `AddWinsSet` |
| `LwwRegister` | `LastWriterWinsValue` |
| `ActorId` | `ClientId` |
| `GCounterComponents` | `IncrementOnlyTotalsByClient` |
| `PNCounterPositiveComponents` | `IncrementsByClient` |
| `PNCounterNegativeComponents` | `DecrementsByClient` |
| `GCounterComponent` | `IncrementOnlyTotal` |
| `PNCounterPositiveComponent` | `IncrementTotal` |
| `PNCounterNegativeComponent` | `DecrementTotal` |
| `DotBindings` | `Elements` |
| `Tombstones` | `RemovedTags` |
| `ObservedDots` | `TagsToRemove` |
| `ORSetFrontier` | `AddWinsSetFrontier` |
| `RegisterValue` | `LatestValue` |
| `RegisterStamp` | `LatestWriteStamp` |

The same family names apply inside method names. For example, `GCounterSet` becomes
`IncrementOnlyCounterSet`, `ORSetAdd` becomes `AddWinsSetAdd`, and `CheckpointORSet` becomes
`CheckpointAddWinsSet`.

The state keeps three separate counter dictionaries. `IncrementOnlyTotalsByClient` belongs to an
increment-only counter. `IncrementsByClient` and `DecrementsByClient` belong to a counter that can go
up and down. Keeping these fields separate preserves their binary slots.

```csharp
using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;

var state = MergeableFunctions.Empty(MergeableKind.IncrementOnlyCounter);
var input = MergeableInput.ForChange(MergeableChange.IncrementOnlyCounterSet("device-a", 3));
state = MergeableFunctions.ApplyLocal(state, input, "device-a", 1);
```

### Input and change cases

Create inputs with `MergeableInput.ForChange` or `MergeableInput.ForAuthoritativeState`.
Each input holds one read-only `Value`. Match its type to read a `MergeableChange` or `MergeableState`.
The factories reject null values. The `Kind` property follows the selected case.

```csharp
if (input.Value is MergeableChange change)
{
    Console.WriteLine(change.ToKind());
}
```

Create changes with the counter, set, or register factory methods. Each change holds one private case.
Its properties expose only that case's fields and cannot be set with an initializer or a `with` expression.
Counter factories require a client identifier and nonnegative components.
Use `ToKind()` in place of the change's `Kind` property when you need its wire enum.
The method derives the enum from the case. A change does not store a separate discriminator.
The same representation works on every target framework.

The decoder checks every incoming wire field before it creates a case.
It rejects unknown kinds and fields from another case. The binary field order and enum values stay the same.

## Options and operation identifiers

`new OccasionallyConnectedOptions()` creates valid nested options. You can change one option without
initializing the others. `Validate()` still rejects invalid values and missing nested options.

`OperationId.New()` creates a version 7 GUID on .NET 9 or later. Earlier targets create a version 4 GUID.
Use `OperationId.New(false)` to request version 4 on every target. The identifier remains a GUID on the wire.

## Runtime implementation details

Activity sampling belongs to the host's `ActivityListener` or OpenTelemetry configuration.
`DiagnosticsOptions.ActivitySamplingRatio` is removed. The library checks for listeners before starting an activity.
Startup logs remain redacted unless the host sets `OccasionallyConnectedServiceOptions.IncludeStartupExceptionDetails`.
That opt-in can expose application data carried in exception messages.

The concrete `RetryPolicy`, `CircuitBreaker`, and retry random source are internal. Configure retry and
circuit breaker behavior through `OccasionallyConnectedOptions` and the public builder.
The runnable retry demo uses a public context, a SQLite store, and a composed transport.
It uses equal minimum and maximum retry delays to avoid a test-only random hook.

`SyncOperation`, `MergeableInput`, `MergeableChange`, and `MergeableState` no longer claim to implement
the .NET 11 `IUnion` marker. Their validation and serialized payloads remain the same.

This revision does not rename `SyncState`, transport `Push` methods, hosting health types, or platform options.
`MergeableBounds.MaximumDotBindings`, `MaximumTombstones`, and `MaximumRegisterBytes` also keep their names.
Their naming needs a separate review of the limits each property controls.
