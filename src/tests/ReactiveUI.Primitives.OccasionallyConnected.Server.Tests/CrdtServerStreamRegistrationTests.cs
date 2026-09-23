// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="CrdtServerStreamRegistration"/>.</summary>
public sealed class CrdtServerStreamRegistrationTests
{
    /// <summary>The tenant.</summary>
    private const string Tenant = "tenant";

    /// <summary>The second tenant.</summary>
    private const string TenantB = "tenant-b";

    /// <summary>The first client.</summary>
    private const string ClientA = "client-a";

    /// <summary>The second client.</summary>
    private const string ClientB = "client-b";

    /// <summary>The first CRDT version.</summary>
    private const string FirstVersion = "crdt-v1";

    /// <summary>The second CRDT version.</summary>
    private const string SecondVersion = "crdt-v2";

    /// <summary>The third CRDT version.</summary>
    private const string ThirdVersion = "crdt-v3";

    /// <summary>The CRDT merge reason.</summary>
    private const string MergeReason = "crdt.merge";

    /// <summary>The expected single item count.</summary>
    private const int SingleCount = 1;

    /// <summary>The expected double item count.</summary>
    private const int DoubleCount = 2;

    /// <summary>The expected triple item count.</summary>
    private const int TripleCount = 3;

    /// <summary>The first operation sequence.</summary>
    private const int FirstSequence = 1;

    /// <summary>The second operation sequence.</summary>
    private const int SecondSequence = 2;

    /// <summary>The third operation sequence.</summary>
    private const int ThirdSequence = 3;

    /// <summary>The accepted counter component.</summary>
    private const int Component = 5;

    /// <summary>The small processor item limit.</summary>
    private const int ProcessorItemLimit = 8;

    /// <summary>The processor compare-and-swap attempt limit.</summary>
    private const int ProcessorAttemptLimit = 2;

    /// <summary>The invalid CRDT kind backing value.</summary>
    private const int InvalidKindValue = -1;

    /// <summary>The stream.</summary>
    private static readonly StreamId Stream = new("crdt/server");

    /// <summary>The server time.</summary>
    private static readonly DateTimeOffset Start = new(2026, 9, 13, 12, 0, 0, TimeSpan.Zero);

    /// <summary>Verifies invalid CRDT registration options are rejected before registration use.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateRejectsInvalidKindBeforeRegistrationUse()
    {
        static void Act() => _ = CrdtServerStreamRegistration.Create(new() { StreamId = Stream, Kind = (CrdtKind)InvalidKindValue });

        await Assert.That(Act).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies uninitialized CRDT stream identifiers are rejected by registration creation.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateRejectsUninitializedStreamId()
    {
        static void Act() => _ = CrdtServerStreamRegistration.Create(new() { StreamId = default, Kind = CrdtKind.GCounter });

        await Assert.That(Act).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies default CRDT registration accepts the first operation with compatible version defaults.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateDefaultsAcceptFirstOperation()
    {
        using var lease = new JournalLease();
        var operation = Operation(OperationId.New(), ClientA, FirstSequence, Component);
        var processor = CreateProcessor(lease.Journal, CrdtKind.GCounter);

        var result = await processor.ProcessAsync(Batch(operation), new(ClientA, Tenant), CancellationToken.None);
        var eventInput = CrdtCodec.DecodeInput(result.ProducedEvents[0].Payload.Payload);
        var snapshot = lease.Journal.Read(
            new(Tenant, Stream),
            [new(ClientA, operation.OperationId)]);

        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(result.Result.Operations[0].ReasonCode).IsNull();
        await Assert.That(result.Result.Operations[0].ServerVersion).IsEqualTo(FirstVersion);
        await Assert.That(result.ProducedEvents).Count().IsEqualTo(SingleCount);
        await Assert.That(eventInput.Kind).IsEqualTo(CrdtInputKind.AuthoritativeState);
        await Assert.That(eventInput.State?.Value.Counter).IsEqualTo(Component);
        await Assert.That(snapshot.State?.Version).IsEqualTo(FirstVersion);
    }

    /// <summary>Verifies malformed CRDT envelopes do not create domain state or events.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ResolveRejectsBadHashWithoutDomainEffects()
    {
        using var lease = new JournalLease();
        var operation = Operation(OperationId.New(), ClientA, FirstSequence, Component) with
        {
            Payload = new(
                CrdtContracts.InputContractId,
                CrdtContracts.SchemaVersion,
                CrdtServerPayloads.ContentType,
                "bad"u8.ToArray(),
                "sha256-invalid"),
        };
        var processor = CreateProcessor(lease.Journal, CrdtKind.GCounter);

        var result = await processor.ProcessAsync(Batch(operation), new(ClientA, Tenant), CancellationToken.None);
        var snapshot = lease.Journal.Read(
            new(Tenant, Stream),
            [new(ClientA, operation.OperationId)]);

        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Rejected);
        await Assert.That(result.Result.Operations[0].ReasonCode).IsEqualTo("crdt-payload-hash-mismatch");
        await Assert.That(result.ProducedEvents).IsEmpty();
        await Assert.That(snapshot.State).IsNull();
        await Assert.That(snapshot.Entries).Count().IsEqualTo(SingleCount);
        await Assert.That(snapshot.Entries[0].Events).IsEmpty();
    }

    /// <summary>Verifies different clients reusing an operation id receive distinct server event ids.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateUsesDistinctServerEventIdsForDifferentClients()
    {
        using var lease = new JournalLease();
        var operationId = OperationId.New();
        var processor = CreateProcessor(lease.Journal, CrdtKind.GCounter);
        var first = await processor.ProcessAsync(
            Batch(Operation(operationId, ClientA, FirstSequence, SingleCount)),
            new(ClientA, Tenant),
            CancellationToken.None);
        var second = await processor.ProcessAsync(
            Batch(Operation(operationId, ClientB, FirstSequence, SingleCount)),
            new(ClientB, Tenant),
            CancellationToken.None);
        var replay = await processor.ProcessAsync(
            Batch(Operation(operationId, ClientA, FirstSequence, SingleCount)),
            new(ClientA, Tenant),
            CancellationToken.None);

        await Assert.That(first.ProducedEvents).Count().IsEqualTo(SingleCount);
        await Assert.That(second.ProducedEvents).Count().IsEqualTo(SingleCount);
        await Assert.That(replay.ProducedEvents).Count().IsEqualTo(SingleCount);
        await Assert.That(first.ProducedEvents[0].EventId).IsNotEqualTo(Guid.Empty);
        await Assert.That(second.ProducedEvents[0].EventId).IsNotEqualTo(Guid.Empty);
        await Assert.That(first.ProducedEvents[0].EventId).IsNotEqualTo(second.ProducedEvents[0].EventId);
        await Assert.That(replay.ProducedEvents[0].EventId).IsEqualTo(first.ProducedEvents[0].EventId);
        await Assert.That(second.Result.Operations[0].ServerVersion).IsEqualTo(SecondVersion);
    }

    /// <summary>Verifies concurrent CRDT writes retry compare-and-swap admission from fresh state for each journal.</summary>
    /// <param name="sqlite">Whether to use the SQLite journal.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task CreateRetriesConcurrentCrdtUpdatesWithFreshState(bool sqlite)
    {
        if (sqlite)
        {
            using var lease = new SqliteLease();
            using var journal = lease.Open();
            await CreateRetriesConcurrentCrdtUpdatesWithFreshState(journal);
            return;
        }

        using var memory = new JournalLease();
        await CreateRetriesConcurrentCrdtUpdatesWithFreshState(memory.Journal);
    }

    /// <summary>Verifies all conflict policies route through the registered CRDT resolver.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateRoutesEveryConflictPolicyToCrdtResolver()
    {
        using var lease = new JournalLease();
        var processor = CreateProcessor(lease.Journal, CrdtKind.GCounter);
        var first = Operation(OperationId.New(), ClientA, FirstSequence, SingleCount) with
        {
            Policy = Policy(ConflictPolicy.LastWriterWins),
        };
        var second = Operation(OperationId.New(), ClientA, SecondSequence, DoubleCount) with
        {
            Policy = Policy(ConflictPolicy.Merge),
        };
        var third = Operation(OperationId.New(), ClientA, ThirdSequence, Component) with
        {
            Policy = Policy(ConflictPolicy.Custom),
        };

        var result = await processor.ProcessAsync(new(Guid.NewGuid(), [first, second, third]), new(ClientA, Tenant), CancellationToken.None);
        var input = CrdtCodec.DecodeInput(result.ProducedEvents[ThirdSequence - 1].Payload.Payload);

        await Assert.That(result.Result.Operations).Count().IsEqualTo(TripleCount);
        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(result.Result.Operations[1].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(result.Result.Operations[ThirdSequence - 1].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(result.Result.Operations[0].ReasonCode).IsNull();
        await Assert.That(result.Result.Operations[1].ReasonCode).IsNull();
        await Assert.That(result.Result.Operations[ThirdSequence - 1].ReasonCode).IsNull();
        await Assert.That(result.Result.Operations[ThirdSequence - 1].ServerVersion).IsEqualTo(ThirdVersion);
        await Assert.That(input.State?.Value.Counter).IsEqualTo(Component);
    }

    /// <summary>Verifies identical operation identities remain isolated across authenticated tenants.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateKeepsTenantsIsolated()
    {
        using var lease = new JournalLease();
        var operationId = OperationId.New();
        var operation = Operation(operationId, ClientA, FirstSequence, Component);
        var processor = CreateProcessor(lease.Journal, CrdtKind.GCounter);

        var first = await processor.ProcessAsync(Batch(operation), new(ClientA, Tenant), CancellationToken.None);
        var second = await processor.ProcessAsync(Batch(operation), new(ClientA, TenantB), CancellationToken.None);
        var tenantA = lease.Journal.Read(new(Tenant, Stream), [new(ClientA, operationId)]);
        var tenantB = lease.Journal.Read(new(TenantB, Stream), [new(ClientA, operationId)]);

        await Assert.That(first.ProducedEvents).Count().IsEqualTo(SingleCount);
        await Assert.That(second.ProducedEvents).Count().IsEqualTo(SingleCount);
        await Assert.That(first.ProducedEvents[0].EventId).IsNotEqualTo(second.ProducedEvents[0].EventId);
        await Assert.That(tenantA.State?.Version).IsEqualTo(FirstVersion);
        await Assert.That(tenantB.State?.Version).IsEqualTo(FirstVersion);
    }

    /// <summary>Verifies SQLite replay after restart keeps the stored event id and does not duplicate events.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CreateReplaysSqliteEventAfterRestart()
    {
        using var lease = new SqliteLease();
        var operation = Operation(OperationId.New(), ClientA, FirstSequence, Component);
        Guid eventId;
        using (var journal = lease.Open())
        {
            var initialProcessor = CreateProcessor(journal, CrdtKind.GCounter);
            var result = await initialProcessor.ProcessAsync(Batch(operation), new(ClientA, Tenant), CancellationToken.None);
            eventId = result.ProducedEvents[0].EventId;
        }

        using var reopened = lease.Open();
        var processor = CreateProcessor(reopened, CrdtKind.GCounter);
        var replay = await processor.ProcessAsync(Batch(operation), new(ClientA, Tenant), CancellationToken.None);
        var snapshot = reopened.Read(new(Tenant, Stream), [new(ClientA, operation.OperationId)]);

        await Assert.That(replay.ProducedEvents).Count().IsEqualTo(SingleCount);
        await Assert.That(replay.ProducedEvents[0].EventId).IsEqualTo(eventId);
        await Assert.That(snapshot.Entries).Count().IsEqualTo(SingleCount);
        await Assert.That(snapshot.Entries[0].Events).Count().IsEqualTo(SingleCount);
    }

    /// <summary>Verifies a resolved CRDT merge acknowledges acceptance and replays one durable decision.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The committed state or resolved audit payload is missing.</exception>
    [Test]
    public async Task CreateReplaysAcceptedResolvedMergeAfterSqliteRestart()
    {
        using var lease = new SqliteLease();
        var operation = Operation(OperationId.New(), ClientA, FirstSequence, Component);
        ServerSyncResult first;
        using (var journal = lease.Open())
        {
            first = await CreateProcessor(journal, CrdtKind.GCounter).ProcessAsync(
                Batch(operation),
                new(ClientA, Tenant),
                CancellationToken.None);
        }

        using var reopened = lease.Open();
        var replay = await CreateProcessor(reopened, CrdtKind.GCounter).ProcessAsync(
            Batch(operation),
            new(ClientA, Tenant),
            CancellationToken.None);
        var snapshot = reopened.Read(new(Tenant, Stream), [new(ClientA, operation.OperationId)]);
        if (snapshot.State is not { } committedState || snapshot.Entries[0].Conflicts[0].ResolvedPayload is not { } resolvedPayload)
        {
            throw new InvalidOperationException("The resolved CRDT merge must retain its canonical state and audit payload.");
        }

        var committedCrdt = CrdtCodec.DecodeState(committedState.State.Payload);
        var resolvedCrdt = CrdtCodec.DecodeState(resolvedPayload.Payload);

        await Assert.That(first.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(first.Result.Operations[0].ReasonCode).IsNull();
        await Assert.That(replay.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(replay.Result.Operations[0].ReasonCode).IsNull();
        await Assert.That(replay.Result.Operations[0].ServerVersion).IsEqualTo(FirstVersion);
        await Assert.That(replay.ProducedEvents).Count().IsEqualTo(SingleCount);
        await Assert.That(replay.ProducedEvents[0].EventId).IsEqualTo(first.ProducedEvents[0].EventId);
        await Assert.That(snapshot.Revision).IsEqualTo(SingleCount);
        await Assert.That(snapshot.State?.Version).IsEqualTo(FirstVersion);
        await Assert.That(committedCrdt.Value.Counter).IsEqualTo(Component);
        await Assert.That(committedCrdt.GCounterComponents[ClientA]).IsEqualTo(Component);
        await Assert.That(resolvedCrdt.GCounterComponents[ClientA]).IsEqualTo(committedCrdt.GCounterComponents[ClientA]);
        await Assert.That(resolvedCrdt.Value.Counter).IsEqualTo(committedCrdt.Value.Counter);
        await Assert.That(snapshot.Entries).Count().IsEqualTo(SingleCount);
        await Assert.That(snapshot.Entries[0].Result.Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(snapshot.Entries[0].Conflicts).Count().IsEqualTo(SingleCount);
        await Assert.That(snapshot.Entries[0].Conflicts[0].OperationId).IsEqualTo(operation.OperationId);
        await Assert.That(snapshot.Entries[0].Conflicts[0].ResolutionCode).IsEqualTo(MergeReason);
        await Assert.That(snapshot.Entries[0].Events).Count().IsEqualTo(SingleCount);
        await Assert.That(snapshot.LastCursor).IsEqualTo(first.ProducedEvents[0].ServerCursor);
    }

    /// <summary>Creates the server processor.</summary>
    /// <param name="journal">The journal.</param>
    /// <param name="kind">The CRDT kind.</param>
    /// <param name="domainHandler">The optional domain handler override.</param>
    /// <returns>The processor.</returns>
    private static ServerOperationProcessor CreateProcessor(
        IServerCommitJournal journal,
        CrdtKind kind,
        IServerDomainHandler? domainHandler = null)
    {
        var registration = CrdtServerStreamRegistration.Create(new() { StreamId = Stream, Kind = kind });
        if (domainHandler is not null)
        {
            registration = registration with { DomainHandler = domainHandler };
        }

        return new(
            journal,
            new Authorizer(),
            new ConflictResolvingServerOperationHandler(new() { Streams = [registration] }),
            options: new()
            {
                MaximumBatchOperations = ProcessorItemLimit,
                MaximumPreparedConflicts = ProcessorItemLimit,
                MaximumPreparedEvents = ProcessorItemLimit,
                MaximumCommitAttempts = ProcessorAttemptLimit,
                TimeProvider = new ManualClock(Start),
            });
    }

    /// <summary>Verifies concurrent operations commit both CRDT deltas after one stale compare-and-swap retry.</summary>
    /// <param name="journal">The journal.</param>
    /// <returns>The assertion task.</returns>
    /// <exception cref="InvalidOperationException">The committed state cannot be read or decoded.</exception>
    private static async Task CreateRetriesConcurrentCrdtUpdatesWithFreshState(IServerCommitJournal journal)
    {
        var domainHandler = new PausingDomainHandler(new CrdtServerDomainHandler(new() { Kind = CrdtKind.GCounter }));
        var firstOperation = Operation(OperationId.New(), ClientA, FirstSequence, Component);
        var secondOperation = Operation(OperationId.New(), ClientB, FirstSequence, DoubleCount);
        var processor = CreateProcessor(journal, CrdtKind.GCounter, domainHandler);

        var firstTask = processor.ProcessAsync(Batch(firstOperation), new(ClientA, Tenant), CancellationToken.None).AsTask();
        var secondTask = processor.ProcessAsync(Batch(secondOperation), new(ClientB, Tenant), CancellationToken.None).AsTask();
        await domainHandler.WhenInitialCallsPausedAsync();
        domainHandler.Release();

        var results = await Task.WhenAll(firstTask, secondTask);
        var snapshot = journal.Read(
            new(Tenant, Stream),
            [new(ClientA, firstOperation.OperationId), new(ClientB, secondOperation.OperationId)]);
        if (snapshot.State is not { } state)
        {
            throw new InvalidOperationException("The CRDT retry test expected committed server state.");
        }

        if (!CrdtServerPayloads.TryDecodeState(state.State, CrdtKind.GCounter, CrdtBounds.Default, out var crdtState, out var reason))
        {
            throw new InvalidOperationException(reason);
        }

        await Assert.That(results[0].ProducedEvents).Count().IsEqualTo(SingleCount);
        await Assert.That(results[1].ProducedEvents).Count().IsEqualTo(SingleCount);
        await Assert.That(results[0].Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(results[1].Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(results[0].Result.Operations[0].ReasonCode).IsNull();
        await Assert.That(results[1].Result.Operations[0].ReasonCode).IsNull();
        await Assert.That(results[0].Result.Operations[0].ServerVersion).IsNotEqualTo(results[1].Result.Operations[0].ServerVersion);
        await Assert.That(results[0].Result.Operations[0].ServerVersion == FirstVersion || results[1].Result.Operations[0].ServerVersion == FirstVersion).IsTrue();
        await Assert.That(results[0].Result.Operations[0].ServerVersion == SecondVersion || results[1].Result.Operations[0].ServerVersion == SecondVersion).IsTrue();
        await Assert.That(snapshot.State?.Version).IsEqualTo(SecondVersion);
        await Assert.That(snapshot.Entries).Count().IsEqualTo(DoubleCount);
        await Assert.That(snapshot.Entries[0].Events).Count().IsEqualTo(SingleCount);
        await Assert.That(snapshot.Entries[1].Events).Count().IsEqualTo(SingleCount);
        await Assert.That(crdtState.GCounterComponents[ClientA]).IsEqualTo(Component);
        await Assert.That(crdtState.GCounterComponents[ClientB]).IsEqualTo(DoubleCount);
        await Assert.That(crdtState.Value.Counter).IsEqualTo(Component + DoubleCount);
        await Assert.That(domainHandler.InitialCallsObservedSameVersion).IsTrue();
        await Assert.That(domainHandler.CallCount).IsEqualTo(TripleCount);
    }

    /// <summary>Creates a synchronization batch.</summary>
    /// <param name="operation">The operation.</param>
    /// <returns>The batch.</returns>
    private static SyncBatch Batch(SyncOperation operation) => new(Guid.NewGuid(), [operation]);

    /// <summary>Creates an operation policy.</summary>
    /// <param name="policy">The conflict policy.</param>
    /// <returns>The operation policy.</returns>
    private static OperationPolicy Policy(ConflictPolicy policy) => OperationPolicy.Default with { ConflictPolicy = policy };

    /// <summary>Creates a CRDT counter operation.</summary>
    /// <param name="operationId">The operation id.</param>
    /// <param name="clientId">The client id.</param>
    /// <param name="sequence">The client sequence.</param>
    /// <param name="component">The component value.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation Operation(
        OperationId operationId,
        string clientId,
        long sequence,
        long component) =>
        new()
        {
            OperationId = operationId,
            StreamId = Stream,
            ClientSequence = sequence,
            TimestampUtc = Start.AddDays(1),
            BaseVersion = null,
            Type = SyncOperationType.Update,
            Payload = CrdtServerPayloads.CreateInput(
                CrdtInput.ForMutation(CrdtMutation.GCounterSet(clientId, component))),
        };

    /// <summary>Authorizes requests as the authenticated client and tenant.</summary>
    private sealed class Authorizer : IServerOperationAuthorizer
    {
        /// <inheritdoc/>
        public ServerOperationScope Authorize(ClientIdentity client, SyncOperation operation) =>
            new(client.TenantHint ?? Tenant, client.ClientId);
    }

    /// <summary>Provides a fixed server clock.</summary>
    /// <param name="utcNow">The current time.</param>
    private sealed class ManualClock(DateTimeOffset utcNow) : TimeProvider
    {
        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => utcNow;
    }

    /// <summary>Pauses the first two domain applications so concurrent commits race from the same snapshot.</summary>
    /// <param name="inner">The composed CRDT domain handler.</param>
    private sealed class PausingDomainHandler(IServerDomainHandler inner) : IServerDomainHandler
    {
        /// <summary>The first paused application signal.</summary>
        private readonly TaskCompletionSource _firstPaused = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The second paused application signal.</summary>
        private readonly TaskCompletionSource _secondPaused = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The release signal.</summary>
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The first observed initial version.</summary>
        private string? _firstVersion;

        /// <summary>The second observed initial version.</summary>
        private string? _secondVersion;

        /// <summary>The number of domain calls.</summary>
        private int _callCount;

        /// <summary>Gets the number of domain calls.</summary>
        internal int CallCount => Volatile.Read(ref _callCount);

        /// <summary>Gets whether both initially paused calls observed the same version.</summary>
        internal bool InitialCallsObservedSameVersion
        {
            get
            {
                var first = Volatile.Read(ref _firstVersion);
                var second = Volatile.Read(ref _secondVersion);
                return first is not null && StringComparer.Ordinal.Equals(first, second);
            }
        }

        /// <inheritdoc/>
        public async ValueTask<ServerDomainApplyResult> ApplyAsync(
            ServerDomainApplyContext context,
            CancellationToken cancellationToken)
        {
            var call = Interlocked.Increment(ref _callCount);
            var result = await inner.ApplyAsync(context, cancellationToken).ConfigureAwait(false);
            if (call <= DoubleCount)
            {
                PauseInitialCall(call, context.Conflict.Current.Version);
                await _release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            }

            return result;
        }

        /// <summary>Releases the paused initial domain applications.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Release() => _release.SetResult();

        /// <summary>Waits until both initial domain applications are paused.</summary>
        /// <returns>The wait task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task WhenInitialCallsPausedAsync() => Task.WhenAll(_firstPaused.Task, _secondPaused.Task);

        /// <summary>Records and pauses one initial domain application.</summary>
        /// <param name="call">The call number.</param>
        /// <param name="version">The observed server version.</param>
        private void PauseInitialCall(int call, string version)
        {
            if (call == SingleCount)
            {
                Volatile.Write(ref _firstVersion, version);
                _firstPaused.SetResult();
                return;
            }

            Volatile.Write(ref _secondVersion, version);
            _secondPaused.SetResult();
        }
    }

    /// <summary>Owns an in-memory journal.</summary>
    private sealed class JournalLease : IDisposable
    {
        /// <summary>The journal stream limit.</summary>
        private const int JournalStreamLimit = 4;

        /// <summary>The journal row limit.</summary>
        private const int JournalRowLimit = 16;

        /// <summary>The journal logical byte limit.</summary>
        private const int JournalLogicalByteLimit = 65_536;

        /// <summary>Gets the journal.</summary>
        internal InMemoryServerCommitJournal Journal { get; } = new(JournalOptions());

        /// <inheritdoc/>
        public void Dispose()
        {
        }

        /// <summary>Creates journal options.</summary>
        /// <returns>The journal options.</returns>
        internal static ServerCommitJournalOptions JournalOptions() =>
            new()
            {
                MaximumStreams = JournalStreamLimit,
                MaximumLedgerEntries = JournalRowLimit,
                MaximumEvents = JournalRowLimit,
                MaximumLogicalBytes = JournalLogicalByteLimit,
                TimeProvider = new ManualClock(Start),
            };
    }

    /// <summary>Owns a temporary SQLite journal.</summary>
    private sealed class SqliteLease : IDisposable
    {
        /// <summary>The SQLite database file name.</summary>
        private const string DatabaseName = "journal.db";

        /// <summary>The temporary directory.</summary>
        private readonly string _directory = Path.Combine(Path.GetTempPath(), $"rxui-crdt-server-{Guid.NewGuid():N}");

        /// <summary>Initializes a new instance of the <see cref="SqliteLease"/> class.</summary>
        internal SqliteLease() => _ = Directory.CreateDirectory(_directory);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => Directory.Delete(_directory, recursive: true);

        /// <summary>Opens the SQLite journal.</summary>
        /// <returns>The SQLite journal.</returns>
        internal SqliteServerCommitJournal Open() =>
            new(Path.Combine(_directory, DatabaseName), JournalLease.JournalOptions());
    }
}
