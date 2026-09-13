// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="ServerOperationProcessor"/>.</summary>
public sealed partial class ServerOperationProcessorTests
{
    /// <summary>Creates a processor.</summary>
    /// <param name="journal">The journal.</param>
    /// <param name="handler">The operation handler.</param>
    /// <returns>The processor.</returns>
    private static ServerOperationProcessor CreateProcessor(IServerCommitJournal journal, RecordingHandler handler) =>
        new(journal, new RecordingAuthorizer(), handler, options: Options());

    /// <summary>Creates processor options.</summary>
    /// <param name="maximumBatchOperations">The maximum batch operation count.</param>
    /// <param name="maximumBatchLogicalBytes">The maximum batch logical byte count.</param>
    /// <param name="maximumPreparedConflicts">The maximum prepared conflict count.</param>
    /// <param name="maximumPreparedEvents">The maximum prepared event count.</param>
    /// <param name="maximumPreparedLogicalBytes">The maximum prepared logical byte count.</param>
    /// <param name="maximumActiveRequests">The maximum active request count.</param>
    /// <returns>The processor options.</returns>
    private static ServerOperationProcessorOptions Options(
        int maximumBatchOperations = DefaultMaximumBatchOperations,
        long maximumBatchLogicalBytes = JournalMaximumLogicalBytes,
        int maximumPreparedConflicts = DefaultMaximumBatchOperations,
        int maximumPreparedEvents = DefaultMaximumBatchOperations,
        long maximumPreparedLogicalBytes = JournalMaximumLogicalBytes,
        int maximumActiveRequests = DefaultMaximumBatchOperations) =>
        new()
        {
            MaximumBatchOperations = maximumBatchOperations,
            MaximumBatchLogicalBytes = maximumBatchLogicalBytes,
            MaximumActiveRequests = maximumActiveRequests,
            MaximumCommitAttempts = MaximumCommitAttempts,
            MaximumPreparedConflicts = maximumPreparedConflicts,
            MaximumPreparedEvents = maximumPreparedEvents,
            MaximumPreparedLogicalBytes = maximumPreparedLogicalBytes,
            RetryAfter = RetryAfter,
            TimeProvider = Clock(),
        };

    /// <summary>Creates a manual time provider.</summary>
    /// <returns>The time provider.</returns>
    private static ManualTimeProvider Clock() => new(Start);

    /// <summary>Creates a journal lease.</summary>
    /// <param name="sqlite">Whether to use the SQLite journal.</param>
    /// <returns>The journal lease.</returns>
    private static JournalLease CreateJournal(bool sqlite)
    {
        if (!sqlite)
        {
            return new(new InMemoryServerCommitJournal(JournalOptions()), null);
        }

        var directory = Path.Combine(Path.GetTempPath(), $"rxui-server-processor-{Guid.NewGuid():N}");
        _ = Directory.CreateDirectory(directory);
        return new(new SqliteServerCommitJournal(Path.Combine(directory, "journal.db"), JournalOptions()), directory);
    }

    /// <summary>Creates journal options.</summary>
    /// <returns>The journal options.</returns>
    private static ServerCommitJournalOptions JournalOptions() =>
        new()
        {
            MaximumStreams = JournalMaximumStreams,
            MaximumLedgerEntries = JournalMaximumRows,
            MaximumEvents = JournalMaximumRows,
            MaximumLogicalBytes = JournalMaximumLogicalBytes,
            OperationRetention = TimeSpan.FromMinutes(RetentionMinutes),
            TimeProvider = new ManualTimeProvider(Start),
        };

    /// <summary>Creates an interfering plan that wins the first compare-and-swap attempt.</summary>
    /// <returns>The interfering plan.</returns>
    private static ServerCommitPlan CreateInterferingPlan()
    {
        var key = OperationKey(SecondOperationSeed);
        var remoteEvent = new RemoteEvent(
            Guid.Parse("cccccccc-cccc-cccc-cccc-cccccccccccc"),
            Stream,
            InterferingText,
            Start,
            key.OperationId,
            Payload(InterferingText),
            new Dictionary<string, string>())
        { Origin = new(Client, key.OperationId) };
        return new(StreamKey(), 0, State(InterferingText), Stamp(key), [Entry(key, OperationResultKind.Accepted, [remoteEvent])]);
    }

    /// <summary>Creates a synchronization batch.</summary>
    /// <param name="operations">The operations.</param>
    /// <returns>The batch.</returns>
    private static SyncBatch Batch(params SyncOperation[] operations) =>
        new(Guid.Parse("dddddddd-dddd-dddd-dddd-dddddddddddd"), operations);

    /// <summary>Creates a malformed synchronization batch.</summary>
    /// <param name="caseValue">The malformed case.</param>
    /// <returns>The malformed batch.</returns>
    private static SyncBatch MalformedBatch(int caseValue)
    {
        var first = Operation(FirstOperationSeed);
        var second = Operation(SecondOperationSeed);
        return caseValue switch
        {
            EmptyBatchIdCase => new(Guid.Empty, [first]),
            EmptyOperationsCase => Batch(),
            MixedStreamsCase => Batch(first, second with { StreamId = new("other") }),
            DuplicateOperationCase => Batch(first, second with { OperationId = first.OperationId }),
            DuplicateClientSequenceCase => Batch(first, second with { ClientSequence = first.ClientSequence }),
            NullOperationCase => new(Guid.Parse("dddddddd-dddd-dddd-dddd-dddddddddddd"), new SyncOperation[SingleCount]),
            EmptyOperationIdCase => Batch(first with { OperationId = new(Guid.Empty) }),
            InvalidOperationTypeCase => Batch(first with { Type = (SyncOperationType)int.MaxValue }),
            _ => Batch(second, first),
        };
    }

    /// <summary>Creates an invalid prepared outcome.</summary>
    /// <param name="context">The operation context.</param>
    /// <param name="caseValue">The invalid case.</param>
    /// <returns>The invalid preparation.</returns>
    private static ServerOperationPreparation InvalidPreparation(
        ServerOperationContext context,
        int caseValue) =>
        caseValue switch
        {
            RetryablePreparationCase => new(new(context.Operation.OperationId, OperationResultKind.Retryable, "retry", null), null, [], []),
            ForeignStatePreparationCase => new(
                new(context.Operation.OperationId, OperationResultKind.Accepted, null, FirstVersion),
                StateForStream("other", FirstVersion),
                [],
                []),
            ForeignConflictPreparationCase => new(
                new(context.Operation.OperationId, OperationResultKind.Conflict, ConflictMessage, FirstVersion),
                null,
                [new(OperationId(SecondOperationSeed), MergeResolution, null)],
                []),
            EmptyEventPreparationCase => new(
                new(context.Operation.OperationId, OperationResultKind.Accepted, null, FirstVersion),
                null,
                [],
                [new(Guid.Empty, Payload(FirstVersion), new Dictionary<string, string>())]),
            DuplicateEventPreparationCase => DuplicateEventPreparation(context),
            InvalidPayloadPreparationCase => new(
                new(context.Operation.OperationId, OperationResultKind.Accepted, null, FirstVersion),
                new(Stream, FirstVersion, InvalidSchemaPayload()),
                [],
                []),
            TooManyConflictsPreparationCase => new(
                new(context.Operation.OperationId, OperationResultKind.Conflict, ConflictMessage, FirstVersion),
                null,
                [
                    new(context.Operation.OperationId, "merge-1", null),
                    new(context.Operation.OperationId, "merge-2", null),
                ],
                []),
            RejectedConflictPreparationCase => new(
                new(context.Operation.OperationId, OperationResultKind.Rejected, DeniedMessage, null),
                null,
                [new(context.Operation.OperationId, MergeResolution, null)],
                []),
            _ => new(
                new(context.Operation.OperationId, OperationResultKind.Rejected, DeniedMessage, null),
                null,
                [],
                [new(Guid.Parse(PreparedEventIdText), Payload(FirstVersion), new Dictionary<string, string>())]),
        };

    /// <summary>Creates a preparation with duplicate event identifiers.</summary>
    /// <param name="context">The operation context.</param>
    /// <returns>The invalid preparation.</returns>
    private static ServerOperationPreparation DuplicateEventPreparation(ServerOperationContext context)
    {
        var eventId = Guid.Parse(PreparedEventIdText);
        return new(
            new(context.Operation.OperationId, OperationResultKind.Accepted, null, FirstVersion),
            null,
            [],
            [
                new(eventId, Payload(FirstVersion), new Dictionary<string, string>()),
                new(eventId, Payload(SecondVersion), new Dictionary<string, string>()),
            ]);
    }

    /// <summary>Creates a client identity.</summary>
    /// <returns>The client identity.</returns>
    private static ClientIdentity ClientIdentity() => new(Client, Tenant);

    /// <summary>Creates an operation.</summary>
    /// <param name="seed">The operation seed.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation Operation(int seed) =>
        new()
        {
            OperationId = OperationId(seed),
            StreamId = Stream,
            ClientSequence = seed,
            TimestampUtc = Start,
            BaseVersion = FirstVersion,
            Type = SyncOperationType.Update,
            Payload = Payload($"operation-{seed}"),
        };

    /// <summary>Creates a ledger entry.</summary>
    /// <param name="key">The operation key.</param>
    /// <param name="kind">The result kind.</param>
    /// <param name="events">The events.</param>
    /// <returns>The ledger entry.</returns>
    private static ServerLedgerEntry Entry(
        ServerOperationKey key,
        OperationResultKind kind,
        IReadOnlyList<RemoteEvent> events) =>
        new(
            key,
            Fingerprint(key.OperationId),
            new(key.OperationId, kind, null, FirstVersion),
            [],
            events);

    /// <summary>Creates a server state.</summary>
    /// <param name="version">The state version.</param>
    /// <returns>The server state.</returns>
    private static ServerState State(string version) => new(Stream, version, Payload(version));

    /// <summary>Creates a server state for a specific stream.</summary>
    /// <param name="stream">The stream value.</param>
    /// <param name="version">The state version.</param>
    /// <returns>The server state.</returns>
    private static ServerState StateForStream(string stream, string version) => new(new(stream), version, Payload(version));

    /// <summary>Creates a write stamp.</summary>
    /// <param name="key">The operation key.</param>
    /// <returns>The write stamp.</returns>
    private static ServerWriteStamp Stamp(ServerOperationKey key) => new(Start, key.ClientId, key.OperationId);

    /// <summary>Creates a stream key.</summary>
    /// <returns>The stream key.</returns>
    private static ServerStreamKey StreamKey() => new(Tenant, Stream);

    /// <summary>Creates an operation key.</summary>
    /// <param name="seed">The operation seed.</param>
    /// <returns>The operation key.</returns>
    private static ServerOperationKey OperationKey(int seed) => new(Client, OperationId(seed));

    /// <summary>Creates an operation identifier.</summary>
    /// <param name="seed">The operation seed.</param>
    /// <returns>The operation identifier.</returns>
    private static OperationId OperationId(int seed) => new(new Guid(seed, 0, 0, [0, 0, 0, 0, 0, 0, 0, 1]));

    /// <summary>Creates a canonical fingerprint for an operation identifier.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The fingerprint.</returns>
    private static ServerCommitFingerprint Fingerprint(OperationId operationId) =>
        new(CanonicalOperationFingerprint.Compute(Tenant, Client, OperationForFingerprint(operationId), FingerprintBudget));

    /// <summary>Creates an operation used for synthetic fingerprints.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation OperationForFingerprint(OperationId operationId) =>
        Operation(FirstOperationSeed) with { OperationId = operationId };

    /// <summary>Creates a payload envelope.</summary>
    /// <param name="text">The payload text.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope Payload(string text) =>
        new(Contract, 1, ContentType, Encoding.UTF8.GetBytes(text), text);

    /// <summary>Creates a payload envelope with an invalid schema version.</summary>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope InvalidSchemaPayload() =>
        new(Contract, InvalidSchemaVersion, ContentType, "invalid"u8.ToArray(), "invalid");

    /// <summary>A journal lease that cleans up SQLite test files.</summary>
    /// <param name="journal">The journal.</param>
    /// <param name="directory">The optional SQLite directory.</param>
    private sealed class JournalLease(IServerCommitJournal journal, string? directory) : IDisposable
    {
        /// <summary>Gets the journal.</summary>
        internal IServerCommitJournal Journal { get; } = journal;

        /// <inheritdoc/>
        public void Dispose()
        {
            if (Journal is IDisposable disposable)
            {
                disposable.Dispose();
            }

            if (directory is null)
            {
                return;
            }

            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Records successful authorizations.</summary>
    private sealed class RecordingAuthorizer : IServerOperationAuthorizer
    {
        /// <summary>Gets the number of authorization calls.</summary>
        internal int CallCount { get; private set; }

        /// <inheritdoc/>
        public ServerOperationScope Authorize(ClientIdentity client, SyncOperation operation)
        {
            CallCount++;
            return new(client.TenantHint ?? Tenant, client.ClientId);
        }
    }

    /// <summary>Returns an authorized scope for a different client.</summary>
    private sealed class MismatchedClientAuthorizer : IServerOperationAuthorizer
    {
        /// <inheritdoc/>
        public ServerOperationScope Authorize(ClientIdentity client, SyncOperation operation) =>
            new(client.TenantHint ?? Tenant, "other-client");
    }

    /// <summary>Rejects every authorization request.</summary>
    private sealed class ThrowingAuthorizer : IServerOperationAuthorizer
    {
        /// <inheritdoc/>
        public ServerOperationScope Authorize(ClientIdentity client, SyncOperation operation) =>
            throw new UnauthorizedAccessException(DeniedMessage);
    }

    /// <summary>Blocks operation preparation until released by the test.</summary>
    private sealed class BlockingHandler : IServerOperationHandler
    {
        /// <summary>The blocking handler event identifier text.</summary>
        private const string BlockingHandlerEventIdText = "cdcdcdcd-cdcd-cdcd-cdcd-cdcdcdcdcdcd";

        /// <summary>The preparation started signal.</summary>
        private readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The preparation release signal.</summary>
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        public async ValueTask<ServerOperationPreparation> PrepareAsync(
            ServerOperationContext context,
            CancellationToken cancellationToken)
        {
            _ = _started.TrySetResult();
            await _released.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return new(
                new(context.Operation.OperationId, OperationResultKind.Accepted, null, FirstVersion),
                State(FirstVersion),
                [],
                [new(Guid.Parse(BlockingHandlerEventIdText), Payload(FirstVersion), new Dictionary<string, string>())]);
        }

        /// <summary>Releases the pending preparation.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Release() => _ = _released.TrySetResult();

        /// <summary>Waits until preparation has started.</summary>
        /// <returns>The asynchronous wait operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task WaitUntilStartedAsync() => _started.Task;
    }

    /// <summary>Reports an oversized count and fails if a caller attempts to copy items.</summary>
    /// <typeparam name="T">The item type.</typeparam>
    /// <param name="count">The reported count.</param>
    private sealed class OversizedReadOnlyList<T>(int count) : IReadOnlyList<T>
    {
        /// <inheritdoc/>
        public int Count { get; } = count;

        /// <inheritdoc/>
        public T this[int index] => throw new InvalidOperationException("The oversized list was indexed.");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<T> GetEnumerator() => throw new InvalidOperationException("The oversized list was enumerated.");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    /// <summary>Records operation preparation calls.</summary>
    /// <param name="prepare">The preparation callback.</param>
    private sealed class RecordingHandler(
        Func<ServerOperationContext, int, ServerOperationPreparation> prepare) : IServerOperationHandler
    {
        /// <summary>Gets the number of preparation calls.</summary>
        internal int PrepareCount { get; private set; }

        /// <inheritdoc/>
        public ValueTask<ServerOperationPreparation> PrepareAsync(
            ServerOperationContext context,
            CancellationToken cancellationToken)
        {
            PrepareCount++;
            return ValueTask.FromResult(prepare(context, PrepareCount));
        }

        /// <summary>Creates an accepted handler.</summary>
        /// <returns>The handler.</returns>
        internal static RecordingHandler CreateAccepted() =>
            new(static (context, count) => Accepted(context, $"accepted-{count}", Guid.Parse("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee")));

        /// <summary>Creates a conflict handler.</summary>
        /// <returns>The handler.</returns>
        internal static RecordingHandler CreateConflict() =>
            new(static (context, count) => new(
                new(context.Operation.OperationId, OperationResultKind.Conflict, ConflictMessage, FirstVersion),
                State(FirstVersion),
                [new(context.Operation.OperationId, MergeResolution, Payload("resolved"))],
                [
                    new(
                        Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                        Payload($"event-{count}"),
                        new Dictionary<string, string> { ["kind"] = ConflictMessage }),
                ]));

        /// <summary>Creates an accepted handler without a state update.</summary>
        /// <returns>The handler.</returns>
        internal static RecordingHandler CreateAcceptedWithoutState() =>
            new(static (context, _) => new(
                new(context.Operation.OperationId, OperationResultKind.Accepted, null, FirstVersion),
                null,
                [],
                [new(Guid.Parse("eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"), Payload(FirstVersion), new Dictionary<string, string>())]));

        /// <summary>Creates a handler returning a result for a different operation.</summary>
        /// <returns>The handler.</returns>
        internal static RecordingHandler CreateMismatched() =>
            new(static (_, _) => new(
                new(OperationId(SecondOperationSeed), OperationResultKind.Accepted, null, FirstVersion),
                State(FirstVersion),
                [],
                []));

        /// <summary>Creates a versioned handler.</summary>
        /// <param name="firstEventId">The first event id.</param>
        /// <param name="secondEventId">The second event id.</param>
        /// <returns>The handler.</returns>
        internal static RecordingHandler CreateVersioned(Guid firstEventId, Guid secondEventId) =>
            new((context, count) => Accepted(context, $"prepared-{count}", count == SingleCount ? firstEventId : secondEventId));

        /// <summary>Creates an accepted preparation.</summary>
        /// <param name="context">The operation context.</param>
        /// <param name="version">The state version.</param>
        /// <param name="eventId">The event id.</param>
        /// <returns>The preparation.</returns>
        private static ServerOperationPreparation Accepted(
            ServerOperationContext context,
            string version,
            Guid eventId) =>
            new(
                new(context.Operation.OperationId, OperationResultKind.Accepted, null, version),
                State(version),
                [],
                [new(eventId, Payload(version), new Dictionary<string, string> { ["version"] = version })]);
    }

    /// <summary>Counts cursor creation attempts.</summary>
    private sealed class CountingCursorFactory : IServerOperationCursorFactory
    {
        /// <summary>Gets the number of cursor creation calls.</summary>
        internal int CallCount { get; private set; }

        /// <inheritdoc/>
        public string CreateCursor(ServerOperationContext context, int eventIndex)
        {
            CallCount++;
            return $"{context.StreamKey.TenantId}:{context.StreamKey.StreamId.Value}:{eventIndex}";
        }
    }

    /// <summary>Injects one competing commit before the wrapped journal sees its first commit.</summary>
    /// <param name="inner">The wrapped journal.</param>
    /// <param name="injected">The competing plan.</param>
    private sealed class InjectingJournal(IServerCommitJournal inner, ServerCommitPlan injected) : IServerCommitJournal
    {
        /// <summary>Whether the competing commit has been injected.</summary>
        private int _injected;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ServerCommitSnapshot Read(ServerStreamKey streamKey, IReadOnlyList<ServerOperationKey> operationKeys) =>
            inner.Read(streamKey, operationKeys);

        /// <inheritdoc/>
        public ServerCommitResult TryCommit(ServerCommitPlan plan)
        {
            if (Interlocked.Exchange(ref _injected, 1) != 0)
            {
                return inner.TryCommit(plan);
            }

            _ = inner.TryCommit(injected);
            return inner.TryCommit(plan);
        }
    }

    /// <summary>Commits without returning a replayable ledger entry.</summary>
    private sealed class CommittedWithoutEntryJournal : IServerCommitJournal
    {
        /// <inheritdoc/>
        public ServerCommitSnapshot Read(ServerStreamKey streamKey, IReadOnlyList<ServerOperationKey> operationKeys) =>
            new(streamKey, 0, null, null, [], null, 0);

        /// <inheritdoc/>
        public ServerCommitResult TryCommit(ServerCommitPlan plan) =>
            new(ServerCommitStatus.Committed, Read(plan.StreamKey, []));
    }

    /// <summary>Returns a fixed commit status for processor status mapping tests.</summary>
    /// <param name="status">The fixed status.</param>
    private sealed class StatusJournal(ServerCommitStatus status) : IServerCommitJournal
    {
        /// <inheritdoc/>
        public ServerCommitSnapshot Read(ServerStreamKey streamKey, IReadOnlyList<ServerOperationKey> operationKeys) =>
            new(streamKey, 0, null, null, [], null, 0);

        /// <inheritdoc/>
        public ServerCommitResult TryCommit(ServerCommitPlan plan)
        {
            var snapshot = status == ServerCommitStatus.Committed ? CommittedSnapshot(plan) : Read(plan.StreamKey, []);
            return new(status, snapshot);
        }

        /// <summary>Creates a committed snapshot for the supplied plan.</summary>
        /// <param name="plan">The plan.</param>
        /// <returns>The committed snapshot.</returns>
        private static ServerCommitSnapshot CommittedSnapshot(ServerCommitPlan plan)
        {
            var entry = plan.Entries[0].Commit(Start, Start.AddMinutes(RetentionMinutes));
            return new(plan.StreamKey, plan.ExpectedRevision + 1, plan.NewState, plan.NewWriteStamp, [entry], null, 0);
        }
    }

    /// <summary>Counts journal access.</summary>
    private sealed class CountingJournal : IServerCommitJournal
    {
        /// <summary>Gets the read count.</summary>
        internal int ReadCount { get; private set; }

        /// <summary>Gets the commit count.</summary>
        internal int CommitCount { get; private set; }

        /// <inheritdoc/>
        public ServerCommitSnapshot Read(ServerStreamKey streamKey, IReadOnlyList<ServerOperationKey> operationKeys)
        {
            ReadCount++;
            return new(streamKey, 0, null, null, [], null, 0);
        }

        /// <inheritdoc/>
        public ServerCommitResult TryCommit(ServerCommitPlan plan)
        {
            CommitCount++;
            return new(ServerCommitStatus.StaleRevision, Read(plan.StreamKey, []));
        }
    }

    /// <summary>Manual clock used by processor tests.</summary>
    /// <param name="utcNow">The initial timestamp.</param>
    private sealed class ManualTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <summary>The current UTC timestamp.</summary>
        private readonly DateTimeOffset _utcNow = utcNow;

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => _utcNow;
    }
}
