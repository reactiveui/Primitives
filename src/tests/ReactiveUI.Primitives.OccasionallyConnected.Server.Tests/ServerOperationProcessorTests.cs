// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="ServerOperationProcessor"/>.</summary>
public sealed partial class ServerOperationProcessorTests
{
    /// <summary>The authenticated tenant.</summary>
    private const string Tenant = "tenant";

    /// <summary>The authenticated client.</summary>
    private const string Client = "client";

    /// <summary>The default stream value.</summary>
    private const string StreamValue = "stream";

    /// <summary>The payload contract id.</summary>
    private const string Contract = "contract";

    /// <summary>The payload content type.</summary>
    private const string ContentType = "text/plain";

    /// <summary>The conflict error text.</summary>
    private const string ConflictMessage = "conflict";

    /// <summary>The merge resolution code.</summary>
    private const string MergeResolution = "merge";

    /// <summary>The denied error text.</summary>
    private const string DeniedMessage = "denied";

    /// <summary>The prepared event identifier text.</summary>
    private const string PreparedEventIdText = "abababab-abab-abab-abab-abababababab";

    /// <summary>The second prepared event identifier text.</summary>
    private const string SecondPreparedEventIdText = "babababa-baba-baba-baba-babababababa";

    /// <summary>The first state version.</summary>
    private const string FirstVersion = "v1";

    /// <summary>The second state version.</summary>
    private const string SecondVersion = "v2";

    /// <summary>The first operation seed.</summary>
    private const int FirstOperationSeed = 1;

    /// <summary>The second operation seed.</summary>
    private const int SecondOperationSeed = 2;

    /// <summary>The expected single item count.</summary>
    private const int SingleCount = 1;

    /// <summary>The expected double item count.</summary>
    private const int DoubleCount = 2;

    /// <summary>The maximum processor attempts used by bounded stale tests.</summary>
    private const int MaximumCommitAttempts = 2;

    /// <summary>The malformed batch case for an empty batch identifier.</summary>
    private const int EmptyBatchIdCase = 0;

    /// <summary>The malformed batch case for empty operations.</summary>
    private const int EmptyOperationsCase = 1;

    /// <summary>The malformed batch case for mixed streams.</summary>
    private const int MixedStreamsCase = 2;

    /// <summary>The malformed batch case for duplicate operation identifiers.</summary>
    private const int DuplicateOperationCase = 3;

    /// <summary>The malformed batch case for duplicate client sequences.</summary>
    private const int DuplicateClientSequenceCase = 4;

    /// <summary>The malformed batch case for descending client sequences.</summary>
    private const int DescendingClientSequenceCase = 5;

    /// <summary>The malformed batch case for a null operation.</summary>
    private const int NullOperationCase = 6;

    /// <summary>The malformed batch case for an empty operation identifier.</summary>
    private const int EmptyOperationIdCase = 7;

    /// <summary>The malformed batch case for an invalid operation type.</summary>
    private const int InvalidOperationTypeCase = 8;

    /// <summary>The prepared failure case for a retryable result.</summary>
    private const int RetryablePreparationCase = 0;

    /// <summary>The prepared failure case for state on another stream.</summary>
    private const int ForeignStatePreparationCase = 1;

    /// <summary>The prepared failure case for a conflict on another operation.</summary>
    private const int ForeignConflictPreparationCase = 2;

    /// <summary>The prepared failure case for an empty prepared event identifier.</summary>
    private const int EmptyEventPreparationCase = 3;

    /// <summary>The prepared failure case for duplicate prepared event identifiers.</summary>
    private const int DuplicateEventPreparationCase = 4;

    /// <summary>The prepared failure case for an invalid payload schema version.</summary>
    private const int InvalidPayloadPreparationCase = 5;

    /// <summary>The prepared failure case for too many conflicts.</summary>
    private const int TooManyConflictsPreparationCase = 6;

    /// <summary>The prepared failure case for rejected conflict effects.</summary>
    private const int RejectedConflictPreparationCase = 7;

    /// <summary>The prepared failure case for rejected event effects.</summary>
    private const int RejectedEventPreparationCase = 8;

    /// <summary>The maximum processor batch operation count.</summary>
    private const int DefaultMaximumBatchOperations = 16;

    /// <summary>The journal stream limit used by tests.</summary>
    private const int JournalMaximumStreams = 8;

    /// <summary>The journal retained row limit used by tests.</summary>
    private const int JournalMaximumRows = 32;

    /// <summary>The journal logical byte limit used by tests.</summary>
    private const int JournalMaximumLogicalBytes = 16_384;

    /// <summary>The journal retention duration in minutes.</summary>
    private const int RetentionMinutes = 5;

    /// <summary>The canonical fingerprint byte budget.</summary>
    private const int FingerprintBudget = 4096;

    /// <summary>The intentionally tiny logical byte budget used by rejection tests.</summary>
    private const int TinyLogicalByteBudget = 1;

    /// <summary>The intentionally invalid payload schema version used by rejection tests.</summary>
    private const int InvalidSchemaVersion = 0;

    /// <summary>The constructor-level maximum prepared item count.</summary>
    private const int ConstructorMaximumPreparedItems = 512;

    /// <summary>The interfering operation state and payload text.</summary>
    private const string InterferingText = "interfering";

    /// <summary>The processor retry delay used by retryable status tests.</summary>
    private static readonly TimeSpan RetryAfter = TimeSpan.FromSeconds(2);

    /// <summary>The fixed server clock instant.</summary>
    private static readonly DateTimeOffset Start = new(2026, 9, 13, 9, 0, 0, TimeSpan.Zero);

    /// <summary>The default stream id.</summary>
    private static readonly StreamId Stream = new(StreamValue);

    /// <summary>Verifies cancellation observed after domain preparation prevents durable side effects.</summary>
    /// <param name="sqlite">Whether to use the durable journal.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ProcessAsyncCancellationAfterPreparationDoesNotCommit(bool sqlite)
    {
        using var journal = CreateJournal(sqlite);
        using var cancellation = new CancellationTokenSource();
        var handler = new RecordingHandler((context, attempt) =>
        {
            _ = cancellation.CancelAsync();
            return new(new(context.Operation.OperationId, OperationResultKind.Accepted, null, FirstVersion), State(FirstVersion), [], []);
        });
        var processor = CreateProcessor(journal.Journal, handler);
        async Task Act() => _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), cancellation.Token);
        await Assert.That(Act)
            .ThrowsExactly<OperationCanceledException>();
        var snapshot = journal.Journal.Read(StreamKey(), [OperationKey(FirstOperationSeed)]);
        await Assert.That(snapshot.Revision).IsEqualTo(0);
        await Assert.That(snapshot.Entries.Count).IsEqualTo(0);
        await Assert.That(snapshot.State).IsNull();
    }

    /// <summary>Verifies committed operation receipts replay without re-running the domain handler.</summary>
    /// <param name="sqlite">Whether to use the SQLite journal.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ProcessAsyncReplaysOriginalTerminalReceipt(bool sqlite)
    {
        using var lease = CreateJournal(sqlite);
        var handler = RecordingHandler.CreateConflict();
        var processor = CreateProcessor(lease.Journal, handler);
        var operation = Operation(FirstOperationSeed);

        var first = await processor.ProcessAsync(Batch(operation), ClientIdentity(), CancellationToken.None);
        var second = await processor.ProcessAsync(Batch(operation), ClientIdentity(), CancellationToken.None);
        var replay = lease.Journal.Read(StreamKey(), [OperationKey(FirstOperationSeed)]);

        await Assert.That(handler.PrepareCount).IsEqualTo(SingleCount);
        await Assert.That(first.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Conflict);
        await Assert.That(second.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Conflict);
        await Assert.That(first.ProducedEvents[0].EventId).IsEqualTo(second.ProducedEvents[0].EventId);
        await Assert.That(first.ProducedEvents[0].Origin?.ClientId).IsEqualTo(Client);
        await Assert.That(first.Result.ServerCursor).IsEqualTo(first.ProducedEvents[0].ServerCursor);
        await Assert.That(replay.Entries[0].Conflicts[0].ResolutionCode).IsEqualTo(MergeResolution);
        await Assert.That(replay.Entries[0].Events[0].Origin?.OperationId).IsEqualTo(operation.OperationId);
    }

    /// <summary>Verifies stale admission discards the whole prepared result before retrying.</summary>
    /// <param name="sqlite">Whether to use the SQLite journal.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ProcessAsyncDiscardsStalePreparationBeforeRetry(bool sqlite)
    {
        using var lease = CreateJournal(sqlite);
        var operation = Operation(FirstOperationSeed);
        var staleEventId = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
        var committedEventId = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
        var handler = RecordingHandler.CreateVersioned(staleEventId, committedEventId);
        var injecting = new InjectingJournal(lease.Journal, CreateInterferingPlan());
        var processor = CreateProcessor(injecting, handler);

        var result = await processor.ProcessAsync(Batch(operation), ClientIdentity(), CancellationToken.None);
        var replay = lease.Journal.Read(StreamKey(), [OperationKey(FirstOperationSeed), OperationKey(SecondOperationSeed)]);

        await Assert.That(handler.PrepareCount).IsEqualTo(DoubleCount);
        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(result.ProducedEvents[0].EventId).IsEqualTo(committedEventId);
        await Assert.That(result.ProducedEvents[0].ServerCursor).Contains(":2:");
        await Assert.That(replay.State?.Version).IsEqualTo("prepared-2");
        await Assert.That(replay.Entries).Count().IsEqualTo(DoubleCount);
        await Assert.That(replay.Entries[0].Events[0].EventId).IsEqualTo(committedEventId);
        await Assert.That(replay.Entries[1].Events[0].EventId).IsNotEqualTo(staleEventId);
    }

    /// <summary>Verifies authorization happens before journal lookup and domain preparation.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ProcessAsyncAuthorizesBeforeJournalLookup()
    {
        var journal = new CountingJournal();
        var handler = RecordingHandler.CreateAccepted();
        var processor = new ServerOperationProcessor(
            journal,
            new ThrowingAuthorizer(),
            handler,
            options: Options());

        async Task Act() => _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), CancellationToken.None);

        await Assert.That(Act)
            .ThrowsExactly<UnauthorizedAccessException>();
        await Assert.That(journal.ReadCount).IsEqualTo(0);
        await Assert.That(journal.CommitCount).IsEqualTo(0);
        await Assert.That(handler.PrepareCount).IsEqualTo(0);
    }

    /// <summary>Verifies batch bounds are checked before authorization or journal capture.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ProcessAsyncChecksBatchBoundsBeforeAuthorization()
    {
        var journal = new CountingJournal();
        var authorizer = new RecordingAuthorizer();
        var processor = new ServerOperationProcessor(
            journal,
            authorizer,
            RecordingHandler.CreateAccepted(),
            options: Options(SingleCount));

        async Task Act() => _ = await processor.ProcessAsync(
                Batch(Operation(FirstOperationSeed), Operation(SecondOperationSeed)),
                ClientIdentity(),
                CancellationToken.None);

        await Assert.That(Act)
            .ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(authorizer.CallCount).IsEqualTo(0);
        await Assert.That(journal.ReadCount).IsEqualTo(0);
    }

    /// <summary>Verifies active request admission is finite and fails before retaining a second batch.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ProcessAsyncRejectsActiveRequestOverCapacityBeforeAuthorization()
    {
        using var lease = CreateJournal(sqlite: false);
        var authorizer = new RecordingAuthorizer();
        var handler = new BlockingHandler();
        var processor = new ServerOperationProcessor(
            lease.Journal,
            authorizer,
            handler,
            options: Options(maximumActiveRequests: SingleCount));
        var first = processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), CancellationToken.None).AsTask();

        await handler.WaitUntilStartedAsync();
        async Task Act() => _ = await processor.ProcessAsync(Batch(Operation(SecondOperationSeed)), ClientIdentity(), CancellationToken.None);

        var exception = await Assert.That(Act).ThrowsExactly<QueueCapacityExceededException>();
        await Assert.That(exception?.CanFitWhenEmpty).IsTrue();
        await Assert.That(authorizer.CallCount).IsEqualTo(SingleCount);

        handler.Release();
        var result = await first;
        var snapshot = lease.Journal.Read(StreamKey(), [OperationKey(FirstOperationSeed)]);
        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(snapshot.Revision).IsEqualTo(SingleCount);
    }

    /// <summary>Verifies malformed whole batches are rejected before authorization or journal capture.</summary>
    /// <param name="caseValue">The malformed batch case.</param>
    /// <param name="expectedError">The expected batch validation error.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [Arguments(EmptyBatchIdCase, SyncBatchValidationError.MalformedBatch)]
    [Arguments(EmptyOperationsCase, SyncBatchValidationError.MalformedBatch)]
    [Arguments(MixedStreamsCase, SyncBatchValidationError.MixedStreams)]
    [Arguments(DuplicateOperationCase, SyncBatchValidationError.DuplicateOperation)]
    [Arguments(DuplicateClientSequenceCase, SyncBatchValidationError.DuplicateClientSequence)]
    [Arguments(DescendingClientSequenceCase, SyncBatchValidationError.MalformedBatch)]
    [Arguments(NullOperationCase, SyncBatchValidationError.MalformedBatch)]
    [Arguments(EmptyOperationIdCase, SyncBatchValidationError.MalformedBatch)]
    [Arguments(InvalidOperationTypeCase, SyncBatchValidationError.MalformedBatch)]
    public async Task ProcessAsyncValidatesWholeBatchBeforeAuthorization(
        int caseValue,
        SyncBatchValidationError expectedError)
    {
        var journal = new CountingJournal();
        var authorizer = new RecordingAuthorizer();
        var processor = new ServerOperationProcessor(
            journal,
            authorizer,
            RecordingHandler.CreateAccepted(),
            options: Options());
        var batch = MalformedBatch(caseValue);

        async Task Act() => _ = await processor.ProcessAsync(batch, ClientIdentity(), CancellationToken.None);

        var exception = await Assert.That(Act).ThrowsExactly<SyncBatchValidationException>();
        await Assert.That(exception?.Error).IsEqualTo(expectedError);
        await Assert.That(authorizer.CallCount).IsEqualTo(0);
        await Assert.That(journal.ReadCount).IsEqualTo(0);
        await Assert.That(journal.CommitCount).IsEqualTo(0);
    }

    /// <summary>Verifies logical batch bytes are bounded before authorization.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ProcessAsyncChecksBatchLogicalBytesBeforeAuthorization()
    {
        var journal = new CountingJournal();
        var authorizer = new RecordingAuthorizer();
        var processor = new ServerOperationProcessor(
            journal,
            authorizer,
            RecordingHandler.CreateAccepted(),
            options: Options(maximumBatchLogicalBytes: TinyLogicalByteBudget));

        async Task Act() => _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(authorizer.CallCount).IsEqualTo(0);
        await Assert.That(journal.ReadCount).IsEqualTo(0);
        await Assert.That(journal.CommitCount).IsEqualTo(0);
    }

    /// <summary>Verifies the authorized client scope must match the authenticated client.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ProcessAsyncRejectsAuthorizedClientMismatchBeforeJournalLookup()
    {
        var journal = new CountingJournal();
        var handler = RecordingHandler.CreateAccepted();
        var processor = new ServerOperationProcessor(
            journal,
            new MismatchedClientAuthorizer(),
            handler,
            options: Options());

        async Task Act() => _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<UnauthorizedAccessException>();
        await Assert.That(journal.ReadCount).IsEqualTo(0);
        await Assert.That(journal.CommitCount).IsEqualTo(0);
        await Assert.That(handler.PrepareCount).IsEqualTo(0);
    }

    /// <summary>Verifies operations without base versions are valid and still commit.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ProcessAsyncAcceptsOperationWithoutBaseVersion()
    {
        using var lease = CreateJournal(sqlite: false);
        var processor = CreateProcessor(lease.Journal, RecordingHandler.CreateAccepted());

        var result = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed) with { BaseVersion = null }), ClientIdentity(), CancellationToken.None);

        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(result.ProducedEvents).Count().IsEqualTo(SingleCount);
    }

    /// <summary>Verifies a duplicate operation identifier with different intent is rejected before handling.</summary>
    /// <param name="sqlite">Whether to use the SQLite journal.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ProcessAsyncRejectsDuplicateIntentMismatch(bool sqlite)
    {
        using var lease = CreateJournal(sqlite);
        var handler = RecordingHandler.CreateAccepted();
        var processor = CreateProcessor(lease.Journal, handler);
        var operation = Operation(FirstOperationSeed);
        var changed = operation with { Payload = Payload("changed") };

        _ = await processor.ProcessAsync(Batch(operation), ClientIdentity(), CancellationToken.None);
        var result = await processor.ProcessAsync(Batch(changed), ClientIdentity(), CancellationToken.None);

        await Assert.That(handler.PrepareCount).IsEqualTo(SingleCount);
        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Rejected);
        await Assert.That(result.Result.Operations[0].ReasonCode).IsEqualTo("intent-mismatch");
        await Assert.That(result.ProducedEvents).IsEmpty();
    }

    /// <summary>Verifies retryable and rejected commit statuses map to stable operation receipts.</summary>
    /// <param name="statusValue">The journal status value.</param>
    /// <param name="expectedKind">The expected operation result kind.</param>
    /// <param name="reasonCode">The expected reason code.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [Arguments((int)ServerCommitStatus.IntentMismatch, OperationResultKind.Rejected, "intent-mismatch")]
    [Arguments((int)ServerCommitStatus.CapacityExceeded, OperationResultKind.Retryable, "capacity-exceeded")]
    [Arguments((int)ServerCommitStatus.RevisionOverflow, OperationResultKind.Rejected, "revision-overflow")]
    [Arguments((int)ServerCommitStatus.EventSequenceOverflow, OperationResultKind.Rejected, "event-sequence-overflow")]
    public async Task ProcessAsyncMapsCommitStatus(
        int statusValue,
        OperationResultKind expectedKind,
        string reasonCode)
    {
        var processor = CreateProcessor(new StatusJournal((ServerCommitStatus)statusValue), RecordingHandler.CreateAccepted());

        var result = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), CancellationToken.None);

        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(expectedKind);
        await Assert.That(result.Result.Operations[0].ReasonCode).IsEqualTo(reasonCode);
        await Assert.That(result.Result.RetryAfter).IsEqualTo(expectedKind == OperationResultKind.Retryable ? RetryAfter : null);
    }

    /// <summary>Verifies bounded stale retries eventually return a retryable receipt.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ProcessAsyncStopsAfterBoundedStaleRetries()
    {
        var handler = RecordingHandler.CreateAccepted();
        var processor = CreateProcessor(new StatusJournal(ServerCommitStatus.StaleRevision), handler);

        var result = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), CancellationToken.None);

        await Assert.That(handler.PrepareCount).IsEqualTo(MaximumCommitAttempts);
        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Retryable);
        await Assert.That(result.Result.Operations[0].ReasonCode).IsEqualTo("stale-revision");
        await Assert.That(result.Result.RetryAfter).IsEqualTo(RetryAfter);
    }

    /// <summary>Verifies invalid journal status values are rejected.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ProcessAsyncRejectsUnknownCommitStatus()
    {
        var processor = CreateProcessor(new StatusJournal((ServerCommitStatus)int.MaxValue), RecordingHandler.CreateAccepted());

        async Task Act() => _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), CancellationToken.None);

        await Assert.That(Act)
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies default options are valid when omitted.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConstructorAcceptsDefaultOptions()
    {
        var processor = new ServerOperationProcessor(
            new CountingJournal(),
            new RecordingAuthorizer(),
            RecordingHandler.CreateAccepted(),
            new ServerOperationCursorFactory());

        await Assert.That(processor).IsNotNull();
    }

    /// <summary>Verifies a committed status without a replayable ledger entry is rejected.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ProcessAsyncRejectsCommittedSnapshotWithoutEntry()
    {
        var processor = CreateProcessor(new CommittedWithoutEntryJournal(), RecordingHandler.CreateAccepted());

        async Task Act() => _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), CancellationToken.None);

        await Assert.That(Act)
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies prepared results must belong to the processed operation.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ProcessAsyncRejectsMismatchedPreparedResult()
    {
        var journal = new CountingJournal();
        var handler = RecordingHandler.CreateMismatched();
        var processor = CreateProcessor(journal, handler);

        async Task Act() => _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), CancellationToken.None);

        await Assert.That(Act)
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(handler.PrepareCount).IsEqualTo(SingleCount);
        await Assert.That(journal.CommitCount).IsEqualTo(0);
    }

    /// <summary>Verifies rejected preparations cannot persist state or events.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ProcessAsyncRejectsPreparedRejectionEffectsBeforeCommit()
    {
        var journal = new CountingJournal();
        var handler = new RecordingHandler(static (context, _) => new(
            new(context.Operation.OperationId, OperationResultKind.Rejected, DeniedMessage, null),
            State(FirstVersion),
            [],
            [new(Guid.Parse(PreparedEventIdText), Payload(FirstVersion), new Dictionary<string, string>())]));
        var processor = CreateProcessor(journal, handler);

        async Task Act() => _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
        await Assert.That(handler.PrepareCount).IsEqualTo(SingleCount);
        await Assert.That(journal.CommitCount).IsEqualTo(0);
    }

    /// <summary>Verifies a rejected preparation without effects can be retained for duplicate replay.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ProcessAsyncCommitsRejectedPreparationWithoutEffects()
    {
        using var lease = CreateJournal(sqlite: false);
        var handler = new RecordingHandler(static (context, _) => new(
            new(context.Operation.OperationId, OperationResultKind.Rejected, DeniedMessage, null),
            null,
            [],
            []));
        var processor = CreateProcessor(lease.Journal, handler);

        var result = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), CancellationToken.None);
        var replay = lease.Journal.Read(StreamKey(), [OperationKey(FirstOperationSeed)]);

        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Rejected);
        await Assert.That(result.ProducedEvents).IsEmpty();
        await Assert.That(replay.Entries[0].Result.Kind).IsEqualTo(OperationResultKind.Rejected);
    }

    /// <summary>Verifies conflicts without resolved payloads are bounded and retained.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ProcessAsyncCommitsConflictWithoutResolvedPayload()
    {
        using var lease = CreateJournal(sqlite: false);
        var handler = new RecordingHandler(static (context, _) => new(
            new(context.Operation.OperationId, OperationResultKind.Conflict, ConflictMessage, FirstVersion),
            State(FirstVersion),
            [new(context.Operation.OperationId, MergeResolution, null)],
            []));
        var processor = CreateProcessor(lease.Journal, handler);

        var result = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), CancellationToken.None);
        var replay = lease.Journal.Read(StreamKey(), [OperationKey(FirstOperationSeed)]);

        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Conflict);
        await Assert.That(replay.Entries[0].Conflicts[0].ResolvedPayload).IsNull();
    }

    /// <summary>Verifies prepared event counts are bounded before cursor stamping and commit.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ProcessAsyncChecksPreparedEventCountBeforeStamping()
    {
        var journal = new CountingJournal();
        var cursorFactory = new CountingCursorFactory();
        var handler = new RecordingHandler(static (context, _) => new(
            new(context.Operation.OperationId, OperationResultKind.Accepted, null, FirstVersion),
            State(FirstVersion),
            [],
            [
                new(Guid.Parse(PreparedEventIdText), Payload(FirstVersion), new Dictionary<string, string>()),
                new(Guid.Parse(SecondPreparedEventIdText), Payload(SecondVersion), new Dictionary<string, string>()),
            ]));
        var processor = new ServerOperationProcessor(
            journal,
            new RecordingAuthorizer(),
            handler,
            cursorFactory,
            Options(maximumPreparedEvents: SingleCount));

        async Task Act() => _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(cursorFactory.CallCount).IsEqualTo(0);
        await Assert.That(journal.CommitCount).IsEqualTo(0);
    }

    /// <summary>Verifies prepared logical bytes are bounded before cursor stamping and commit.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ProcessAsyncChecksPreparedLogicalBytesBeforeStamping()
    {
        var journal = new CountingJournal();
        var cursorFactory = new CountingCursorFactory();
        var handler = RecordingHandler.CreateAccepted();
        var processor = new ServerOperationProcessor(
            journal,
            new RecordingAuthorizer(),
            handler,
            cursorFactory,
            Options(maximumPreparedLogicalBytes: TinyLogicalByteBudget));

        async Task Act() => _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(cursorFactory.CallCount).IsEqualTo(0);
        await Assert.That(journal.CommitCount).IsEqualTo(0);
    }

    /// <summary>Verifies invalid prepared outcomes are rejected before commit.</summary>
    /// <param name="caseValue">The invalid prepared outcome case.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [Arguments(RetryablePreparationCase)]
    [Arguments(ForeignStatePreparationCase)]
    [Arguments(ForeignConflictPreparationCase)]
    [Arguments(EmptyEventPreparationCase)]
    [Arguments(DuplicateEventPreparationCase)]
    [Arguments(InvalidPayloadPreparationCase)]
    [Arguments(TooManyConflictsPreparationCase)]
    [Arguments(RejectedConflictPreparationCase)]
    [Arguments(RejectedEventPreparationCase)]
    public async Task ProcessAsyncRejectsInvalidPreparedOutcomesBeforeCommit(int caseValue)
    {
        var journal = new CountingJournal();
        var cursorFactory = new CountingCursorFactory();
        var handler = new RecordingHandler((context, _) => InvalidPreparation(context, caseValue));
        var processor = new ServerOperationProcessor(
            journal,
            new RecordingAuthorizer(),
            handler,
            cursorFactory,
            Options(maximumPreparedConflicts: SingleCount));

        async Task Act() => _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), CancellationToken.None);

        await Assert.That(Act).Throws<Exception>();
        await Assert.That(cursorFactory.CallCount).IsEqualTo(0);
        await Assert.That(journal.CommitCount).IsEqualTo(0);
    }

    /// <summary>Verifies prepared collections are bounded before the preparation copies items.</summary>
    /// <param name="events">Whether to reject the event collection.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ServerOperationPreparationRejectsOversizedPreparedCollectionsBeforeCopy(bool events)
    {
        IReadOnlyList<ResolvedConflict> conflicts = events
            ? []
            : new OversizedReadOnlyList<ResolvedConflict>(ConstructorMaximumPreparedItems + 1);
        IReadOnlyList<ServerPreparedEvent> preparedEvents = events
            ? new OversizedReadOnlyList<ServerPreparedEvent>(ConstructorMaximumPreparedItems + 1)
            : [];

        void Act() => _ = new ServerOperationPreparation(
            new(OperationId(FirstOperationSeed), OperationResultKind.Accepted, null, FirstVersion),
            null,
            conflicts,
            preparedEvents);

        await Assert.That(Act).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies append-only operation preparation does not stamp stream state.</summary>
    /// <param name="sqlite">Whether to use the SQLite journal.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ProcessAsyncCommitsPreparationWithoutState(bool sqlite)
    {
        using var lease = CreateJournal(sqlite);
        var processor = CreateProcessor(lease.Journal, RecordingHandler.CreateAcceptedWithoutState());

        _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), CancellationToken.None);

        var snapshot = lease.Journal.Read(StreamKey(), [OperationKey(FirstOperationSeed)]);
        await Assert.That(snapshot.State).IsNull();
        await Assert.That(snapshot.LastWriteStamp).IsNull();
        await Assert.That(snapshot.Entries[0].Result.Kind).IsEqualTo(OperationResultKind.Accepted);
    }

    /// <summary>Verifies option and cursor validation reject invalid values.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ValidationRejectsInvalidOptionsAndCursorIndexes()
    {
        var context = new ServerOperationContext(
            ClientIdentity(),
            Operation(FirstOperationSeed),
            new(Tenant, Client),
            StreamKey(),
            OperationKey(FirstOperationSeed),
            new(StreamKey(), 0, null, null, [], null, 0),
            Stamp(OperationKey(FirstOperationSeed)));
        var cursorFactory = new ServerOperationCursorFactory();

        await Assert.That(static () => new ServerOperationProcessorOptions { RetryAfter = TimeSpan.FromTicks(-1) }.Validate())
            .ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(static () => new ServerOperationProcessorOptions { MaximumBatchLogicalBytes = 0 }.Validate())
            .ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(static () => new ServerOperationProcessorOptions { MaximumPreparedLogicalBytes = 0 }.Validate())
            .ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(static () => new ServerOperationProcessorOptions { MaximumActiveRequests = 0 }.Validate())
            .ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => cursorFactory.CreateCursor(context, -1)).ThrowsExactly<ArgumentOutOfRangeException>();
    }
}
