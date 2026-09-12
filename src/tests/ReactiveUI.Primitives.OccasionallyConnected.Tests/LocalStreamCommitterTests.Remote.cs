// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Remote receive tests for <see cref="LocalStreamCommitter{TState,TInput}"/>.</summary>
public sealed partial class LocalStreamCommitterTests
{
    /// <summary>The first remote reading value.</summary>
    private const int FirstRemoteValue = 4;

    /// <summary>The second remote reading value.</summary>
    private const int SecondRemoteValue = 5;

    /// <summary>The duplicate remote reading value.</summary>
    private const int DuplicateRemoteValue = 100;

    /// <summary>The expected filtered remote event count.</summary>
    private const int FilteredRemoteEventCount = 2;

    /// <summary>The expected duplicate event count.</summary>
    private const int DuplicateRemoteEventCount = 1;

    /// <summary>The current cursor after remote replay.</summary>
    private const string CurrentRemoteCursor = "cursor-10";

    /// <summary>The cursor before the next remote batch.</summary>
    private const string PreviousRemoteCursor = "cursor-1";

    /// <summary>The cursor after the next remote batch.</summary>
    private const string NextRemoteCursor = "cursor-2";

    /// <summary>The cursor after an advanced duplicate-only remote batch.</summary>
    private const string AdvancedRemoteCursor = "cursor-11";

    /// <summary>The mismatched cursor carried by a remote event.</summary>
    private const string MismatchedEventCursor = "cursor-mismatch-event";

    /// <summary>The expected poisoned committer message fragment.</summary>
    private const string PoisonedMessage = "poisoned";

    /// <summary>The maximum accepted UTF-8 byte count for a remote cursor.</summary>
    private const int RemoteCursorUtf8ByteLimit = 4096;

    /// <summary>A cursor length above the UTF-8 byte limit.</summary>
    private const int CursorLengthAboveUtf8ByteLimit = RemoteCursorUtf8ByteLimit + 1;

    /// <summary>Verifies a server echo replaces the optimistic effect instead of adding it again.</summary>
    /// <param name="authoritativeValue">The original or server-transformed mutation.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments(FirstReadingValue)]
    [Arguments(FirstRemoteValue)]
    public async Task ApplyRemoteBatchAsyncLocalEchoDoesNotDuplicateOptimisticMutation(int authoritativeValue)
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var local = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var echoed = CreateRemoteEvent(authoritativeValue, causedByOperationId: local.Operation.OperationId) with
        {
            Origin = new(ReconciliationClientId, local.Operation.OperationId),
        };

        var remote = await committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, NextRemoteCursor, [echoed]), CancellationToken.None);

        await Assert.That(remote.State.State.Sum).IsEqualTo(authoritativeValue);
        await Assert.That(remote.Receipt.AppliedCount).IsEqualTo(1);
        await Assert.That(remote.Inputs[0].Value).IsEqualTo(authoritativeValue);
        await Assert.That(remote.State.ServerCursor).IsEqualTo(NextRemoteCursor);
    }

    /// <summary>Verifies a mixed remote batch filters duplicates before projection and commits the new events atomically.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncCommitsFilteredNewBatchAtomically()
    {
        var duplicate = CreateRemoteEvent(DuplicateRemoteValue);
        var first = CreateRemoteEvent(FirstRemoteValue);
        var second = CreateRemoteEvent(SecondRemoteValue);
        var store = new ScriptedLocalStore();
        _ = store.MarkEventApplied(duplicate.EventId);
        var committer = await CreateRecoveredCommitterAsync(store);
        var batch = CreateRemoteBatch(null, NextRemoteCursor, [duplicate, first, second]);

        var result = await committer.ApplyRemoteBatchAsync(batch, CancellationToken.None);

        await Assert.That(result.Batch.Events.Count).IsEqualTo(FilteredRemoteEventCount);
        await Assert.That(result.Batch.Events[0]).IsSameReferenceAs(first);
        await Assert.That(result.Batch.Events[1]).IsSameReferenceAs(second);
        await Assert.That(result.Inputs.Count).IsEqualTo(FilteredRemoteEventCount);
        await Assert.That(result.Inputs[0].Value).IsEqualTo(FirstRemoteValue);
        await Assert.That(result.Inputs[1].Value).IsEqualTo(SecondRemoteValue);
        await Assert.That(result.Receipt.NextCursor).IsEqualTo(NextRemoteCursor);
        await Assert.That(result.Receipt.AppliedCount).IsEqualTo(FilteredRemoteEventCount);
        await Assert.That(result.Receipt.DuplicateCount).IsEqualTo(DuplicateRemoteEventCount);
        await Assert.That(result.State.State.Sum).IsEqualTo(FirstRemoteValue + SecondRemoteValue);
        await Assert.That(result.State.Revision).IsEqualTo(1);
        await Assert.That(committer.Current.ServerCursor).IsEqualTo(NextRemoteCursor);
        await Assert.That(store.UnappliedLookupCallCount).IsEqualTo(1);
        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(1);
        await Assert.That(store.AppliedRemoteBatch?.Events.Count).IsEqualTo(FilteredRemoteEventCount);
        await Assert.That(store.AppliedRemoteBatch?.Events[0]).IsSameReferenceAs(first);
        await Assert.That(store.AppliedRemoteBatch?.Events[1]).IsSameReferenceAs(second);
        await Assert.That(store.AppliedRemoteSnapshot?.ExpectedRevision).IsEqualTo(0);
        await Assert.That(store.AppliedRemoteSnapshot?.State.ContractId).IsEqualTo(StateContract);
    }

    /// <summary>Verifies old all-duplicate replay cannot roll back a later cursor.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncAllDuplicateReplayLeavesCursorAndState()
    {
        var snapshot = await CreateSnapshotWithCursorAsync(RecoveredSnapshotSum, CurrentRemoteCursor);
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [], RecoveredNextSequence, serverCursor: CurrentRemoteCursor) };
        var duplicate = CreateRemoteEvent(DuplicateRemoteValue);
        _ = store.MarkEventApplied(duplicate.EventId);
        var committer = await CreateRecoveredCommitterAsync(store);
        var batch = CreateRemoteBatch(PreviousRemoteCursor, NextRemoteCursor, [duplicate]);

        var result = await committer.ApplyRemoteBatchAsync(batch, CancellationToken.None);

        await Assert.That(result.Receipt.AppliedCount).IsEqualTo(0);
        await Assert.That(result.Receipt.DuplicateCount).IsEqualTo(1);
        await Assert.That(result.State.State.Sum).IsEqualTo(RecoveredSnapshotSum);
        await Assert.That(committer.Current.ServerCursor).IsEqualTo(CurrentRemoteCursor);
        await Assert.That(committer.Current.Revision).IsEqualTo(RecoveredSnapshotRevision);
        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies all-duplicate batches that continue the current cursor still durably advance the cursor.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncAllDuplicateCurrentCursorCommitsCursorAdvance()
    {
        var snapshot = await CreateSnapshotWithCursorAsync(RecoveredSnapshotSum, CurrentRemoteCursor);
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [], RecoveredNextSequence, serverCursor: CurrentRemoteCursor) };
        var duplicate = CreateRemoteEvent(DuplicateRemoteValue, serverCursor: AdvancedRemoteCursor);
        _ = store.MarkEventApplied(duplicate.EventId);
        var committer = await CreateRecoveredCommitterAsync(store);
        var batch = CreateRemoteBatch(CurrentRemoteCursor, AdvancedRemoteCursor, [duplicate]);

        var result = await committer.ApplyRemoteBatchAsync(batch, CancellationToken.None);

        await Assert.That(result.Receipt.NextCursor).IsEqualTo(AdvancedRemoteCursor);
        await Assert.That(result.Receipt.AppliedCount).IsEqualTo(0);
        await Assert.That(result.Receipt.DuplicateCount).IsEqualTo(DuplicateRemoteEventCount);
        await Assert.That(result.State.State.Sum).IsEqualTo(RecoveredSnapshotSum);
        await Assert.That(result.State.Revision).IsEqualTo(RecoveredSnapshotRevision + 1);
        await Assert.That(committer.Current.ServerCursor).IsEqualTo(AdvancedRemoteCursor);
        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(1);
        await Assert.That(store.AppliedRemoteBatch?.Events.Count).IsEqualTo(0);
    }

    /// <summary>Verifies old duplicate replay can succeed even when no mutation revision can be created.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncAllDuplicateReplayAtMaxRevisionDoesNotCheckOverflow()
    {
        var snapshot = await CreateSnapshotWithCursorAsync(RecoveredSnapshotSum, CurrentRemoteCursor, long.MaxValue);
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [], RecoveredNextSequence, serverCursor: CurrentRemoteCursor) };
        var duplicate = CreateRemoteEvent(DuplicateRemoteValue);
        _ = store.MarkEventApplied(duplicate.EventId);
        var committer = await CreateRecoveredCommitterAsync(store);

        var result = await committer.ApplyRemoteBatchAsync(CreateRemoteBatch(PreviousRemoteCursor, NextRemoteCursor, [duplicate]), CancellationToken.None);

        await Assert.That(result.State.Revision).IsEqualTo(long.MaxValue);
        await Assert.That(result.Receipt.NextCursor).IsEqualTo(CurrentRemoteCursor);
        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies duplicate replay cannot invent a current durable cursor.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncAllDuplicateReplayWithoutCurrentCursorFails()
    {
        var duplicate = CreateRemoteEvent(DuplicateRemoteValue);
        var store = new ScriptedLocalStore();
        _ = store.MarkEventApplied(duplicate.EventId);
        var committer = await CreateRecoveredCommitterAsync(store);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(CreateRemoteBatch(PreviousRemoteCursor, NextRemoteCursor, [duplicate]), CancellationToken.None).AsTask());

        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.ServerCursor).IsNull();
    }

    /// <summary>Verifies a stale reader cannot accept an all-duplicate batch without a store transaction.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncStaleReaderDuplicateBatchFailsUntilRecover()
    {
        var store = new ScriptedLocalStore();
        var fresh = await CreateRecoveredCommitterAsync(store);
        var stale = await CreateRecoveredCommitterAsync(store);
        var remoteEvent = CreateRemoteEvent(FirstRemoteValue);
        var batch = CreateRemoteBatch(null, NextRemoteCursor, [remoteEvent]);
        _ = await fresh.ApplyRemoteBatchAsync(batch, CancellationToken.None);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => stale.ApplyRemoteBatchAsync(batch, CancellationToken.None).AsTask());

        await Assert.That(stale.Current.ServerCursor).IsNull();
        await Assert.That(stale.Current.State.Sum).IsEqualTo(InitialSum);

        _ = await stale.RecoverAsync(CancellationToken.None);
        var replay = await stale.ApplyRemoteBatchAsync(batch, CancellationToken.None);

        await Assert.That(replay.State.ServerCursor).IsEqualTo(NextRemoteCursor);
        await Assert.That(replay.Receipt.AppliedCount).IsEqualTo(0);
    }

    /// <summary>Verifies new work with an unexpected previous cursor fails after inbox lookup but before projection.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncMixedWrongPreviousCursorFailsBeforeProjection()
    {
        var duplicate = CreateRemoteEvent(DuplicateRemoteValue);
        var next = CreateRemoteEvent(FirstRemoteValue);
        var store = new ScriptedLocalStore();
        _ = store.MarkEventApplied(duplicate.EventId);
        var serializer = new ScriptedPayloadSerializer();
        var committer = await CreateRecoveredCommitterAsync(store, serializer);
        var batch = CreateRemoteBatch(PreviousRemoteCursor, NextRemoteCursor, [duplicate, next]);

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(batch, CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("cursor");
        await Assert.That(store.UnappliedLookupCallCount).IsEqualTo(1);
        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(0);
        await Assert.That(serializer.RemoteInputDeserializeCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
    }

    /// <summary>Verifies empty batches still validate cursor continuity.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncEmptyBatchWithWrongPreviousCursorFails()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var batch = CreateRemoteBatch(PreviousRemoteCursor, NextRemoteCursor, []);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(batch, CancellationToken.None).AsTask());

        await Assert.That(store.UnappliedLookupCallCount).IsEqualTo(1);
        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.ServerCursor).IsNull();
    }

    /// <summary>Verifies malformed inbox lookup results poison the committer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncLookupMustReturnExactSubset()
    {
        var store = new ScriptedLocalStore { UnappliedEventIdsOverride = [Guid.NewGuid()] };
        var serializer = new ScriptedPayloadSerializer();
        var committer = await CreateRecoveredCommitterAsync(store, serializer);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue)]), CancellationToken.None).AsTask());
        var poisoned = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(poisoned?.Message).Contains(PoisonedMessage);
        await Assert.That(serializer.RemoteInputDeserializeCount).IsEqualTo(0);
        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies malformed remote store receipts do not make projected state visible.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncStateChangesOnlyAfterValidReceipt()
    {
        var store = new ScriptedLocalStore { RemoteReceiptRevisionOffset = 1 };
        var committer = await CreateRecoveredCommitterAsync(store);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue)]), CancellationToken.None).AsTask());
        var poisoned = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, "cursor-3", [CreateRemoteEvent(SecondRemoteValue, serverCursor: "cursor-3")]), CancellationToken.None).AsTask());

        await Assert.That(poisoned?.Message).Contains(PoisonedMessage);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
        await Assert.That(committer.Current.ServerCursor).IsNull();
    }

    /// <summary>Verifies cancellation after the remote store transaction returns the committed receipt.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncCancellationAfterStoreCommitReturnsReceipt()
    {
        using CancellationTokenSource source = new();
        var store = new ScriptedLocalStore { CancelAfterSuccessfulRemoteApply = source };
        var committer = await CreateRecoveredCommitterAsync(store);

        var result = await committer.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue)]),
            source.Token);

        await Assert.That(source.IsCancellationRequested).IsTrue();
        await Assert.That(result.Receipt.NextCursor).IsEqualTo(NextRemoteCursor);
        await Assert.That(result.State.State.Sum).IsEqualTo(FirstRemoteValue);
        await Assert.That(committer.Current.ServerCursor).IsEqualTo(NextRemoteCursor);
    }

    /// <summary>Verifies remote payloads that decode to the wrong input type fail before persistence.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncRejectsWrongDecodedInputTypeBeforeStore()
    {
        var store = new ScriptedLocalStore();
        var serializer = new ScriptedPayloadSerializer { DeserializeInputAsState = true };
        var committer = await CreateRecoveredCommitterAsync(store, serializer);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue)]), CancellationToken.None).AsTask());

        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
    }

    /// <summary>Verifies oversized cursors fail before inbox lookup.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncRejectsOversizedCursorBeforeLookup()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var cursor = new string('a', CursorLengthAboveUtf8ByteLimit);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, cursor, [CreateRemoteEvent(FirstRemoteValue)]), CancellationToken.None).AsTask());

        await Assert.That(store.UnappliedLookupCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies the last event cursor must match the batch next cursor.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncRejectsMismatchedLastEventCursorBeforeStore()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(
                CreateRemoteBatch(null, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue, serverCursor: MismatchedEventCursor)]),
                CancellationToken.None).AsTask());

        await Assert.That(store.UnappliedLookupCallCount).IsEqualTo(0);
        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies default causal operation identifiers fail before inbox lookup.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncRejectsDefaultCausedByOperationIdBeforeLookup()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(
                CreateRemoteBatch(null, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue, causedByOperationId: default(OperationId))]),
                CancellationToken.None).AsTask());

        await Assert.That(store.UnappliedLookupCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies duplicate remote event identifiers fail before inbox lookup.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncRejectsDuplicateEventIdsBeforeLookup()
    {
        var eventId = Guid.NewGuid();
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(
                CreateRemoteBatch(null, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue, eventId), CreateRemoteEvent(SecondRemoteValue, eventId)]),
                CancellationToken.None).AsTask());

        await Assert.That(store.UnappliedLookupCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies malformed batch identifiers fail before inbox lookup.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncRejectsDefaultBatchIdBeforeLookup()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var batch = new RemoteEventBatch(Guid.Empty, Stream, null, NextRemoteCursor, []);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(batch, CancellationToken.None).AsTask());

        await Assert.That(store.UnappliedLookupCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies foreign batch streams fail before inbox lookup.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncRejectsWrongBatchStreamBeforeLookup()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var batch = new RemoteEventBatch(Guid.NewGuid(), new StreamId("other-stream"), null, NextRemoteCursor, []);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(batch, CancellationToken.None).AsTask());

        await Assert.That(store.UnappliedLookupCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies malformed cursor Unicode fails before inbox lookup.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncRejectsMalformedCursorUnicodeBeforeLookup()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, "\ud800", []), CancellationToken.None).AsTask());
        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, "\ud800a", []), CancellationToken.None).AsTask());
        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, "\udc00", []), CancellationToken.None).AsTask());

        await Assert.That(store.UnappliedLookupCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies valid surrogate-pair cursors are accepted.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncAcceptsWellFormedSupplementaryCursor()
    {
        const string cursor = "cursor-🚀";
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);

        var result = await committer.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, cursor, [CreateRemoteEvent(FirstRemoteValue, serverCursor: cursor)]),
            CancellationToken.None);

        await Assert.That(result.Receipt.NextCursor).IsEqualTo(cursor);
        await Assert.That(result.State.State.Sum).IsEqualTo(FirstRemoteValue);
    }

    /// <summary>Verifies missing event entries fail before inbox lookup.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncRejectsMissingEventBeforeLookup()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var batch = new RemoteEventBatch(Guid.NewGuid(), Stream, null, NextRemoteCursor, [null!]);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(batch, CancellationToken.None).AsTask());

        await Assert.That(store.UnappliedLookupCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies empty event identifiers fail before inbox lookup.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncRejectsDefaultEventIdBeforeLookup()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue, Guid.Empty)]), CancellationToken.None).AsTask());

        await Assert.That(store.UnappliedLookupCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies foreign event streams fail before inbox lookup.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncRejectsWrongEventStreamBeforeLookup()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(
                CreateRemoteBatch(null, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue, streamId: new StreamId("other-stream"))]),
                CancellationToken.None).AsTask());

        await Assert.That(store.UnappliedLookupCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies missing payloads fail before inbox lookup.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncRejectsMissingPayloadBeforeLookup()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var remoteEvent = new RemoteEvent(Guid.NewGuid(), Stream, NextRemoteCursor, CommittedUtc, null, null!, new Dictionary<string, string>());

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, NextRemoteCursor, [remoteEvent]), CancellationToken.None).AsTask());

        await Assert.That(store.UnappliedLookupCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies invalid payload contracts fail before inbox lookup.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncRejectsInvalidPayloadContractsBeforeLookup()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var wrongContract = CreateRemoteEventWithPayload(CreateRemotePayload("wrong-contract", InputSchemaVersion));
        var zeroSchema = CreateRemoteEventWithPayload(CreateRemotePayload(InputContract, 0));
        var futureSchema = CreateRemoteEventWithPayload(CreateRemotePayload(InputContract, InputSchemaVersion + 1));

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, NextRemoteCursor, [wrongContract]), CancellationToken.None).AsTask());
        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, NextRemoteCursor, [zeroSchema]), CancellationToken.None).AsTask());
        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, NextRemoteCursor, [futureSchema]), CancellationToken.None).AsTask());

        await Assert.That(store.UnappliedLookupCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies missing inbox lookup results poison the committer before projection.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncRejectsNullInboxLookupAndPoisons()
    {
        var store = new ScriptedLocalStore { ReturnNullUnappliedLookupResult = true };
        var serializer = new ScriptedPayloadSerializer();
        var committer = await CreateRecoveredCommitterAsync(store, serializer);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue)]), CancellationToken.None).AsTask());
        var poisoned = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(poisoned?.Message).Contains(PoisonedMessage);
        await Assert.That(serializer.RemoteInputDeserializeCount).IsEqualTo(0);
        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies mutable inbox lookup results are copied before filtering.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncSnapshotsMutableLookupBeforeFiltering()
    {
        var first = CreateRemoteEvent(FirstRemoteValue);
        var second = CreateRemoteEvent(SecondRemoteValue);
        var store = new ScriptedLocalStore { UnappliedEventIdsOverride = new SwitchingLookupResult(first.EventId, second.EventId) };
        var committer = await CreateRecoveredCommitterAsync(store);

        var result = await committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, NextRemoteCursor, [first, second]), CancellationToken.None);

        await Assert.That(result.Batch.Events.Count).IsEqualTo(1);
        await Assert.That(result.Batch.Events[0]).IsSameReferenceAs(first);
        await Assert.That(result.State.State.Sum).IsEqualTo(FirstRemoteValue);
    }

    /// <summary>Creates a remote batch.</summary>
    /// <param name="previousCursor">The previous cursor.</param>
    /// <param name="nextCursor">The next cursor.</param>
    /// <param name="events">The batch events.</param>
    /// <returns>The remote batch.</returns>
    private static RemoteEventBatch CreateRemoteBatch(
        string? previousCursor,
        string nextCursor,
        IReadOnlyList<RemoteEvent> events) =>
        new(Guid.NewGuid(), Stream, previousCursor, nextCursor, events);

    /// <summary>Creates a recovered snapshot with an explicit cursor.</summary>
    /// <param name="sum">The state sum.</param>
    /// <param name="serverCursor">The snapshot cursor.</param>
    /// <param name="revision">The snapshot revision.</param>
    /// <returns>The local snapshot.</returns>
    private static async ValueTask<LocalSnapshot> CreateSnapshotWithCursorAsync(
        int sum,
        string serverCursor,
        long revision = RecoveredSnapshotRevision)
    {
        var serializer = new ScriptedPayloadSerializer();
        var payload = await serializer.SerializeAsync(StateContract, StateSchemaVersion, new ReadingState(sum), CancellationToken.None);
        return new(Stream, SnapshotFormatVersion, serverCursor, payload, revision, CommittedUtc);
    }

    /// <summary>Creates a remote payload envelope.</summary>
    /// <param name="contractId">The payload contract identifier.</param>
    /// <param name="schemaVersion">The payload schema version.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope CreateRemotePayload(string contractId, int schemaVersion)
    {
        var payload = System.Text.Encoding.UTF8.GetBytes(FirstRemoteValue.ToString(CultureInfo.InvariantCulture));
        return new(contractId, schemaVersion, TestContentType, payload, "hash-invalid");
    }

    /// <summary>Creates a remote event with an explicit payload.</summary>
    /// <param name="payload">The payload envelope.</param>
    /// <returns>The remote event.</returns>
    private static RemoteEvent CreateRemoteEventWithPayload(PayloadEnvelope payload) =>
        new(Guid.NewGuid(), Stream, NextRemoteCursor, CommittedUtc, null, payload, new Dictionary<string, string>());

    /// <summary>Creates a remote event.</summary>
    /// <param name="value">The payload value.</param>
    /// <param name="eventId">The event identifier.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="serverCursor">The event cursor.</param>
    /// <param name="causedByOperationId">The optional causal operation identifier.</param>
    /// <returns>The remote event.</returns>
    private static RemoteEvent CreateRemoteEvent(
        int value,
        Guid? eventId = null,
        StreamId? streamId = null,
        string serverCursor = NextRemoteCursor,
        OperationId? causedByOperationId = null)
    {
        var payload = System.Text.Encoding.UTF8.GetBytes(value.ToString(CultureInfo.InvariantCulture));
        var envelope = new PayloadEnvelope(InputContract, InputSchemaVersion, TestContentType, payload, $"hash-{value}");
        return new(eventId ?? Guid.NewGuid(), streamId ?? Stream, serverCursor, CommittedUtc, causedByOperationId, envelope, new Dictionary<string, string>());
    }

    /// <summary>Returns one identifier during validation and another during later reads.</summary>
    /// <param name="first">The first identifier.</param>
    /// <param name="later">The later identifier.</param>
    private sealed class SwitchingLookupResult(Guid first, Guid later) : IReadOnlyList<Guid>
    {
        /// <summary>The first identifier.</summary>
        private readonly Guid _first = first;

        /// <summary>The later identifier.</summary>
        private readonly Guid _later = later;

        /// <summary>The number of indexed reads.</summary>
        private int _reads;

        /// <inheritdoc/>
        public int Count => 1;

        /// <inheritdoc/>
        public Guid this[int index]
        {
            get
            {
                if (index != 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(index));
                }

                var read = _reads;
                _reads++;
                return read == 0 ? _first : _later;
            }
        }

        /// <inheritdoc/>
        public IEnumerator<Guid> GetEnumerator()
        {
            yield return this[0];
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
