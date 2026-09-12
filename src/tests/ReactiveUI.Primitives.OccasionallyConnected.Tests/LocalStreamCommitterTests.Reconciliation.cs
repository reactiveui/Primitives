// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests reconciliation with pending noninvertible application mutations.</summary>
public sealed partial class LocalStreamCommitterTests
{
    /// <summary>The local client whose authenticated server echoes are under test.</summary>
    private const string ReconciliationClientId = "reconciliation-client";

    /// <summary>The number of locally committed replacement edits awaiting the server.</summary>
    private const int PendingReplacementCount = 2;

    /// <summary>Verifies persisted replay ordering for noninvertible edits across restart and complete receive groups.</summary>
    /// <param name="ownCompletion">Whether the complete operation belongs to the bound client.</param>
    /// <param name="zeroEvents">Whether the complete operation has no server effects.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(false, false)]
    [Arguments(false, true)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task ApplyRemoteBatchAsyncRetainsOrderedReplacementEditsAcrossRestart(bool ownCompletion, bool zeroEvents)
    {
        await using var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(ReconciliationClientId, 1, false) { ClientId = ReconciliationClientId }, CancellationToken.None);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, Subscription, CancellationToken.None);
        var template = CreateOptions(new(), new());
        var options = template with { SubscriptionId = subscription, Dependencies = template.Dependencies with { Store = store, Projection = new ReplacementProjection() } };
        var first = CreateLocalCommitter(options);
        _ = await first.RecoverAsync(CancellationToken.None);
        var policy = OperationPolicy.Default with { Durability = OperationDurability.Volatile };
        var local = await first.CommitAsync(new MutableReading { Value = FirstReadingValue }, policy, CancellationToken.None);
        _ = await first.CommitAsync(new MutableReading { Value = SecondReadingValue }, policy, CancellationToken.None);
        var reopened = CreateLocalCommitter(options);
        _ = await reopened.RecoverAsync(CancellationToken.None);
        var origin = new RemoteEventOrigin(ownCompletion ? ReconciliationClientId : "foreign-client", local.Operation.OperationId);
        var received = CreateRemoteEvent(FirstRemoteValue, causedByOperationId: local.Operation.OperationId) with { Origin = origin };
        var batch = CreateRemoteBatch(null, NextRemoteCursor, zeroEvents ? [] : [received]) with
        {
            CompletedOperations = [new(origin, zeroEvents ? [] : [received.EventId])],
        };

        var result = await reopened.ApplyRemoteBatchAsync(batch, CancellationToken.None);

        await Assert.That(result.State.State.Sum).IsEqualTo(SecondReadingValue);
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(PendingReplacementCount);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(ownCompletion ? 1 : PendingReplacementCount);
        var final = CreateLocalCommitter(options);
        _ = await final.RecoverAsync(CancellationToken.None);
        await Assert.That(final.Current.State.Sum).IsEqualTo(SecondReadingValue);
    }

    /// <summary>Verifies an unknown historical authoritative base preserves pending local work until resynchronization.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncPreservesHistoricalPendingStateUntilResynchronization()
    {
        var snapshot = await CreateSnapshotAsync(new(FirstReadingValue));
        var pending = CreatePendingOperation(FirstClientSequence, FirstReadingValue);
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [pending], RecoveredNextSequence) };
        var committer = await CreateRecoveredCommitterAsync(store);

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => committer.ApplyRemoteBatchAsync(
            CreateRemoteBatch(RecoveryCursor, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue)]),
            CancellationToken.None).AsTask());

        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue);
        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(0);
        await Assert.That(store.Recovery.PendingOperations[0]).IsSameReferenceAs(pending);
    }

    /// <summary>Verifies malformed recovered replay entries cannot reach application projection or storage mutation.</summary>
    /// <param name="foreignStream">Whether the entry belongs to another stream instead of repeating a sequence.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ApplyRemoteBatchAsyncRejectsMalformedReplayEntries(bool foreignStream)
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var committed = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var invalid = foreignStream ? committed.Operation with { StreamId = new("another-stream") } : committed.Operation;
        store.Recovery = store.Recovery with { ReplayOperations = [committed.Operation, invalid] };

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => committer.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue)]),
            CancellationToken.None).AsTask());

        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue);
        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies a missing recovered replay entry is rejected before projection.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncRejectsMissingReplayEntry()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        store.Recovery = store.Recovery with { ReplayOperations = new SyncOperation[1] };

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => committer.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue)]),
            CancellationToken.None).AsTask());

        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies a complete proof for an inbox event can retire its optimistic operation without rewinding the cursor.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncOldDuplicateCompletionRebuildsWithoutCursorRewind()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store);
        var local = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var legacyEvent = CreateRemoteEvent(FirstRemoteValue, causedByOperationId: local.Operation.OperationId);
        var initial = await committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, NextRemoteCursor, [legacyEvent]), CancellationToken.None);
        var provenEvent = legacyEvent with { Origin = new(ReconciliationClientId, local.Operation.OperationId) };
        var completion = CreateRemoteBatch(null, NextRemoteCursor, [provenEvent]) with
        {
            CompletedOperations = [new(new(ReconciliationClientId, local.Operation.OperationId), [provenEvent.EventId])],
        };

        var reconciled = await committer.ApplyRemoteBatchAsync(completion, CancellationToken.None);

        await Assert.That(initial.State.State.Sum).IsEqualTo(FirstReadingValue + FirstRemoteValue);
        await Assert.That(reconciled.State.State.Sum).IsEqualTo(FirstRemoteValue);
        await Assert.That(reconciled.State.ServerCursor).IsEqualTo(NextRemoteCursor);
        await Assert.That(reconciled.Receipt.AppliedCount).IsEqualTo(0);
        await Assert.That(reconciled.Receipt.DuplicateCount).IsEqualTo(1);
        await Assert.That(reconciled.State.Revision).IsEqualTo(initial.State.Revision + 1);
    }

    /// <summary>Verifies a later receive after restart never replays an already included local operation.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RecoveredCommitterDoesNotReplayIncludedOperationOnLaterReceive()
    {
        await using var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(ReconciliationClientId, 1, false) { ClientId = ReconciliationClientId }, CancellationToken.None);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, Subscription, CancellationToken.None);
        var template = CreateOptions(new(), new());
        var options = template with { SubscriptionId = subscription, Dependencies = template.Dependencies with { Store = store } };
        var first = CreateLocalCommitter(options);
        _ = await first.RecoverAsync(CancellationToken.None);
        var local = await first.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default with { Durability = OperationDurability.Volatile }, CancellationToken.None);
        var echoed = CreateRemoteEvent(FirstRemoteValue, causedByOperationId: local.Operation.OperationId) with { Origin = new(ReconciliationClientId, local.Operation.OperationId) };
        var batch = CreateRemoteBatch(null, NextRemoteCursor, [echoed]) with
        {
            CompletedOperations = [new(new(ReconciliationClientId, local.Operation.OperationId), [echoed.EventId])],
        };
        _ = await first.ApplyRemoteBatchAsync(batch, CancellationToken.None);
        var reopened = CreateLocalCommitter(options);
        _ = await reopened.RecoverAsync(CancellationToken.None);

        var result = await reopened.ApplyRemoteBatchAsync(
            CreateRemoteBatch(NextRemoteCursor, AdvancedRemoteCursor, [CreateRemoteEvent(SecondRemoteValue, serverCursor: AdvancedRemoteCursor)]),
            CancellationToken.None);

        await Assert.That(result.State.State.Sum).IsEqualTo(FirstRemoteValue + SecondRemoteValue);
        var recovered = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.ReplayOperations.Count).IsEqualTo(0);
    }

    /// <summary>Verifies recovery rejects an authoritative checkpoint encoded under an input contract.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RecoverAsyncRejectsWrongAuthoritativeContract()
    {
        var serializer = new ScriptedPayloadSerializer();
        var inputPayload = await serializer.SerializeAsync(InputContract, InputSchemaVersion, new MutableReading { Value = FirstReadingValue }, CancellationToken.None);
        var snapshot = await CreateSnapshotAsync(new(FirstReadingValue));
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot with { AuthoritativeState = inputPayload }, [], RecoveredNextSequence) };
        var committer = CreateCommitter(store, serializer);

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => committer.RecoverAsync(CancellationToken.None).AsTask());

        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
        await Assert.That(store.CommitCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies malformed state codecs fail before projection or persistence.</summary>
    /// <param name="wrongContract">Whether the codec writes the wrong contract instead of decoding the wrong type.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task InvalidProjectionStateCodecLeavesCommittedStateUnchanged(bool wrongContract)
    {
        var store = new ScriptedLocalStore();
        var serializer = new ScriptedPayloadSerializer { SerializeStateAsInputContract = wrongContract, DeserializeStateAsInput = !wrongContract };
        var committer = await CreateRecoveredCommitterAsync(store, serializer);

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(store.CommitCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
        await Assert.That(committer.Current.Revision).IsEqualTo(0);
    }

    /// <summary>Verifies local edits never reinterpret a historical optimistic snapshot as authoritative state.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CommitAsyncPreservesUnknownHistoricalAuthoritativeState()
    {
        var snapshot = await CreateSnapshotAsync(new(FirstReadingValue));
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [], RecoveredNextSequence) };
        var committer = await CreateRecoveredCommitterAsync(store);

        _ = await committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None);

        await Assert.That(store.Recovery.Snapshot?.AuthoritativeState).IsNull();
        await Assert.That(committer.Current.AuthoritativePayload).IsNull();
        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue + SecondReadingValue);
    }

    /// <summary>Verifies a failed store transaction does not expose an in-place projection mutation.</summary>
    /// <param name="remote">Whether the failing transaction receives a remote event.</param>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task FailedTransactionDoesNotExposeInPlaceProjectionMutation(bool remote)
    {
        var failure = new InvalidOperationException("store rejected mutation");
        var store = new ScriptedLocalStore { CommitException = failure, RemoteCommitException = failure };
        var options = CreateOptions(store, new());
        var committer = CreateLocalCommitter(options with { Dependencies = options.Dependencies with { Projection = new MutatingProjection() } });
        _ = await committer.RecoverAsync(CancellationToken.None);
        var before = committer.Current;
        Func<Task> commit = remote
            ? () => committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue)]), CancellationToken.None).AsTask()
            : () => committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None).AsTask();

        var thrown = await Assert.That(commit).ThrowsExactly<InvalidOperationException>();

        await Assert.That(thrown).IsEqualTo(failure);
        await Assert.That(before.State.Sum).IsEqualTo(InitialSum);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
        await Assert.That(committer.Current.Revision).IsEqualTo(0);
        await Assert.That(store.Recovery.Snapshot).IsNull();
    }

    /// <summary>Verifies a fresh committer persists the initial authoritative base separately from its optimistic state.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task CommitAsyncPersistsInitialAuthoritativeBaseWithOptimisticState()
    {
        await using var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(ReconciliationClientId, 1, false) { ClientId = ReconciliationClientId }, CancellationToken.None);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, Subscription, CancellationToken.None);
        var options = CreateOptions(new(), new());
        var committer = CreateLocalCommitter(options with
        {
            SubscriptionId = subscription,
            Dependencies = options.Dependencies with { Store = store },
        });
        _ = await committer.RecoverAsync(CancellationToken.None);
        var policy = OperationPolicy.Default with { Durability = OperationDurability.Volatile };

        _ = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, policy, CancellationToken.None);

        var recovery = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(recovery.Snapshot?.AuthoritativeState).IsNotNull();
        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue);
        await Assert.That(recovery.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovery.Snapshot?.AuthoritativeState?.PayloadHash).IsEqualTo("hash-0");

        _ = await committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, policy, CancellationToken.None);
        var afterSecondCommit = await store.RecoverStreamAsync(Stream, subscription, CancellationToken.None);
        await Assert.That(afterSecondCommit.Snapshot?.AuthoritativeState).IsEqualTo(recovery.Snapshot?.AuthoritativeState);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue + SecondReadingValue);
    }

    /// <summary>Verifies reopening a committer preserves enough information to replace an optimistic effect with its transformed echo.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task RecoveredCommitterReplacesOptimisticEffectWithCompleteTransformedEcho()
    {
        await using var store = new InMemoryLocalStoreAdapter();
        await store.InitializeAsync(new(ReconciliationClientId, 1, false) { ClientId = ReconciliationClientId }, CancellationToken.None);
        var subscription = await store.GetOrCreateSubscriptionIdAsync(Stream, Subscription, CancellationToken.None);
        var template = CreateOptions(new(), new());
        var options = template with { SubscriptionId = subscription, Dependencies = template.Dependencies with { Store = store } };
        var first = CreateLocalCommitter(options);
        _ = await first.RecoverAsync(CancellationToken.None);
        var policy = OperationPolicy.Default with { Durability = OperationDurability.Volatile };
        var local = await first.CommitAsync(new MutableReading { Value = FirstReadingValue }, policy, CancellationToken.None);
        var reopened = CreateLocalCommitter(options);
        _ = await reopened.RecoverAsync(CancellationToken.None);
        var echoed = CreateRemoteEvent(FirstRemoteValue, causedByOperationId: local.Operation.OperationId) with
        {
            Origin = new(ReconciliationClientId, local.Operation.OperationId),
        };
        var batch = CreateRemoteBatch(null, NextRemoteCursor, [echoed]) with
        {
            CompletedOperations = [new(new(ReconciliationClientId, local.Operation.OperationId), [echoed.EventId])],
        };

        var result = await reopened.ApplyRemoteBatchAsync(batch, CancellationToken.None);

        await Assert.That(result.State.State.Sum).IsEqualTo(FirstRemoteValue);
        await Assert.That(result.Inputs[0].Value).IsEqualTo(FirstRemoteValue);
        var recoveredAgain = CreateLocalCommitter(options);
        _ = await recoveredAgain.RecoverAsync(CancellationToken.None);
        await Assert.That(recoveredAgain.Current.State.Sum).IsEqualTo(FirstRemoteValue);
    }

    /// <summary>Verifies remote replacement preserves the last pending local replacement in client sequence order.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncReplaysPendingReplacementsAfterAuthoritativeState()
    {
        var store = new ScriptedLocalStore();
        var options = CreateOptions(store, new(), new SequenceOperationIdSource());
        var committer = CreateLocalCommitter(options with { Dependencies = options.Dependencies with { Projection = new ReplacementProjection() } });
        _ = await committer.RecoverAsync(CancellationToken.None);
        _ = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        _ = await committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None);

        var remote = await committer.ApplyRemoteBatchAsync(
            CreateRemoteBatch(null, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue)]),
            CancellationToken.None);

        await Assert.That(remote.State.State.Sum).IsEqualTo(SecondReadingValue);
        await Assert.That(remote.Inputs[0].Value).IsEqualTo(FirstRemoteValue);
        await Assert.That(store.Recovery.PendingOperations.Count).IsEqualTo(PendingReplacementCount);
        await Assert.That(store.Recovery.PendingOperations[0].ClientSequence).IsEqualTo(1);
        await Assert.That(store.Recovery.PendingOperations[1].ClientSequence).IsEqualTo(PendingReplacementCount);
    }

    /// <summary>Models replacement edits whose previous state cannot be recovered by subtracting an input.</summary>
    private sealed class ReplacementProjection : ILocalProjection<ReadingState, MutableReading>
    {
        /// <inheritdoc/>
        public ReadingState InitialState { get; } = new(InitialSum);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadingState ApplyLocal(ReadingState state, MutableReading input, SyncOperation operation) => new(input.Value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadingState ApplyRemote(ReadingState state, MutableReading input, RemoteEvent remoteEvent) => new(input.Value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadingState Reconcile(ReadingState state, ConflictResolutionResult result) => state;
    }

    /// <summary>Models an application projection that changes its input state in place.</summary>
    private sealed class MutatingProjection : ILocalProjection<ReadingState, MutableReading>
    {
        /// <inheritdoc/>
        public ReadingState InitialState { get; } = new(InitialSum);

        /// <inheritdoc/>
        public ReadingState ApplyLocal(ReadingState state, MutableReading input, SyncOperation operation)
        {
            state.Sum += input.Value;
            return state;
        }

        /// <inheritdoc/>
        public ReadingState ApplyRemote(ReadingState state, MutableReading input, RemoteEvent remoteEvent)
        {
            state.Sum += input.Value;
            return state;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadingState Reconcile(ReadingState state, ConflictResolutionResult result) => state;
    }
}
