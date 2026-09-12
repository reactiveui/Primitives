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
