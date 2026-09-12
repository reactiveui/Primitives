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
}
