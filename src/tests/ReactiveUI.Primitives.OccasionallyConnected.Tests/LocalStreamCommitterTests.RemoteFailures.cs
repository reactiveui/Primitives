// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Remote transaction failure and restart tests.</summary>
public sealed partial class LocalStreamCommitterTests
{
    /// <summary>The failed attempt plus its successful retry.</summary>
    private const int ExpectedRemoteStoreAttempts = 2;

    /// <summary>Verifies every receipt invariant before publishing projected state.</summary>
    /// <param name="fault">The malformed receipt field.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments("null")]
    [Arguments("cursor")]
    [Arguments("applied")]
    [Arguments("duplicates")]
    public async Task RemoteMalformedReceiptPoisonsWithoutPublishing(string fault)
    {
        var store = new ScriptedLocalStore
        {
            ReturnNullRemoteApplyResult = fault == "null",
            TransformRemoteReceipt = fault == "null" ? null : receipt => fault switch
            {
                "cursor" => receipt with { NextCursor = "wrong-cursor" },
                "applied" => receipt with { AppliedCount = receipt.AppliedCount + 1 },
                _ => receipt with { DuplicateCount = 1 },
            },
        };
        var committer = await CreateRecoveredCommitterAsync(store);
        var previous = committer.Current;
        var batch = CreateRemoteBatch(null, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue)]);

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => committer.ApplyRemoteBatchAsync(batch, CancellationToken.None).AsTask());
        await Assert.That(committer.Current).IsSameReferenceAs(previous);
        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => committer.RecoverAsync(CancellationToken.None).AsTask());
        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(1);
    }

    /// <summary>Verifies failed storage leaves a batch retryable without changing visible state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task RemoteStoreFailureCanRetryThenRecoverWithoutDoubleProjection()
    {
        var error = new IOException("transaction failed");
        var store = new ScriptedLocalStore { RemoteCommitException = error };
        var committer = await CreateRecoveredCommitterAsync(store);
        var batch = CreateRemoteBatch(null, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue)]);
        var previous = committer.Current;

        var thrown = await Assert.ThrowsExactlyAsync<IOException>(() => committer.ApplyRemoteBatchAsync(batch, CancellationToken.None).AsTask());
        await Assert.That(thrown).IsSameReferenceAs(error);
        await Assert.That(committer.Current).IsSameReferenceAs(previous);
        await Assert.That(store.AppliedRemoteBatch).IsNull();

        store.RemoteCommitException = null;
        _ = await committer.ApplyRemoteBatchAsync(batch, CancellationToken.None);
        var restarted = await CreateRecoveredCommitterAsync(store);
        var replay = await restarted.ApplyRemoteBatchAsync(batch, CancellationToken.None);

        await Assert.That(replay.State.State.Sum).IsEqualTo(FirstRemoteValue);
        await Assert.That(replay.State.SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(replay.State.NextClientSequence).IsEqualTo(previous.NextClientSequence);
        await Assert.That(replay.Receipt.AppliedCount).IsEqualTo(0);
        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(ExpectedRemoteStoreAttempts);
    }

    /// <summary>Verifies remote storage shares the same exclusive owner as local commits and recovery.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task RemotePendingCommitExcludesOtherTransactionsAndPublishesOnlyAfterReceipt()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new ScriptedLocalStore { BeforeRemoteCommitAsync = () => release.Task };
        var committer = await CreateRecoveredCommitterAsync(store);
        var previous = committer.Current;
        var batch = CreateRemoteBatch(null, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue)]);
        var pending = committer.ApplyRemoteBatchAsync(batch, CancellationToken.None).AsTask();
        try
        {
            await Assert.That(pending.IsCompleted).IsFalse();
            await Assert.That(committer.Current).IsSameReferenceAs(previous);
            await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => committer.ApplyRemoteBatchAsync(batch, CancellationToken.None).AsTask());
            await Assert.ThrowsExactlyAsync<InvalidOperationException>(
                () => committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None).AsTask());
            await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => committer.RecoverAsync(CancellationToken.None).AsTask());
        }
        finally
        {
            _ = release.TrySetResult();
            _ = await pending;
        }

        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstRemoteValue);
        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(1);
    }

    /// <summary>Verifies cancellation after projection but before storage leaves no durable effects.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task RemoteCancellationAfterSerializationLeavesBatchRetryable()
    {
        using var cancellation = new CancellationTokenSource();
        var serializer = new ScriptedPayloadSerializer { CancelAfterStateSerialization = cancellation };
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer);
        var previous = committer.Current;
        var batch = CreateRemoteBatch(null, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue)]);

        await Assert.ThrowsExactlyAsync<OperationCanceledException>(() => committer.ApplyRemoteBatchAsync(batch, cancellation.Token).AsTask());
        await Assert.That(committer.Current).IsSameReferenceAs(previous);
        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(0);
        serializer.CancelAfterStateSerialization = null;
        var result = await committer.ApplyRemoteBatchAsync(batch, CancellationToken.None);
        await Assert.That(result.State.State.Sum).IsEqualTo(FirstRemoteValue);
    }

    /// <summary>Verifies duplicate lookup identifiers are rejected before decoding.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task RemoteLookupWithRepeatedIdentifierPoisonsBeforeDecode()
    {
        var remoteEvent = CreateRemoteEvent(FirstRemoteValue);
        var store = new ScriptedLocalStore { UnappliedEventIdsOverride = [remoteEvent.EventId, remoteEvent.EventId] };
        var serializer = new ScriptedPayloadSerializer();
        var committer = await CreateRecoveredCommitterAsync(store, serializer);
        var batch = CreateRemoteBatch(null, NextRemoteCursor, [remoteEvent]);

        await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => committer.ApplyRemoteBatchAsync(batch, CancellationToken.None).AsTask());
        await Assert.That(serializer.RemoteInputDeserializeCount).IsEqualTo(0);
        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(0);
    }
}
