// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Receive pump inactive-stream tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies a batch yielded after stream stop is not applied or acknowledged.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopStreamDropsBatchYieldedAfterCancellationBeforeApply()
    {
        var batchMoveNextEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseBatchMoveNext = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var batch = CreateReceiveBatch();
        var session = new ReceiveSession { BatchMoveNextEntered = batchMoveNextEntered, ReleaseBatchMoveNext = releaseBatchMoveNext };
        session.Batches.Add(batch);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        var participant = new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) };
        await using var engine = CreateEngine(transport: new() { SessionOverride = session });
        using var faultSubscription = engine.Faults.Subscribe(faults);
        using var registration = engine.RegisterParticipant(participant);
        Task? stop = null;

        try
        {
            await engine.StartAsync(CancellationToken.None);
            await batchMoveNextEntered.Task.WaitAsync(GuardTimeout);
            stop = engine.StopStreamAsync(Stream, CancellationToken.None).AsTask();
            _ = await Assert.ThrowsExactlyAsync<TimeoutException>(() => stop.WaitAsync(StopCallbackObservationWindow));

            releaseBatchMoveNext.SetResult();
            await stop.WaitAsync(GuardTimeout);
            await session.SubscribeCompleted.Task.WaitAsync(GuardTimeout);

            await Assert.That(participant.RemoteApplyCalls).IsEqualTo(0);
            await Assert.That(participant.AppliedRemoteBatches).IsEmpty();
            await Assert.That(session.Acknowledgements).IsEmpty();
            await Assert.That(faults.Values).IsEmpty();
            await Assert.That(session.SubscribeCompletedCount).IsEqualTo(ExpectedSingleOperation);

            await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        }
        finally
        {
            _ = releaseBatchMoveNext.TrySetResult();
            if (stop is not null)
            {
                await ObserveTaskCompletionAsync(stop).ConfigureAwait(false);
            }
        }
    }
}
