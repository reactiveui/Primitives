// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Receive restart tests for <see cref="SyncEngine"/>.</content>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies an expired receive failure delivered during stop does not renew.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ExpiredReceiveFailureDuringStopDoesNotRenew()
    {
        var session = new ReceiveSession { SubscribeCancellationException = CreateTransportFailure(RetryFailureKind.RemoteSessionExpired) };
        var transport = new RecordingTransport { SessionOverride = session };
        await using var engine = CreateEngine(transport: transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) });

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);
        _ = await Assert.ThrowsExactlyAsync<AggregateException>(
            () => engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout));

        await Assert.That(session.SubscribeCancellationCount).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SubscribeCancellationFailures.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);
    }

    /// <summary>Verifies startup replaces receive ownership canceled while the transport is connecting.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StreamRestartDuringInitialConnectStartsReceiveWithFreshCancellation()
    {
        var releaseConnect = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var connectEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = new ReceiveSession();
        var transport = new RecordingTransport { ConnectEntered = connectEntered, ReleaseConnect = releaseConnect, SessionOverride = session };
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        await using var engine = CreateEngine(transport: transport);
        using var faultSubscription = engine.Faults.Subscribe(faults);
        var participant = new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) };
        using var registration = engine.RegisterParticipant(participant);
        Task? start = null;

        try
        {
            start = engine.StartAsync(CancellationToken.None).AsTask();
            await transport.ConnectEntered.Task.WaitAsync(GuardTimeout);
            await engine.StopStreamAsync(Stream, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
            await engine.StartStreamAsync(Stream, CancellationToken.None).AsTask().WaitAsync(GuardTimeout);

            releaseConnect.SetResult();
            await start.WaitAsync(GuardTimeout);
            await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);

            await Assert.That(session.SubscribeRequests.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(session.LastSubscriptionCancellationToken.IsCancellationRequested).IsFalse();
            await Assert.That(faults.Values).IsEmpty();
            await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        }
        finally
        {
            _ = releaseConnect.TrySetResult();
            if (start is not null)
            {
                await ObserveTaskCompletionAsync(start);
            }
        }
    }
}
