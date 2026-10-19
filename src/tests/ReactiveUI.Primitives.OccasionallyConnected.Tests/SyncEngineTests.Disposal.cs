// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Disposal tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies a failed receive cancellation does not skip later owned cleanup.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DisposeAsyncPreservesReceiveCancellationFailureAndClosesOwnedDependencies()
    {
        var cancellationFailure = new InvalidOperationException("subscription cancellation failed");
        var session = new ReceiveSession { SubscribeCancellationException = cancellationFailure, DisposeException = new IOException("session disposal failed") };
        var store = new RecordingStore();
        var transport = new RecordingTransport { SessionOverride = session };
        var engine = CreateEngine(store, transport);
        var faults = new RecordingObserver<OccasionallyConnectedFault>();
        using var registration = engine.RegisterParticipant(
            new RecordingParticipant { ReceiveSubscription = new(Stream, Subscription, Cursor: null, StartPosition.Latest) });
        using var faultSubscription = engine.Faults.Subscribe(faults);

        await engine.StartAsync(CancellationToken.None);
        await session.SubscribeEntered.Task.WaitAsync(GuardTimeout);

        Exception? disposalFailure = null;
        try
        {
            await engine.DisposeAsync().AsTask().WaitAsync(GuardTimeout);
        }
        catch (Exception exception)
        {
            disposalFailure = exception;
        }

        var failureTrace = CreateExceptionTrace(disposalFailure, faults);
        await Assert.That(disposalFailure).IsNotNull();
        await Assert.That(failureTrace).Contains(nameof(InvalidOperationException));
        await Assert.That(session.SubscribeCancellationFailures.Count).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(session.SubscribeCancellationFailures[0]).IsSameReferenceAs(cancellationFailure);
        await Assert.That(session.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(store.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
    }
}
