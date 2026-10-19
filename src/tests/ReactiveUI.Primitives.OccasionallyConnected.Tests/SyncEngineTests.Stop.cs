// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Stop lifecycle tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies caller cancellation cannot abandon an accepted stop while a local commit drains.</summary>
    /// <returns>The assertion task.</returns>
    /// <exception cref="TimeoutException">The accepted stop does not complete after the blocked commit drains.</exception>
    [Test]
    public async Task StopAsyncCallerCancellationWaitsOnlyAndAcceptedStopCompletesAfterCommitDrains()
    {
        var store = new RecordingStore();
        var firstSession = new ReceiveSession();
        var secondSession = new ReceiveSession();
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(firstSession);
        transport.Sessions.Enqueue(secondSession);
        TaskCompletionSource commitEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseCommit = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var engine = CreateEngine(store, transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { CommitEntered = commitEntered, ReleaseCommit = releaseCommit });
        using CancellationTokenSource stopWait = new();
        var operation = CreateOperation();

        await engine.StartAsync(CancellationToken.None);
        var publish = engine.EnqueueOperationAsync(operation, CancellationToken.None).AsTask();
        Task? stop = null;
        try
        {
            await commitEntered.Task.WaitAsync(GuardTimeout);

            stop = engine.StopAsync(stopWait.Token).AsTask();
            await stopWait.CancelAsync();

            await Assert.That(async () => await stop.WaitAsync(GuardTimeout).ConfigureAwait(false))
                .ThrowsExactly<TaskCanceledException>();
            await Assert.That(firstSession.DisposeCalls).IsEqualTo(0);

            releaseCommit.SetResult();
            _ = await publish.WaitAsync(GuardTimeout);
            await WaitForConditionAsync(() => firstSession.DisposeCalls == ExpectedSingleOperation);

            await engine.StartAsync(CancellationToken.None);
            var restartReceipt = await engine.EnqueueOperationAsync(
                CreateOperation(operationId: OperationId.New()),
                CancellationToken.None);

            await Assert.That(restartReceipt.State).IsEqualTo(SyncOperationState.SavedLocally);
            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedRestartConnectCalls);
            await Assert.That(firstSession.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(secondSession.DisposeCalls).IsEqualTo(0);
        }
        finally
        {
            _ = releaseCommit.TrySetResult();
            await ObserveTaskCompletionAsync(publish).ConfigureAwait(false);
            if (stop is not null)
            {
                await ObserveTaskCompletionAsync(stop).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Verifies concurrent stop callers share cleanup after one caller cancels its wait.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task ConcurrentStopAsyncSharesCommitDrainAfterCallerCancellation()
    {
        var firstSession = new ReceiveSession();
        var secondSession = new ReceiveSession();
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(firstSession);
        transport.Sessions.Enqueue(secondSession);
        TaskCompletionSource commitEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseCommit = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var engine = CreateEngine(transport: transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { CommitEntered = commitEntered, ReleaseCommit = releaseCommit });
        using CancellationTokenSource canceledWait = new();

        await engine.StartAsync(CancellationToken.None);
        var publish = engine.EnqueueOperationAsync(CreateOperation(), CancellationToken.None).AsTask();
        Task? canceledStop = null;
        Task? joinedStop = null;
        try
        {
            await commitEntered.Task.WaitAsync(GuardTimeout);
            canceledStop = engine.StopAsync(canceledWait.Token).AsTask();
            await canceledWait.CancelAsync();

            await Assert.That(async () => await canceledStop.WaitAsync(GuardTimeout).ConfigureAwait(false))
                .ThrowsExactly<TaskCanceledException>();

            joinedStop = engine.StopAsync(CancellationToken.None).AsTask();
            await Assert.That(joinedStop.IsCompleted).IsFalse();
            releaseCommit.SetResult();
            _ = await publish.WaitAsync(GuardTimeout);
            await joinedStop.WaitAsync(GuardTimeout);

            await Assert.That(firstSession.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedSingleOperation);

            await engine.StartAsync(CancellationToken.None);

            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedRestartConnectCalls);
            await Assert.That(secondSession.DisposeCalls).IsEqualTo(0);
        }
        finally
        {
            _ = releaseCommit.TrySetResult();
            await ObserveTaskCompletionAsync(publish).ConfigureAwait(false);
            if (canceledStop is not null)
            {
                await ObserveTaskCompletionAsync(canceledStop).ConfigureAwait(false);
            }

            if (joinedStop is not null)
            {
                await ObserveTaskCompletionAsync(joinedStop).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Verifies a start requested while stop drains local work restarts after stop cleanup.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StartAsyncDuringStopDrainWaitsForAcceptedStopBeforeRestarting()
    {
        var firstSession = new ReceiveSession();
        var secondSession = new ReceiveSession();
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(firstSession);
        transport.Sessions.Enqueue(secondSession);
        TaskCompletionSource commitEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseCommit = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var engine = CreateEngine(transport: transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { CommitEntered = commitEntered, ReleaseCommit = releaseCommit });

        await engine.StartAsync(CancellationToken.None);
        var publish = engine.EnqueueOperationAsync(CreateOperation(), CancellationToken.None).AsTask();
        Task? stop = null;
        Task? restart = null;
        try
        {
            await commitEntered.Task.WaitAsync(GuardTimeout);
            stop = engine.StopAsync(CancellationToken.None).AsTask();
            await Assert.That(stop.IsCompleted).IsFalse();

            restart = engine.StartAsync(CancellationToken.None).AsTask();
            await Assert.That(restart.IsCompleted).IsFalse();

            releaseCommit.SetResult();
            _ = await publish.WaitAsync(GuardTimeout);
            await stop.WaitAsync(GuardTimeout);
            await restart.WaitAsync(GuardTimeout);

            await Assert.That(firstSession.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedRestartConnectCalls);
            await Assert.That(secondSession.DisposeCalls).IsEqualTo(0);

            var receipt = await engine.EnqueueOperationAsync(
                CreateOperation(operationId: OperationId.New()),
                CancellationToken.None);

            await Assert.That(receipt.State).IsEqualTo(SyncOperationState.SavedLocally);
        }
        finally
        {
            _ = releaseCommit.TrySetResult();
            await ObserveTaskCompletionAsync(publish).ConfigureAwait(false);
            if (stop is not null)
            {
                await ObserveTaskCompletionAsync(stop).ConfigureAwait(false);
            }

            if (restart is not null)
            {
                await ObserveTaskCompletionAsync(restart).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Verifies cancellation of a start caller during stop does not cancel the accepted restart intent.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StartAsyncCallerCancellationDuringStopDrainDoesNotCancelAcceptedRestart()
    {
        var firstSession = new ReceiveSession();
        var secondSession = new ReceiveSession();
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(firstSession);
        transport.Sessions.Enqueue(secondSession);
        TaskCompletionSource commitEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseCommit = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var engine = CreateEngine(transport: transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant { CommitEntered = commitEntered, ReleaseCommit = releaseCommit });
        using CancellationTokenSource restartWait = new();

        await engine.StartAsync(CancellationToken.None);
        var publish = engine.EnqueueOperationAsync(CreateOperation(), CancellationToken.None).AsTask();
        Task? stop = null;
        Task? restart = null;
        try
        {
            await commitEntered.Task.WaitAsync(GuardTimeout);
            stop = engine.StopAsync(CancellationToken.None).AsTask();
            restart = engine.StartAsync(restartWait.Token).AsTask();
            await Assert.That(restart.IsCompleted).IsFalse();

            await restartWait.CancelAsync();

            await Assert.That(async () => await restart.WaitAsync(GuardTimeout).ConfigureAwait(false))
                .ThrowsExactly<TaskCanceledException>();

            releaseCommit.SetResult();
            _ = await publish.WaitAsync(GuardTimeout);
            await stop.WaitAsync(GuardTimeout);
            await WaitForConditionAsync(() => transport.ConnectCalls == ExpectedRestartConnectCalls);

            await Assert.That(firstSession.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(secondSession.DisposeCalls).IsEqualTo(0);

            var receipt = await engine.EnqueueOperationAsync(
                CreateOperation(operationId: OperationId.New()),
                CancellationToken.None);

            await Assert.That(receipt.State).IsEqualTo(SyncOperationState.SavedLocally);
        }
        finally
        {
            _ = releaseCommit.TrySetResult();
            await ObserveTaskCompletionAsync(publish).ConfigureAwait(false);
            if (stop is not null)
            {
                await ObserveTaskCompletionAsync(stop).ConfigureAwait(false);
            }

            if (restart is not null)
            {
                await ObserveTaskCompletionAsync(restart).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Verifies a stop cleanup failure is surfaced and a later start can retry cleanup.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task StopAsyncCleanupFailureCanBeRetriedByStartAsync()
    {
        var cleanupFailure = new InvalidOperationException("stop cleanup failed");
        var firstSession = new ReceiveSession { DisposeException = cleanupFailure };
        var secondSession = new ReceiveSession();
        var transport = new RecordingTransport();
        transport.Sessions.Enqueue(firstSession);
        transport.Sessions.Enqueue(secondSession);
        await using var engine = CreateEngine(transport: transport);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());

        await engine.StartAsync(CancellationToken.None);

        await Assert.That(async () => await engine.StopAsync(CancellationToken.None).ConfigureAwait(false))
            .ThrowsExactly<InvalidOperationException>();

        await engine.StartAsync(CancellationToken.None);

        await Assert.That(firstSession.DisposeCalls).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(transport.ConnectCalls).IsEqualTo(ExpectedRestartConnectCalls);
        await Assert.That(secondSession.DisposeCalls).IsEqualTo(0);
    }

    /// <summary>Observes task completion without letting cleanup mask the assertion that failed first.</summary>
    /// <param name="task">The task to observe.</param>
    /// <returns>The observation task.</returns>
    private static async Task ObserveTaskCompletionAsync(Task task)
    {
        try
        {
            await task.WaitAsync(GuardTimeout).ConfigureAwait(false);
        }
        catch
        {
            _ = task.Exception;
        }
    }
}
