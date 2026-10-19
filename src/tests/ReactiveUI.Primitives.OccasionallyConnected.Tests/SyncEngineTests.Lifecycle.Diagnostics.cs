// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Lifecycle diagnostic tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies lifecycle states publish current aggregate queue totals.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task LifecycleStatePublishesQueuedTotalsFromQueueDiagnostics()
    {
        var operation = CreateOperation();
        var pendingBytes = SyncEngine.GetOperationRetainedBytes(operation);
        var observer = new RecordingObserver<SyncState>();
        await using var engine = CreateEngine();
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        using var subscription = engine.SyncStates.Subscribe(observer);
        engine.RecordSavedLocalCommit(
            Stream,
            operation,
            new(ExpectedSingleOperation, pendingBytes, ExpectedSingleOperation),
            new(operation.OperationId, operation.ClientSequence, SyncOperationState.SavedLocally, operation.TimestampUtc));

        await engine.StartAsync(CancellationToken.None);
        await WaitForConditionAsync(() => observer.Values.Exists(static state => state.Status == SyncLifecycleStatus.Online));
        var online = observer.Values.Single(static state => state.Status == SyncLifecycleStatus.Online);
        await engine.StopAsync(CancellationToken.None);
        await WaitForConditionAsync(() => observer.Values.Exists(static state => state.Status == SyncLifecycleStatus.Stopped));
        var stopped = observer.Values.Single(static state => state.Status == SyncLifecycleStatus.Stopped);

        await AssertLifecycleQueueTotalsAsync(online, SyncLifecycleStatus.Online, pendingBytes);
        await AssertLifecycleQueueTotalsAsync(stopped, SyncLifecycleStatus.Stopped, pendingBytes);
    }

    /// <summary>Asserts a lifecycle state contains expected queue totals.</summary>
    /// <param name="state">The observed lifecycle state.</param>
    /// <param name="status">The expected status.</param>
    /// <param name="pendingBytes">The expected retained bytes.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertLifecycleQueueTotalsAsync(
        SyncState state,
        SyncLifecycleStatus status,
        long pendingBytes)
    {
        await Assert.That(state.Status).IsEqualTo(status);
        await Assert.That(state.PendingOperations).IsEqualTo(ExpectedSingleOperation);
        await Assert.That(state.PendingBytes).IsEqualTo(pendingBytes);
    }
}
