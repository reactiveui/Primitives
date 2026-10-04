// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Retry persistence boundaries for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Checks retry publication, timer registration, and the configured attempt limit.</summary>
    /// <param name="engine">The upload engine.</param>
    /// <param name="clock">The upload clock.</param>
    /// <param name="store">The recording store.</param>
    /// <param name="session">The upload session.</param>
    /// <param name="faults">The recorded faults.</param>
    /// <param name="operation">The pending operation.</param>
    /// <param name="retryDelay">The retry timer delay.</param>
    /// <returns>The assertion task.</returns>
    private static async Task AssertConfiguredRetryLimitAsync(
        SyncEngine engine,
        ManualTimerTimeProvider clock,
        RecordingStore store,
        PreparedSession session,
        RecordingObserver<OccasionallyConnectedFault> faults,
        SyncOperation operation,
        TimeSpan retryDelay)
    {
        await engine.StartAsync(CancellationToken.None);
        engine.NotifyLocalCommitReady(Stream, operation);
        await DriveUploadDwellWithTraceAsync(clock, store, session, faults, operationStates: null);
        await WaitForUploadConditionWithTraceAsync(
            () => store.RetryStates.TryGetValue(operation.OperationId, out var state)
                && state.TransientAttemptCount == ExpectedSingleOperation,
            store,
            session,
            faults,
            operationStates: null);

        if (store.ReleaseRetryStateSave is { } release)
        {
            await Assert.That(clock.HasTimerDueIn(retryDelay)).IsFalse();
            await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedSingleOperation);
            await Assert.That(faults.Values.Count).IsEqualTo(0);
            _ = release.TrySetResult();
        }

        await WaitForUploadConditionWithTraceAsync(
            () => clock.HasTimerDueIn(retryDelay),
            store,
            session,
            faults,
            operationStates: null);
        clock.Advance(retryDelay);
        await WaitForUploadConditionWithTraceAsync(
            () => faults.Values.Count == ExpectedSingleOperation,
            store,
            session,
            faults,
            operationStates: null);

        await Assert.That(session.SentBatches.Count).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(store.ReleaseLeaseCalls).IsEqualTo(ExpectedCapacityCommitAttempts);
        await Assert.That(store.RetryStates[operation.OperationId].TransientAttemptCount)
            .IsEqualTo(ExpectedSingleOperation);
        await Assert.That(faults.Values[0].Code).IsEqualTo(UploadAttemptFaultCode);
        await engine.StopAsync(CancellationToken.None);
    }
}
