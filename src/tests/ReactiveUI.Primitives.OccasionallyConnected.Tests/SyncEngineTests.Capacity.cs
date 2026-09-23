// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Capacity waiter tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>Verifies caller cancellation after release does not fault the completed waiter.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CapacityReleaseWinsOverLaterCallerCancellation()
    {
        await using var engine = CreateEngine();
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        var generation = engine.GetCapacityReleaseGeneration(Stream);
        using CancellationTokenSource canceledAfterRelease = new();

        var wait = engine.WaitForCapacityReleaseAsync(
                Stream,
                generation,
                TestQueueBytes,
                canceledAfterRelease.Token)
            .AsTask();

        engine.NotifyCapacityReleased(Stream);
        await canceledAfterRelease.CancelAsync();
        await wait.WaitAsync(GuardTimeout);

        await Assert.That(wait.IsCompletedSuccessfully).IsTrue();
        await Assert.That(engine.GetCapacityReleaseGeneration(Stream)).IsEqualTo(generation + 1);
    }
}
