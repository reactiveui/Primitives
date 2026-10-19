// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests shared outbox admission and capacity signals for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The second registered stream sharing outbox capacity.</summary>
    private static readonly StreamId OtherOutboxStream = new("sync/outbox-other");

    /// <summary>Verifies capacity released by another stream wakes a blocked publisher.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OutboxReleaseWakesPublisherWaitingOnAnotherStream()
    {
        await using var engine = CreateEngine();
        using var first = engine.RegisterParticipant(new RecordingParticipant());
        using var second = engine.RegisterParticipant(new RecordingParticipant { StreamId = OtherOutboxStream });
        using CancellationTokenSource cancellation = new();
        var generation = engine.GetCapacityReleaseGeneration(OtherOutboxStream);
        var wait = engine.WaitForCapacityReleaseAsync(OtherOutboxStream, generation, TestQueueBytes, cancellation.Token).AsTask();
        try
        {
            await Assert.That(wait.IsCompleted).IsFalse();
            engine.NotifyCapacityReleased(Stream);
            await wait.WaitAsync(GuardTimeout);
            await Assert.That(engine.GetCapacityReleaseGeneration(OtherOutboxStream)).IsEqualTo(generation + 1);
        }
        finally
        {
            await cancellation.CancelAsync();
            await ObserveTaskCompletionAsync(wait);
        }
    }

    /// <summary>Verifies another stream's release cannot be missed before a publisher installs its waiter.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OutboxReleaseBeforeAnotherStreamWaitDoesNotLoseWakeup()
    {
        await using var engine = CreateEngine();
        using var first = engine.RegisterParticipant(new RecordingParticipant());
        using var second = engine.RegisterParticipant(new RecordingParticipant { StreamId = OtherOutboxStream });
        using CancellationTokenSource cancellation = new();
        var generation = engine.GetCapacityReleaseGeneration(OtherOutboxStream);
        engine.NotifyCapacityReleased(Stream);
        var wait = engine.WaitForCapacityReleaseAsync(OtherOutboxStream, generation, TestQueueBytes, cancellation.Token).AsTask();
        try
        {
            await wait.WaitAsync(GuardTimeout);
        }
        finally
        {
            await cancellation.CancelAsync();
            await ObserveTaskCompletionAsync(wait);
        }
    }

    /// <summary>Verifies a durable release still wakes live streams after its owning stream unregisters.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OutboxReleaseAfterUnregisterWakesOtherStream()
    {
        await using var engine = CreateEngine();
        var first = engine.RegisterParticipant(new RecordingParticipant());
        using var second = engine.RegisterParticipant(new RecordingParticipant { StreamId = OtherOutboxStream });
        first.Dispose();
        using CancellationTokenSource cancellation = new();
        var generation = engine.GetCapacityReleaseGeneration(OtherOutboxStream);
        var wait = engine.WaitForCapacityReleaseAsync(OtherOutboxStream, generation, TestQueueBytes, cancellation.Token).AsTask();
        try
        {
            engine.NotifyCapacityReleased(Stream);
            await wait.WaitAsync(GuardTimeout);
        }
        finally
        {
            await cancellation.CancelAsync();
            await ObserveTaskCompletionAsync(wait);
        }
    }

    /// <summary>Verifies local admission cannot multiply the retained input budget by registering more streams.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OutboxLocalAdmissionBytesAreSharedAcrossStreams()
    {
        var options = OccasionallyConnectedOptions.Default with { Outbox = new() { MaxBytes = AdmissionRetainedBytes } };
        await using var engine = CreateEngine(options: options);
        using var first = engine.RegisterParticipant(new RecordingParticipant());
        using var second = engine.RegisterParticipant(new RecordingParticipant { StreamId = OtherOutboxStream });
        var admission = await engine.EnterLocalCommitAsync(Stream, AdmissionRetainedBytes, CancellationToken.None);
        try
        {
            await Assert.That(async () =>
                {
                    var unexpected = await engine.EnterLocalCommitAsync(OtherOutboxStream, 1, CancellationToken.None);
                    engine.CompleteLocalCommit(unexpected);
                })
                .ThrowsExactly<QueueCapacityExceededException>();
        }
        finally
        {
            engine.CompleteLocalCommit(admission);
        }
    }

    /// <summary>Verifies another stream cannot bypass the shared active publisher limit.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OutboxLocalAdmissionCountIsSharedAcrossStreams()
    {
        var options = OccasionallyConnectedOptions.Default with { Outbox = new() { MaximumBlockedPublishers = 1 } };
        await using var engine = CreateEngine(options: options);
        using var first = engine.RegisterParticipant(new RecordingParticipant());
        using var second = engine.RegisterParticipant(new RecordingParticipant { StreamId = OtherOutboxStream });
        var admission = await engine.EnterLocalCommitAsync(Stream, AdmissionRetainedBytes, CancellationToken.None);
        try
        {
            await Assert.That(async () =>
                {
                    var unexpected = await engine.EnterLocalCommitAsync(OtherOutboxStream, AdmissionRetainedBytes, CancellationToken.None);
                    engine.CompleteLocalCommit(unexpected);
                })
                .ThrowsExactly<QueueCapacityExceededException>();
        }
        finally
        {
            engine.CompleteLocalCommit(admission);
        }

        var next = await engine.EnterLocalCommitAsync(OtherOutboxStream, AdmissionRetainedBytes, CancellationToken.None);
        engine.CompleteLocalCommit(next);
    }

    /// <summary>Verifies canceling a shared waiter refunds its count and bytes to another stream.</summary>
    /// <param name="countLimited">Whether count rather than bytes is the limiting budget.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task OutboxCanceledWaiterReleasesSharedBudget(bool countLimited)
    {
        var outbox = countLimited
            ? new OutboxOptions { MaximumBlockedPublishers = 1 }
            : new OutboxOptions { MaxBytes = AdmissionRetainedBytes };
        await using var engine = CreateEngine(options: OccasionallyConnectedOptions.Default with { Outbox = outbox });
        using var first = engine.RegisterParticipant(new RecordingParticipant());
        using var second = engine.RegisterParticipant(new RecordingParticipant { StreamId = OtherOutboxStream });
        using CancellationTokenSource cancellation = new();
        using CancellationTokenSource otherCancellation = new();
        var generation = engine.GetCapacityReleaseGeneration(Stream);
        var otherGeneration = engine.GetCapacityReleaseGeneration(OtherOutboxStream);
        var wait = engine.WaitForCapacityReleaseAsync(Stream, generation, AdmissionRetainedBytes, cancellation.Token).AsTask();
        Task? unexpected = null;
        try
        {
            await Assert.That(() => unexpected = engine.WaitForCapacityReleaseAsync(
                    OtherOutboxStream,
                    otherGeneration,
                    AdmissionRetainedBytes,
                    otherCancellation.Token).AsTask())
                .ThrowsExactly<QueueCapacityExceededException>();
        }
        finally
        {
            await cancellation.CancelAsync();
            await otherCancellation.CancelAsync();
            await ObserveTaskCompletionAsync(wait);
            if (unexpected is not null)
            {
                await ObserveTaskCompletionAsync(unexpected);
            }
        }

        await Assert.That(wait.IsCanceled).IsTrue();
        var next = engine.WaitForCapacityReleaseAsync(OtherOutboxStream, otherGeneration, AdmissionRetainedBytes, CancellationToken.None).AsTask();
        engine.NotifyCapacityReleased(Stream);
        await next.WaitAsync(GuardTimeout);
    }
}
