// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Raw operation admission tests for <see cref="SyncEngine"/>.</summary>
public sealed partial class SyncEngineTests
{
    /// <summary>The byte cap used by retained-operation admission tests.</summary>
    private const long AdmissionRetainedBytes = 2048;

    /// <summary>The metadata value length that must exceed the retained byte cap by itself.</summary>
    private const int OversizedMetadataCharacters = 4096;

    /// <summary>The publisher count used by re-registered admission tests.</summary>
    private const int ReregisteredAdmissionPublisherLimit = 2;

    /// <summary>The retained bytes owned by each overlapping registration.</summary>
    private const long ReregisteredAdmissionBytes = AdmissionRetainedBytes / ReregisteredAdmissionPublisherLimit;

    /// <summary>Verifies an extra completion cannot underflow admission counts or prevent later lifecycle drain.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DuplicateCompletedAdmissionDoesNotPoisonLaterAdmissionOrStop()
    {
        await using var engine = CreateEngine();
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        var first = await engine.EnterLocalCommitAsync(Stream, AdmissionRetainedBytes, CancellationToken.None);
        engine.CompleteLocalCommit(first);

        await Assert.That(() => engine.CompleteLocalCommit(first)).ThrowsExactly<InvalidOperationException>();

        var next = await engine.EnterLocalCommitAsync(Stream, AdmissionRetainedBytes, CancellationToken.None);
        engine.CompleteLocalCommit(next);
        await engine.StopAsync(CancellationToken.None).AsTask().WaitAsync(GuardTimeout);
        var generation = engine.GetCapacityReleaseGeneration(Stream);

        await Assert.That(() => engine.WaitForCapacityReleaseAsync(Stream, generation, AdmissionRetainedBytes, CancellationToken.None).AsTask())
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies raw metadata is charged while store initialization is blocked.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task BlockedInitializerAdmissionCountsRawOperationMetadataBytes()
    {
        TaskCompletionSource initializeEntered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseInitialize = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new RecordingStore { InitializeEntered = initializeEntered, ReleaseInitialize = releaseInitialize };
        var options = OccasionallyConnectedOptions.Default with { Outbox = new() { MaxBytes = AdmissionRetainedBytes } };
        await using var engine = CreateEngine(store, options: options);
        using var registration = engine.RegisterParticipant(new RecordingParticipant());
        var first = engine.EnqueueOperationAsync(CreateOperation(operationId: OperationId.New()), CancellationToken.None).AsTask();
        await initializeEntered.Task.WaitAsync(GuardTimeout);
        var metadata = new Dictionary<string, string> { ["retained"] = new('m', OversizedMetadataCharacters) };
        var metadataHeavy = CreateOperation(payload: EmptyPayload, operationId: OperationId.New()) with { Metadata = metadata };
        try
        {
            await Assert.That(async () => await engine.EnqueueOperationAsync(metadataHeavy, CancellationToken.None).ConfigureAwait(false))
                .ThrowsExactly<QueueCapacityExceededException>();
        }
        finally
        {
            releaseInitialize.SetResult();
        }

        var receipt = await first.WaitAsync(GuardTimeout);

        await Assert.That(receipt.OperationId).IsNotEqualTo(metadataHeavy.OperationId);
    }

    /// <summary>Verifies completing an old local admission after re-registration cannot free the new signal's capacity.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task OldLocalAdmissionCompletionDoesNotReleaseReregisteredCapacity()
    {
        var store = new RecordingStore();
        var options = OccasionallyConnectedOptions.Default with
        {
            Outbox = new() { MaxBytes = AdmissionRetainedBytes, MaximumBlockedPublishers = ReregisteredAdmissionPublisherLimit },
        };
        await using var engine = CreateEngine(store, options: options);
        var stream = Stream;
        var oldRegistration = engine.RegisterParticipant(new RecordingParticipant { StreamId = stream });
        var oldAdmission = await engine.EnterLocalCommitAsync(stream, ReregisteredAdmissionBytes, CancellationToken.None);
        oldRegistration.Dispose();

        using var newRegistration = engine.RegisterParticipant(new RecordingParticipant { StreamId = stream });
        var currentAdmission = await engine.EnterLocalCommitAsync(stream, ReregisteredAdmissionBytes, CancellationToken.None);
        var currentAdmissionCompleted = false;
        try
        {
            engine.CompleteLocalCommit(oldAdmission);

            await Assert.That(async () => await engine.EnterLocalCommitAsync(stream, AdmissionRetainedBytes, CancellationToken.None).ConfigureAwait(false))
                .ThrowsExactly<QueueCapacityExceededException>();

            engine.CompleteLocalCommit(currentAdmission);
            currentAdmissionCompleted = true;
            var afterReleaseAdmission = await engine.EnterLocalCommitAsync(stream, AdmissionRetainedBytes, CancellationToken.None);
            engine.CompleteLocalCommit(afterReleaseAdmission);
        }
        finally
        {
            if (!currentAdmissionCompleted)
            {
                engine.CompleteLocalCommit(currentAdmission);
            }
        }
    }

    /// <summary>Verifies stop releases a publisher waiting for outbox space without needing a remote peer.</summary>
    /// <param name="startEngine">Whether the engine has started before the queue becomes full.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task StopAsyncTerminatesPublisherWaitingForFullOutbox(bool startEngine)
    {
        await using var engine = CreateEngine();
        var participant = new RecordingParticipant { CapacityFailures = int.MaxValue };
        using var registration = engine.RegisterParticipant(participant);
        using CancellationTokenSource publisherCancellation = new();
        if (startEngine)
        {
            await engine.StartAsync(CancellationToken.None);
        }

        var publisher = engine.EnqueueOperationAsync(CreateOperation(), publisherCancellation.Token).AsTask();
        await participant.CapacityFailureObserved.Task.WaitAsync(GuardTimeout);
        var stop = engine.StopAsync(CancellationToken.None).AsTask();
        try
        {
            await stop.WaitAsync(GuardTimeout);
            await Assert.That(async () => await publisher.ConfigureAwait(false)).ThrowsExactly<InvalidOperationException>();
            await Assert.That(participant.CommittedOperation).IsNull();
            await Assert.That(participant.CommitAttempts).IsEqualTo(ExpectedSingleOperation);
        }
        finally
        {
            await publisherCancellation.CancelAsync();
            await Assert.That(async () => await publisher.ConfigureAwait(false)).Throws<Exception>();
            await stop.WaitAsync(GuardTimeout);
        }
    }
}
