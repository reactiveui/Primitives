// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Quarantine tests for <see cref="LocalStreamCommitter{TState,TInput}"/>.</summary>
public sealed partial class LocalStreamCommitterTests
{
    /// <summary>The corrupt payload hash used to trigger scripted schema failure.</summary>
    private const string CorruptPayloadHash = "hash-corrupt";

    /// <summary>The stable quarantine message fragment.</summary>
    private const string QuarantineMessageFragment = "quarantine";

    /// <summary>The second client sequence used by quarantine replay tests.</summary>
    private const long QuarantineSecondClientSequence = 2;

    /// <summary>Verifies recovered persisted snapshot schema failures are quarantined and fail the stream closed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncSnapshotPayloadSchemaFailureQuarantinesStream()
    {
        var snapshot = new LocalSnapshot(
            Stream,
            SnapshotFormatVersion,
            RecoveryCursor,
            new PayloadEnvelope(StateContract, StateSchemaVersion, TestContentType, "42"u8.ToArray(), CorruptPayloadHash),
            Revision: 1,
            CommittedUtc);
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [], CrossStreamNextSequence) };
        var serializer = new ScriptedPayloadSerializer { RejectStateHash = true };
        var committer = CreateCommitterWithStore(store, serializer);

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains(QuarantineMessageFragment);
        await Assert.That(store.QuarantineCallCount).IsEqualTo(1);
        await Assert.That(store.QuarantineRequest?.Source).IsEqualTo(LocalPayloadQuarantineSource.Snapshot);
        await Assert.That(store.QuarantineRequest?.Reason).IsEqualTo(LocalPayloadQuarantineReason.PayloadHashMismatch);
        await Assert.That(store.QuarantineRequest?.Envelope).IsSameReferenceAs(snapshot.State);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
    }

    /// <summary>Verifies recovered authoritative snapshot schema failures quarantine the original authoritative envelope.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncAuthoritativeSnapshotPayloadSchemaFailureQuarantinesOriginalEnvelope()
    {
        var optimistic = await CreateStatePayloadAsync(FirstReadingValue);
        var authoritative = new PayloadEnvelope(
            StateContract,
            StateSchemaVersion,
            TestContentType,
            "0"u8.ToArray(),
            CorruptPayloadHash);
        var snapshot = new LocalSnapshot(
            Stream,
            SnapshotFormatVersion,
            RecoveryCursor,
            optimistic,
            Revision: 1,
            CommittedUtc) { AuthoritativeState = authoritative };
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [], CrossStreamNextSequence) };
        var serializer = new ScriptedPayloadSerializer { RejectStateHash = true };
        var committer = CreateCommitterWithStore(store, serializer);

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());
        var poisoned = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = RetryCommitValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains(QuarantineMessageFragment);
        await Assert.That(poisoned?.Message).Contains(PoisonedMessage);
        await Assert.That(store.QuarantineCallCount).IsEqualTo(1);
        await Assert.That(store.QuarantineRequest?.Source).IsEqualTo(LocalPayloadQuarantineSource.Snapshot);
        await Assert.That(store.QuarantineRequest?.OperationId).IsNull();
        await Assert.That(store.QuarantineRequest?.EventId).IsNull();
        await Assert.That(store.QuarantineRequest?.Reason).IsEqualTo(LocalPayloadQuarantineReason.PayloadHashMismatch);
        await Assert.That(store.QuarantineRequest?.Envelope).IsSameReferenceAs(authoritative);
        await Assert.That(store.QuarantineRequest?.Envelope).IsNotSameReferenceAs(optimistic);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
    }

    /// <summary>Verifies remote schema failures quarantine before projection, cursor advancement, or store apply.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncPayloadSchemaFailureQuarantinesBeforeApply()
    {
        var store = new ScriptedLocalStore();
        var serializer = new ScriptedPayloadSerializer { RejectInputHash = true };
        var committer = await CreateRecoveredCommitterAsync(store, serializer);
        var remoteEvent = CreateRemoteEventWithPayload(
            new(InputContract, InputSchemaVersion, TestContentType, "4"u8.ToArray(), CorruptPayloadHash));

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, NextRemoteCursor, [remoteEvent]), CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains(QuarantineMessageFragment);
        await Assert.That(store.QuarantineCallCount).IsEqualTo(1);
        await Assert.That(store.QuarantineRequest?.Source).IsEqualTo(LocalPayloadQuarantineSource.RemoteEvent);
        await Assert.That(store.QuarantineRequest?.Reason).IsEqualTo(LocalPayloadQuarantineReason.PayloadHashMismatch);
        await Assert.That(store.QuarantineRequest?.EventId).IsEqualTo(remoteEvent.EventId);
        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.ServerCursor).IsNull();
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
    }

    /// <summary>Verifies cancellation during decode is not converted to quarantine.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplyRemoteBatchAsyncCanceledDecodeDoesNotQuarantine()
    {
        using CancellationTokenSource source = new();
        var serializer = new ScriptedPayloadSerializer { CancelDuringInputDeserialization = source };
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer);

        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            () => committer.ApplyRemoteBatchAsync(CreateRemoteBatch(null, NextRemoteCursor, [CreateRemoteEvent(FirstRemoteValue)]), source.Token).AsTask());

        await Assert.That(exception?.CancellationToken).IsEqualTo(source.Token);
        await Assert.That(store.QuarantineCallCount).IsEqualTo(0);
        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies cancellation during local input decode is not converted to quarantine.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitAsyncCanceledLocalDecodeDoesNotQuarantine()
    {
        using CancellationTokenSource source = new();
        var serializer = new ScriptedPayloadSerializer { CancelDuringInputDeserialization = source };
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer);

        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            () => committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, source.Token).AsTask());

        await Assert.That(exception?.CancellationToken).IsEqualTo(source.Token);
        await Assert.That(store.QuarantineCallCount).IsEqualTo(0);
        await Assert.That(store.CommitCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies corrupt persisted authoritative state is quarantined as snapshot evidence before result storage.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncCorruptAuthoritativePayloadQuarantinesSnapshotBeforeStore()
    {
        var snapshot = await CreateSnapshotWithAuthoritativeAsync(
            FirstReadingValue,
            new(StateContract, StateSchemaVersion, TestContentType, "0"u8.ToArray(), CorruptPayloadHash));
        var pending = CreatePendingOperation(FirstClientSequence, FirstReadingValue);
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [pending], CrossStreamNextSequence) };
        var serializer = new ScriptedPayloadSerializer();
        var committer = await CreateRecoveredCommitterAsync(store, serializer);
        var batchId = Guid.NewGuid();
        serializer.RejectStateHash = true;

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplySyncResultAsync(
                new(batchId, [pending]),
                new(batchId, [new(pending.OperationId, OperationResultKind.Rejected, ResultRejectedReasonCode, null)], null, null),
                CancellationToken.None).AsTask());
        var poisoned = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = RetryCommitValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains(QuarantineMessageFragment);
        await Assert.That(poisoned?.Message).Contains(PoisonedMessage);
        await Assert.That(store.QuarantineCallCount).IsEqualTo(1);
        await Assert.That(store.QuarantineRequest?.Source).IsEqualTo(LocalPayloadQuarantineSource.Snapshot);
        await Assert.That(store.QuarantineRequest?.OperationId).IsNull();
        await Assert.That(store.QuarantineRequest?.EventId).IsNull();
        await Assert.That(store.QuarantineRequest?.Reason).IsEqualTo(LocalPayloadQuarantineReason.PayloadHashMismatch);
        await Assert.That(store.QuarantineRequest?.Envelope).IsSameReferenceAs(snapshot.AuthoritativeState);
        await Assert.That(store.ResultApplyCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies corrupt persisted replay operations are quarantined as outbox evidence before receive storage.</summary>
    /// <param name="fault">The persisted replay payload fault.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments("malformed")]
    [Arguments("hash")]
    public async Task ApplyRemoteBatchAsyncCorruptPersistedReplayPayloadQuarantinesOutboxBeforeStore(string fault)
    {
        var malformed = string.Equals(fault, "malformed", StringComparison.Ordinal);
        PayloadEnvelope payload = malformed
            ? new(InputContract, InputSchemaVersion, TestContentType, "not-int"u8.ToArray(), "hash-not-int")
            : new(InputContract, InputSchemaVersion, TestContentType, "21"u8.ToArray(), CorruptPayloadHash);
        var pending = CreatePendingOperation(FirstClientSequence, FirstReadingValue) with { Payload = payload };
        var snapshot = await CreateSnapshotWithAuthoritativeAsync(FirstReadingValue, await CreateStatePayloadAsync(InitialSum));
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [pending], CrossStreamNextSequence) };
        var serializer = new ScriptedPayloadSerializer { TreatMalformedInputAsSchemaFailure = malformed, RejectInputHash = !malformed };
        var committer = await CreateRecoveredCommitterAsync(store, serializer);

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplyRemoteBatchAsync(
                CreateRemoteBatch(RecoveryCursor, NextRemoteCursor, [CreateRemoteEvent(SecondReadingValue)]),
                CancellationToken.None).AsTask());
        var poisoned = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = RetryCommitValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains(QuarantineMessageFragment);
        await Assert.That(poisoned?.Message).Contains(PoisonedMessage);
        await Assert.That(store.QuarantineCallCount).IsEqualTo(1);
        await Assert.That(store.QuarantineRequest?.Source).IsEqualTo(LocalPayloadQuarantineSource.OutboxOperation);
        await Assert.That(store.QuarantineRequest?.OperationId).IsEqualTo(pending.OperationId);
        await Assert.That(store.QuarantineRequest?.EventId).IsNull();
        var expectedReason = malformed ? LocalPayloadQuarantineReason.SchemaRejected : LocalPayloadQuarantineReason.PayloadHashMismatch;
        await Assert.That(store.QuarantineRequest?.Reason).IsEqualTo(expectedReason);
        await Assert.That(store.QuarantineRequest?.Envelope).IsSameReferenceAs(payload);
        await Assert.That(store.RemoteApplyCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies corrupt persisted replay operations are quarantined as outbox evidence before result reconciliation storage.</summary>
    /// <param name="fault">The persisted replay payload fault.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments("malformed")]
    [Arguments("hash")]
    public async Task ApplySyncResultAsyncCorruptPersistedReplayPayloadQuarantinesOutboxBeforeStore(string fault)
    {
        var malformed = string.Equals(fault, "malformed", StringComparison.Ordinal);
        PayloadEnvelope payload = malformed
            ? new(InputContract, InputSchemaVersion, TestContentType, "not-int"u8.ToArray(), "hash-not-int")
            : new(InputContract, InputSchemaVersion, TestContentType, "21"u8.ToArray(), CorruptPayloadHash);
        var first = CreatePendingOperation(FirstClientSequence, FirstReadingValue) with { Payload = payload };
        var second = CreatePendingOperation(QuarantineSecondClientSequence, SecondReadingValue);
        var snapshot = await CreateSnapshotWithAuthoritativeAsync(
            FirstReadingValue + SecondReadingValue,
            await CreateStatePayloadAsync(InitialSum));
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [first, second], RecoveredNextSequence) };
        var serializer = new ScriptedPayloadSerializer { TreatMalformedInputAsSchemaFailure = malformed, RejectInputHash = !malformed };
        var committer = await CreateRecoveredCommitterAsync(store, serializer);
        var batchId = Guid.NewGuid();

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.ApplySyncResultAsync(
                new(batchId, [first, second]),
                new(
                    batchId,
                    [
                        new(first.OperationId, OperationResultKind.Accepted, null, null),
                        new(second.OperationId, OperationResultKind.Rejected, ResultRejectedReasonCode, null),
                    ],
                    null,
                    null),
                CancellationToken.None).AsTask());
        var poisoned = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = RetryCommitValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains(QuarantineMessageFragment);
        await Assert.That(poisoned?.Message).Contains(PoisonedMessage);
        await Assert.That(store.QuarantineCallCount).IsEqualTo(1);
        await Assert.That(store.QuarantineRequest?.Source).IsEqualTo(LocalPayloadQuarantineSource.OutboxOperation);
        await Assert.That(store.QuarantineRequest?.OperationId).IsEqualTo(first.OperationId);
        await Assert.That(store.QuarantineRequest?.EventId).IsNull();
        var expectedReason = malformed ? LocalPayloadQuarantineReason.SchemaRejected : LocalPayloadQuarantineReason.PayloadHashMismatch;
        await Assert.That(store.QuarantineRequest?.Reason).IsEqualTo(expectedReason);
        await Assert.That(store.QuarantineRequest?.Envelope).IsSameReferenceAs(payload);
        await Assert.That(store.ResultApplyCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies cancellation during persisted replay decode is not converted to quarantine.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ApplySyncResultAsyncCanceledPersistedReplayDecodeDoesNotQuarantine()
    {
        using CancellationTokenSource source = new();
        var snapshot = await CreateSnapshotWithAuthoritativeAsync(FirstReadingValue, await CreateStatePayloadAsync(InitialSum));
        var first = CreatePendingOperation(FirstClientSequence, FirstReadingValue);
        var second = CreatePendingOperation(QuarantineSecondClientSequence, SecondReadingValue);
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [first, second], RecoveredNextSequence) };
        var serializer = new ScriptedPayloadSerializer { CancelDuringInputDeserialization = source };
        var committer = await CreateRecoveredCommitterAsync(store, serializer);
        var batchId = Guid.NewGuid();

        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            () => committer.ApplySyncResultAsync(
                new(batchId, [first, second]),
                new(
                    batchId,
                    [
                        new(first.OperationId, OperationResultKind.Accepted, null, null),
                        new(second.OperationId, OperationResultKind.Rejected, ResultRejectedReasonCode, null),
                    ],
                    null,
                    null),
                source.Token).AsTask());

        await Assert.That(exception?.CancellationToken).IsEqualTo(source.Token);
        await Assert.That(store.QuarantineCallCount).IsEqualTo(0);
        await Assert.That(store.ResultApplyCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies cancellation during projection state decode is not converted to quarantine.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitAsyncCanceledProjectionStateDecodeDoesNotQuarantine()
    {
        using CancellationTokenSource source = new();
        var snapshot = await CreateSnapshotAsync(new(FirstReadingValue));
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [], RecoveredNextSequence) };
        var serializer = new ScriptedPayloadSerializer();
        var committer = await CreateRecoveredCommitterAsync(store, serializer);
        serializer.CancelAfterStateDeserialization = source;

        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            () => committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, source.Token).AsTask());

        await Assert.That(exception?.CancellationToken).IsEqualTo(source.Token);
        await Assert.That(store.QuarantineCallCount).IsEqualTo(0);
        await Assert.That(store.CommitCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies freshly serialized projection state schema failures do not fabricate quarantine markers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitAsyncFreshProjectionStateSchemaFailureDoesNotQuarantine()
    {
        var store = new ScriptedLocalStore();
        var serializer = new ScriptedPayloadSerializer();
        var committer = await CreateRecoveredCommitterAsync(store, serializer);
        serializer.RejectAllStatePayloads = true;

        _ = await Assert.ThrowsExactlyAsync<PayloadSchemaException>(
            () => committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(store.QuarantineCallCount).IsEqualTo(0);
        await Assert.That(store.CommitCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies recovered schema failure reasons map to stable quarantine reasons.</summary>
    /// <param name="schemaReason">The schema failure reason.</param>
    /// <param name="quarantineReason">The expected quarantine reason.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    [Arguments(PayloadSchemaFailureReason.MissingUpcaster, LocalPayloadQuarantineReason.UpcastFailed)]
    [Arguments(PayloadSchemaFailureReason.AmbiguousUpcaster, LocalPayloadQuarantineReason.UpcastFailed)]
    [Arguments(PayloadSchemaFailureReason.DowncastNotSupported, LocalPayloadQuarantineReason.UpcastFailed)]
    [Arguments(PayloadSchemaFailureReason.UpcasterContractMismatch, LocalPayloadQuarantineReason.UpcastFailed)]
    [Arguments(PayloadSchemaFailureReason.UpcasterFailed, LocalPayloadQuarantineReason.UpcastFailed)]
    [Arguments(PayloadSchemaFailureReason.InvalidSchemaVersion, LocalPayloadQuarantineReason.SchemaRejected)]
    public async Task RecoverAsyncSnapshotPayloadSchemaFailureMapsQuarantineReason(
        PayloadSchemaFailureReason schemaReason,
        LocalPayloadQuarantineReason quarantineReason)
    {
        var snapshot = new LocalSnapshot(
            Stream,
            SnapshotFormatVersion,
            RecoveryCursor,
            new PayloadEnvelope(StateContract, StateSchemaVersion, TestContentType, "42"u8.ToArray(), CorruptPayloadHash),
            Revision: 1,
            CommittedUtc);
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [], CrossStreamNextSequence) };
        var serializer = new ScriptedPayloadSerializer { RejectStateHash = true, RejectedStateReason = schemaReason };
        var committer = CreateCommitterWithStore(store, serializer);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());

        await Assert.That(store.QuarantineRequest?.Reason).IsEqualTo(quarantineReason);
        await Assert.That(store.QuarantineRequest?.ReasonCode).IsEqualTo(schemaReason.ToString());
    }

    /// <summary>Verifies recovered streams with existing quarantine markers fail closed before decoding.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncExistingQuarantineMarkerPoisonsStreamBeforeDecode()
    {
        var marker = CreateQuarantineRecord();
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(null, [], 1) with { Quarantine = marker } };
        var committer = CreateCommitter(store, new());

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());
        var poisoned = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("quarantined");
        await Assert.That(poisoned?.Message).Contains(PoisonedMessage);
        await Assert.That(store.QuarantineCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies quarantine persistence failure poisons the committer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncQuarantineWriteFailurePoisonsStream()
    {
        var snapshot = new LocalSnapshot(
            Stream,
            SnapshotFormatVersion,
            RecoveryCursor,
            new PayloadEnvelope(StateContract, StateSchemaVersion, TestContentType, "42"u8.ToArray(), CorruptPayloadHash),
            Revision: 1,
            CommittedUtc);
        var recovery = CreateRecoveredStream(snapshot, [], CrossStreamNextSequence);
        var failure = new InvalidOperationException("capacity exhausted");
        var store = new ScriptedLocalStore { Recovery = recovery, QuarantineException = failure };
        var serializer = new ScriptedPayloadSerializer { RejectStateHash = true };
        var committer = CreateCommitter(store, serializer);

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());
        var poisoned = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("could not be persisted");
        await Assert.That(poisoned?.Message).Contains(PoisonedMessage);
        await Assert.That(store.QuarantineCallCount).IsEqualTo(1);
    }

    /// <summary>Verifies stores without quarantine support poison the committer on persisted decode failure.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncMissingQuarantineStorePoisonsStream()
    {
        var snapshot = new LocalSnapshot(
            Stream,
            SnapshotFormatVersion,
            RecoveryCursor,
            new PayloadEnvelope(StateContract, StateSchemaVersion, TestContentType, "42"u8.ToArray(), CorruptPayloadHash),
            Revision: 1,
            CommittedUtc);
        var store = new RecoveryOnlyLocalStore(CreateRecoveredStream(snapshot, [], CrossStreamNextSequence));
        var serializer = new ScriptedPayloadSerializer { RejectStateHash = true };
        var committer = CreateCommitterWithStore(store, serializer);

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());
        var poisoned = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("does not support payload quarantine");
        await Assert.That(poisoned?.Message).Contains(PoisonedMessage);
    }

    /// <summary>Verifies cancellation during quarantine persistence remains cancellation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncQuarantineWriteCancellationDoesNotWrapCancellation()
    {
        using CancellationTokenSource source = new();
        var snapshot = new LocalSnapshot(
            Stream,
            SnapshotFormatVersion,
            RecoveryCursor,
            new PayloadEnvelope(StateContract, StateSchemaVersion, TestContentType, "42"u8.ToArray(), CorruptPayloadHash),
            Revision: 1,
            CommittedUtc);
        var store = new ScriptedLocalStore
        {
            Recovery = CreateRecoveredStream(snapshot, [], CrossStreamNextSequence),
            CancelBeforeQuarantineException = source,
            QuarantineException = new OperationCanceledException(source.Token),
        };
        var serializer = new ScriptedPayloadSerializer { RejectStateHash = true };
        var committer = CreateCommitter(store, serializer);

        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            () => committer.RecoverAsync(source.Token).AsTask());
        var poisoned = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = RetryCommitValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(exception?.CancellationToken).IsEqualTo(source.Token);
        await Assert.That(poisoned?.Message).Contains(PoisonedMessage);
        await Assert.That(store.QuarantineCallCount).IsEqualTo(1);
    }

    /// <summary>Creates a state payload for quarantine replay tests.</summary>
    /// <param name="sum">The state sum.</param>
    /// <returns>The state payload.</returns>
    private static async ValueTask<PayloadEnvelope> CreateStatePayloadAsync(int sum)
    {
        var serializer = new ScriptedPayloadSerializer();
        return await serializer.SerializeAsync(StateContract, StateSchemaVersion, new ReadingState(sum), CancellationToken.None);
    }

    /// <summary>Creates a snapshot with explicit authoritative state.</summary>
    /// <param name="sum">The visible optimistic sum.</param>
    /// <param name="authoritative">The authoritative payload.</param>
    /// <returns>The local snapshot.</returns>
    private static async ValueTask<LocalSnapshot> CreateSnapshotWithAuthoritativeAsync(
        int sum,
        PayloadEnvelope authoritative)
    {
        var snapshot = await CreateSnapshotAsync(new(sum));
        return snapshot with { AuthoritativeState = authoritative };
    }

    /// <summary>Creates a representative quarantine marker.</summary>
    /// <returns>The quarantine marker.</returns>
    private static LocalPayloadQuarantineRecord CreateQuarantineRecord() =>
        new(
            Guid.NewGuid(),
            Stream,
            Subscription,
            null,
            null,
            LocalPayloadQuarantineSource.Snapshot,
            LocalPayloadQuarantineReason.PayloadHashMismatch,
            "PayloadHashMismatch",
            RecoveryCursor,
            new(null, null, null, 0, null, ReadOnlyMemory<byte>.Empty),
            CommittedUtc);

    /// <summary>A local store test double that deliberately lacks quarantine support.</summary>
    /// <param name="recovery">The recovered stream returned to the committer.</param>
    private sealed class RecoveryOnlyLocalStore(RecoveredStream recovery) : ILocalStoreAdapter
    {
        /// <inheritdoc/>
        public LocalStoreCapabilities Capabilities { get; } = LocalStoreCapabilities.AtomicLocalCommit;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SubscriptionId> GetOrCreateSubscriptionIdAsync(
            StreamId streamId,
            SubscriptionId? preferredId,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask InitializeAsync(LocalStoreInitialization initialization, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RecoveredStream> RecoverStreamAsync(
            StreamId streamId,
            SubscriptionId subscriptionId,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(recovery);

        /// <inheritdoc/>
        public ValueTask<LocalCommitResult> CommitLocalOperationAsync(
            SyncOperation operation,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public async IAsyncEnumerable<LeasedOperationBatch> LeasePendingOperationsAsync(
            OutboxLeaseRequest request,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            await Task.CompletedTask.ConfigureAwait(false);
            yield break;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ApplySyncResultAsync(Guid leaseId, RemoteSyncResult result, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<IReadOnlyList<LocalSnapshot>> ApplySyncResultAsync(
            Guid leaseId,
            RemoteSyncResult result,
            IReadOnlyList<SnapshotMutation> snapshotMutations,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask<LocalSnapshot> DeadLetterOperationAsync(
            Guid leaseId,
            OperationId operationId,
            string reasonCode,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask<IReadOnlyList<Guid>> GetUnappliedEventIdsAsync(
            StreamId streamId,
            IReadOnlyList<Guid> eventIds,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(
            RemoteEventBatch batch,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RetryState?> GetRetryStateAsync(OperationId operationId, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        public ValueTask<AttemptBarrierResult> TryBeginRemoteAttemptAsync(
            Guid leaseId,
            OperationId operationId,
            int nextAttempt,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask SaveRetryStateAsync(OperationId operationId, RetryState retryState, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask RenewLeaseAsync(Guid leaseId, TimeSpan extension, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask ReleaseLeaseAsync(Guid leaseId, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        /// <inheritdoc/>
        public ValueTask<CompactionResult> CompactAsync(CompactionRequest request, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
