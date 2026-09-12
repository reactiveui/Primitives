// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Recovery tests for <see cref="LocalStreamCommitter{TState,TInput}"/>.</summary>
public sealed partial class LocalStreamCommitterTests
{
    /// <summary>The recovered snapshot sum.</summary>
    private const int RecoveredSnapshotSum = 42;

    /// <summary>The recovered snapshot revision.</summary>
    private const long RecoveredSnapshotRevision = 6;

    /// <summary>The recovered next sequence.</summary>
    private const long RecoveredNextSequence = 7;

    /// <summary>The two pending operations retained across a committer restart.</summary>
    private const int RestartedPendingOperationCount = 2;

    /// <summary>The pending operation value.</summary>
    private const int PendingOperationValue = 100;

    /// <summary>The cross-stream next sequence.</summary>
    private const long CrossStreamNextSequence = 2;

    /// <summary>The first client sequence.</summary>
    private const long FirstClientSequence = 1;

    /// <summary>The initial snapshot revision.</summary>
    private const long InitialSnapshotRevision = 0;

    /// <summary>The nonpositive recovered sequence.</summary>
    private const long NonpositiveRecoveredSequence = 0;

    /// <summary>The negative snapshot revision.</summary>
    private const long NegativeSnapshotRevision = -1;

    /// <summary>The first delivery attempt count.</summary>
    private const int FirstAttemptCount = 1;

    /// <summary>The unsupported snapshot format version.</summary>
    private const int UnsupportedSnapshotFormatVersion = SnapshotFormatVersion + 1;

    /// <summary>The mismatched subscription identifier.</summary>
    private static readonly SubscriptionId MismatchedSubscription = new(new Guid("94dd4758-9641-4644-80bd-bf81650ed72d"));

    /// <summary>Verifies a new committer resumes the stored optimistic snapshot and advances its sequence once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CommitAsyncRestartPreservesSnapshotAndPendingSequence()
    {
        var store = new ScriptedLocalStore();
        var first = await CreateRecoveredCommitterAsync(store, operationIdSource: GuidOperationIdSource.Instance);
        var firstResult = await first.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);
        var restarted = await CreateRecoveredCommitterAsync(store, operationIdSource: GuidOperationIdSource.Instance);

        await Assert.That(restarted.Current.State.Sum).IsEqualTo(FirstReadingValue);
        await Assert.That(restarted.Current.SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(restarted.Current.NextClientSequence).IsEqualTo(firstResult.Receipt.ClientSequence + 1);

        var second = await restarted.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None);

        await Assert.That(second.State.State.Sum).IsEqualTo(FirstReadingValue + SecondReadingValue);
        await Assert.That(second.Receipt.OperationId).IsNotEqualTo(firstResult.Receipt.OperationId);
        await Assert.That(second.Receipt.ClientSequence).IsEqualTo(firstResult.Receipt.ClientSequence + 1);
        await Assert.That(second.State.Revision).IsEqualTo(firstResult.State.Revision + 1);
        await Assert.That(store.Recovery.PendingOperations.Count).IsEqualTo(RestartedPendingOperationCount);
    }

    /// <summary>Verifies a stale writer cannot overwrite another committer's durable snapshot.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CommitAsyncStaleWriterRecoversBeforeRetry()
    {
        var store = new ScriptedLocalStore();
        var first = await CreateRecoveredCommitterAsync(store, operationIdSource: GuidOperationIdSource.Instance);
        var stale = await CreateRecoveredCommitterAsync(store, operationIdSource: GuidOperationIdSource.Instance);
        _ = await first.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => stale.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(stale.Current.State.Sum).IsEqualTo(InitialSum);
        await Assert.That(store.Recovery.PendingOperations.Count).IsEqualTo(1);

        _ = await stale.RecoverAsync(CancellationToken.None);
        var result = await stale.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None);

        await Assert.That(result.State.State.Sum).IsEqualTo(FirstReadingValue + SecondReadingValue);
        await Assert.That(store.Recovery.PendingOperations.Count).IsEqualTo(RestartedPendingOperationCount);
    }

    /// <summary>Verifies valid snapshot recovery uses the store state without replaying pending operations.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncRestoresValidSnapshotWithoutReplayingPendingOperations()
    {
        var snapshot = await CreateSnapshotAsync(new(RecoveredSnapshotSum));
        var pending = CreatePendingOperation(RecoveredSnapshotRevision, PendingOperationValue);
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [pending], RecoveredNextSequence) };
        var committer = CreateCommitter(store);

        var state = await committer.RecoverAsync(CancellationToken.None);

        await Assert.That(state.SubscriptionId).IsEqualTo(Subscription);
        await Assert.That(state.State.Sum).IsEqualTo(RecoveredSnapshotSum);
        await Assert.That(state.Revision).IsEqualTo(RecoveredSnapshotRevision);
        await Assert.That(state.NextClientSequence).IsEqualTo(RecoveredNextSequence);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(RecoveredSnapshotSum);
    }

    /// <summary>Verifies no snapshot is accepted only for a pristine recovered stream.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncAcceptsMissingSnapshotOnlyForPristineStream()
    {
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(null, [], FirstClientSequence) };
        var committer = CreateCommitter(store);

        var state = await committer.RecoverAsync(CancellationToken.None);

        await Assert.That(state.State.Sum).IsEqualTo(InitialSum);
        await Assert.That(state.Revision).IsEqualTo(InitialSnapshotRevision);
        await Assert.That(state.NextClientSequence).IsEqualTo(FirstClientSequence);
    }

    /// <summary>Verifies no snapshot with pending operations fails closed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncMissingSnapshotWithPendingOperationsFailsClosed()
    {
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(null, [CreatePendingOperation(FirstClientSequence, FailedCommitValue)], CrossStreamNextSequence) };
        var committer = CreateCommitter(store);

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("snapshot");
    }

    /// <summary>Verifies a cross-stream snapshot fails closed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncCrossStreamSnapshotFailsClosed()
    {
        var snapshot = await CreateSnapshotAsync(new(1), new StreamId("sensor/humidity"));
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [], CrossStreamNextSequence) };
        var committer = CreateCommitter(store);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());
    }

    /// <summary>Verifies an input-contract snapshot fails closed instead of being decoded as state.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncSnapshotWithInputContractFailsClosed()
    {
        var payload = "1"u8.ToArray();
        var snapshot = new LocalSnapshot(
            Stream,
            SnapshotFormatVersion,
            RecoveryCursor,
            new PayloadEnvelope(InputContract, InputSchemaVersion, TestContentType, payload, "hash-1"),
            Revision: 1,
            CommittedUtc);
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [], CrossStreamNextSequence) };
        var committer = CreateCommitter(store);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());
    }

    /// <summary>Verifies a corrupt snapshot payload fails closed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncCorruptSnapshotFailsClosed()
    {
        var snapshot = new LocalSnapshot(
            Stream,
            SnapshotFormatVersion,
            RecoveryCursor,
            new PayloadEnvelope(StateContract, StateSchemaVersion, TestContentType, "bad"u8.ToArray(), "hash-bad"),
            Revision: 1,
            CommittedUtc);
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [], CrossStreamNextSequence) };
        var committer = CreateCommitter(store);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());
    }

    /// <summary>Verifies a null recovery result fails closed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncNullStoreResultFailsClosed()
    {
        var store = new ScriptedLocalStore { Recovery = null! };
        var committer = CreateCommitter(store);

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("no stream state");
    }

    /// <summary>Verifies a recovered subscription mismatch fails closed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncSubscriptionMismatchFailsClosed()
    {
        var snapshot = await CreateSnapshotAsync(new(RecoveredSnapshotSum));
        var store = new ScriptedLocalStore { Recovery = new(MismatchedSubscription, RecoveryCursor, snapshot, pendingOperations: [], deadLetters: [], RecoveredNextSequence) };
        var committer = CreateCommitter(store);

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("subscription");
    }

    /// <summary>Verifies a snapshot missing its state payload fails clearly before it can become current.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncNullSnapshotStateFailsClosed()
    {
        var snapshot = new LocalSnapshot(
            Stream,
            SnapshotFormatVersion,
            RecoveryCursor,
            State: null!,
            RecoveredSnapshotRevision,
            CommittedUtc);
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [], RecoveredNextSequence) };
        var committer = CreateCommitter(store);

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("payload");
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
    }

    /// <summary>Verifies an unsupported snapshot format fails closed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncUnsupportedSnapshotFormatFailsClosed()
    {
        var snapshot = await CreateSnapshotAsync(new(RecoveredSnapshotSum), formatVersion: UnsupportedSnapshotFormatVersion);
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [], RecoveredNextSequence) };
        var committer = CreateCommitter(store);

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("format");
    }

    /// <summary>Verifies cancellation during recovered snapshot decode is preserved.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncCancellationDuringSnapshotDecodePreservesCancellation()
    {
        using CancellationTokenSource source = new();
        var snapshot = await CreateSnapshotAsync(new(RecoveredSnapshotSum));
        var serializer = new ScriptedPayloadSerializer { CancelAfterStateDeserialization = source };
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [], RecoveredNextSequence) };
        var committer = CreateCommitter(store, serializer);

        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            () => committer.RecoverAsync(source.Token).AsTask());

        await Assert.That(exception?.CancellationToken).IsEqualTo(source.Token);
    }

    /// <summary>Verifies recovered snapshots that decode to the wrong type fail closed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncWrongDecodedStateTypeFailsClosed()
    {
        var snapshot = await CreateSnapshotAsync(new(RecoveredSnapshotSum));
        var serializer = new ScriptedPayloadSerializer { DeserializeStateAsInput = true };
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [], RecoveredNextSequence) };
        var committer = CreateCommitter(store, serializer);

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("wrong state type");
    }

    /// <summary>Verifies a failed recovery after success blocks stale commits until a later successful recovery.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncFailureAfterSuccessRequiresSuccessfulRetryBeforeCommit()
    {
        var recoveredSnapshot = await CreateSnapshotAsync(new(RecoveredSnapshotSum));
        var corruptSnapshot = new LocalSnapshot(
            Stream,
            SnapshotFormatVersion,
            RecoveryCursor,
            State: null!,
            RecoveredSnapshotRevision,
            CommittedUtc);
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(recoveredSnapshot, [], RecoveredNextSequence) };
        var committer = CreateCommitter(store);
        _ = await committer.RecoverAsync(CancellationToken.None);
        store.Recovery = CreateRecoveredStream(corruptSnapshot, [], RecoveredNextSequence);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());
        var commitException = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(commitException?.Message).Contains("RecoverAsync");
        await Assert.That(store.CommitCallCount).IsEqualTo(InitialCommitCallCount);

        store.Recovery = CreateRecoveredStream(recoveredSnapshot, [], RecoveredNextSequence);
        _ = await committer.RecoverAsync(CancellationToken.None);
        var result = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);

        await Assert.That(result.State.State.Sum).IsEqualTo(RecoveredSnapshotSum + FirstReadingValue);
    }

    /// <summary>Verifies a snapshot with a nonpositive next sequence fails closed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncNonpositiveNextSequenceFailsClosed()
    {
        var snapshot = await CreateSnapshotAsync(new(RecoveredSnapshotSum));
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [], NonpositiveRecoveredSequence) };
        var committer = CreateCommitter(store);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());
    }

    /// <summary>Verifies a negative snapshot revision fails closed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncNegativeSnapshotRevisionFailsClosed()
    {
        var snapshot = await CreateSnapshotAsync(new(RecoveredSnapshotSum), revision: NegativeSnapshotRevision);
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [], RecoveredNextSequence) };
        var committer = CreateCommitter(store);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());
    }

    /// <summary>Verifies a recovered cursor must match the snapshot cursor.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncCursorMismatchFailsClosed()
    {
        var snapshot = await CreateSnapshotAsync(new(RecoveredSnapshotSum));
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [], RecoveredNextSequence, serverCursor: MismatchedCursor) };
        var committer = CreateCommitter(store);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());
    }

    /// <summary>Verifies older positive state schema versions are left to the serializer upcast path.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncOlderStateSchemaVersionUsesSerializer()
    {
        var snapshot = await CreateSnapshotAsync(new(RecoveredSnapshotSum), stateSchemaVersion: OlderStateSchemaVersion);
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, [], RecoveredNextSequence) };
        var committer = CreateCommitter(store);

        var state = await committer.RecoverAsync(CancellationToken.None);

        await Assert.That(state.State.Sum).IsEqualTo(RecoveredSnapshotSum);
    }

    /// <summary>Verifies no snapshot with dead letters is not pristine.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncMissingSnapshotWithDeadLettersFailsClosed()
    {
        var deadLetter = new DeadLetterRecord(CreatePendingOperation(FirstClientSequence, PendingOperationValue), DeadLetterReason, FirstAttemptCount, CommittedUtc);
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(null, [], FirstClientSequence, [deadLetter]) };
        var committer = CreateCommitter(store);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.RecoverAsync(CancellationToken.None).AsTask());
    }

    /// <summary>Verifies overlapping recovery and commit calls are rejected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RecoverAsyncRejectsOverlapImmediately()
    {
        TaskCompletionSource enteredRecovery = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseRecovery = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new ScriptedLocalStore { BeforeRecoveryAsync = PauseAfterSignal(enteredRecovery, releaseRecovery) };
        var committer = CreateCommitter(store);
        var first = committer.RecoverAsync(CancellationToken.None).AsTask();
        try
        {
            await enteredRecovery.Task.WaitAsync(TimeSpan.FromSeconds(StoreStartWaitSeconds));
            var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
                () => committer.CommitAsync(new MutableReading { Value = 1 }, OperationPolicy.Default, CancellationToken.None).AsTask());

            await Assert.That(exception?.Message).Contains("already in progress");
            await Assert.That(first.IsCompleted).IsFalse();
        }
        finally
        {
            _ = releaseRecovery.TrySetResult();
            await first;
        }
    }

    /// <summary>Creates a pending operation for recovery tests.</summary>
    /// <param name="sequence">The client sequence.</param>
    /// <param name="value">The operation value.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation CreatePendingOperation(long sequence, int value)
    {
        var payload = System.Text.Encoding.UTF8.GetBytes(value.ToString(CultureInfo.InvariantCulture));
        return new()
        {
            OperationId = OperationId.New(),
            StreamId = Stream,
            ClientSequence = sequence,
            TimestampUtc = CommittedUtc,
            Type = SyncOperationType.Update,
            Payload = new(InputContract, InputSchemaVersion, "test/json", payload, $"hash-{value}"),
            Policy = OperationPolicy.Default,
            Metadata = new Dictionary<string, string>(),
        };
    }
}
