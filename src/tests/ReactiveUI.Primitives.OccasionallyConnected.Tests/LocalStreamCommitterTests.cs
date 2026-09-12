// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.ObjectModel;
using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="LocalStreamCommitter{TState,TInput}"/>.</summary>
public sealed partial class LocalStreamCommitterTests
{
    /// <summary>The input contract identifier.</summary>
    private const string InputContract = "reading-input";

    /// <summary>The state contract identifier.</summary>
    private const string StateContract = "reading-state";

    /// <summary>The input schema version.</summary>
    private const int InputSchemaVersion = 2;

    /// <summary>The state schema version.</summary>
    private const int StateSchemaVersion = 4;

    /// <summary>The snapshot format version.</summary>
    private const int SnapshotFormatVersion = 3;

    /// <summary>The fake serializer content type.</summary>
    private const string TestContentType = "test/json";

    /// <summary>The initial state sum.</summary>
    private const int InitialSum = 0;

    /// <summary>The first reading value.</summary>
    private const int FirstReadingValue = 21;

    /// <summary>The caller mutation value.</summary>
    private const int MutatedReadingValue = 999;

    /// <summary>The failed commit reading value.</summary>
    private const int FailedCommitValue = 5;

    /// <summary>The retry commit reading value.</summary>
    private const int RetryCommitValue = 7;

    /// <summary>The canceled commit reading value.</summary>
    private const int CanceledCommitValue = 3;

    /// <summary>The post-commit cancellation reading value.</summary>
    private const int PostCommitCancellationValue = 8;

    /// <summary>The second reading value.</summary>
    private const int SecondReadingValue = 2;

    /// <summary>The initial store commit call count.</summary>
    private const int InitialCommitCallCount = 0;

    /// <summary>The maximum wait for a fake store call to start.</summary>
    private const int StoreStartWaitSeconds = 5;

    /// <summary>The older state schema version used for upcast recovery.</summary>
    private const int OlderStateSchemaVersion = StateSchemaVersion - 1;

    /// <summary>The mismatched recovered cursor.</summary>
    private const string MismatchedCursor = "cursor-mismatch";

    /// <summary>The recovered snapshot cursor.</summary>
    private const string RecoveryCursor = "cursor-9";

    /// <summary>The dead letter reason code.</summary>
    private const string DeadLetterReason = "OC.Test";

    /// <summary>The invalid high minimum priority.</summary>
    private const int InvalidMinimumPriority = 10;

    /// <summary>The invalid low maximum priority.</summary>
    private const int InvalidMaximumPriority = -10;

    /// <summary>The committed timestamp used by store receipts.</summary>
    private static readonly DateTimeOffset CommittedUtc = new(2026, 9, 11, 12, 30, 0, TimeSpan.Zero);

    /// <summary>The fixed stream identifier used by tests.</summary>
    private static readonly StreamId Stream = new("sensor/temperature");

    /// <summary>The stable subscription identifier used by tests.</summary>
    private static readonly SubscriptionId Subscription = new(new Guid("04b7b012-d6fe-4c81-8d78-f3d9b6a4b741"));

    /// <summary>Verifies a durable commit persists the payload before mutating observable state.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitAsyncCommitsDecodedPayloadAndSnapshotAtomically()
    {
        var input = new MutableReading { Value = FirstReadingValue };
        var serializer = new ScriptedPayloadSerializer { MutateInputAfterSerialization = true };
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer);

        var result = await committer.CommitAsync(input, OperationPolicy.Default, CancellationToken.None);

        await Assert.That(input.Value).IsEqualTo(MutatedReadingValue);
        await Assert.That(result.Operation).IsSameReferenceAs(store.CommittedOperation);
        await Assert.That(result.Operation.ClientSequence).IsEqualTo(1);
        await Assert.That(result.Operation.Payload.ContractId).IsEqualTo(InputContract);
        await Assert.That(result.Operation.Payload.SchemaVersion).IsEqualTo(InputSchemaVersion);
        await Assert.That(result.Receipt.ClientSequence).IsEqualTo(1);
        await Assert.That(result.Receipt.State).IsEqualTo(SyncOperationState.SavedLocally);
        await Assert.That(result.State.State.Sum).IsEqualTo(FirstReadingValue);
        await Assert.That(result.State.Revision).IsEqualTo(1);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(FirstReadingValue);
        await Assert.That(store.CommittedSnapshot?.State.ContractId).IsEqualTo(StateContract);
        await Assert.That(store.CommittedSnapshot?.State.SchemaVersion).IsEqualTo(StateSchemaVersion);
        await Assert.That(store.CommittedSnapshot?.FormatVersion).IsEqualTo(SnapshotFormatVersion);
        await Assert.That(store.CommittedSnapshot?.ExpectedRevision).IsEqualTo(0);
    }

    /// <summary>Verifies the default operation identifier source produces durable operation identifiers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitAsyncUsesDefaultOperationIdSourceWhenNotConfigured()
    {
        var store = new ScriptedLocalStore();
        LocalStreamCommitter<ReadingState, MutableReading> committer = new(CreateOptions(store, new()));
        _ = await committer.RecoverAsync(CancellationToken.None);

        var result = await committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None);

        await Assert.That(result.Operation.OperationId.Value).IsNotEqualTo(Guid.Empty);
    }

    /// <summary>Verifies overlapping asynchronous entry is rejected instead of queued.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitAsyncRejectsOverlapImmediately()
    {
        TaskCompletionSource enteredStore = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseStore = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new ScriptedLocalStore { BeforeCommitAsync = PauseAfterSignal(enteredStore, releaseStore) };
        var committer = await CreateRecoveredCommitterAsync(store);
        var first = committer.CommitAsync(new MutableReading { Value = 1 }, OperationPolicy.Default, CancellationToken.None).AsTask();
        try
        {
            await enteredStore.Task.WaitAsync(TimeSpan.FromSeconds(StoreStartWaitSeconds));
            var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
                () => committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

            await Assert.That(exception?.Message).Contains("already in progress");
            await Assert.That(first.IsCompleted).IsFalse();
            await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
        }
        finally
        {
            _ = releaseStore.TrySetResult();
            await first;
        }

        await Assert.That(committer.Current.State.Sum).IsEqualTo(1);
    }

    /// <summary>Verifies pre-commit store failures leave observable state and counters unchanged.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitAsyncStoreFailureLeavesStateAndSequenceUnchanged()
    {
        var store = new ScriptedLocalStore { CommitException = new InvalidOperationException("commit failed") };
        var committer = await CreateRecoveredCommitterAsync(store);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = FailedCommitValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
        await Assert.That(committer.Current.NextClientSequence).IsEqualTo(1);

        store.CommitException = null;
        var result = await committer.CommitAsync(new MutableReading { Value = RetryCommitValue }, OperationPolicy.Default, CancellationToken.None);

        await Assert.That(result.Operation.ClientSequence).IsEqualTo(1);
        await Assert.That(result.State.State.Sum).IsEqualTo(RetryCommitValue);
    }

    /// <summary>Verifies cancellation before the store transaction preserves state.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitAsyncCancellationBeforeStoreLeavesStateUnchanged()
    {
        using CancellationTokenSource source = new();
        var serializer = new ScriptedPayloadSerializer { CancelAfterStateSerialization = source };
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, serializer);

        var exception = await Assert.ThrowsExactlyAsync<OperationCanceledException>(
            () => committer.CommitAsync(new MutableReading { Value = CanceledCommitValue }, OperationPolicy.Default, source.Token).AsTask());

        await Assert.That(exception?.CancellationToken).IsEqualTo(source.Token);
        await Assert.That(store.CommitCallCount).IsEqualTo(0);
        await Assert.That(committer.Current.State.Sum).IsEqualTo(InitialSum);
        await Assert.That(committer.Current.NextClientSequence).IsEqualTo(1);
    }

    /// <summary>Verifies cancellation after a successful durable commit does not obscure the receipt.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitAsyncCancellationAfterStoreCommitReturnsReceipt()
    {
        using CancellationTokenSource source = new();
        var store = new ScriptedLocalStore { CancelAfterSuccessfulCommit = source };
        var committer = await CreateRecoveredCommitterAsync(store);

        var result = await committer.CommitAsync(new MutableReading { Value = PostCommitCancellationValue }, OperationPolicy.Default, source.Token);

        await Assert.That(source.IsCancellationRequested).IsTrue();
        await Assert.That(result.Receipt.OperationId).IsEqualTo(result.Operation.OperationId);
        await Assert.That(result.State.State.Sum).IsEqualTo(PostCommitCancellationValue);
        await Assert.That(committer.Current.Revision).IsEqualTo(1);
    }

    /// <summary>Verifies malformed store receipts poison the committer before later use.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitAsyncMalformedReceiptPoisonsCommitter()
    {
        var store = new ScriptedLocalStore { ReceiptSequenceOffset = 1 };
        var committer = await CreateRecoveredCommitterAsync(store);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = 1 }, OperationPolicy.Default, CancellationToken.None).AsTask());

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("poisoned");
    }

    /// <summary>Verifies a null store receipt poisons the committer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitAsyncNullReceiptPoisonsCommitter()
    {
        var store = new ScriptedLocalStore { ReturnNullCommitResult = true };
        var committer = await CreateRecoveredCommitterAsync(store);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = SecondReadingValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("poisoned");
    }

    /// <summary>Verifies default operation identifiers are rejected before persistence.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitAsyncRejectsDefaultOperationIdBeforeStore()
    {
        var store = new ScriptedLocalStore();
        var committer = await CreateRecoveredCommitterAsync(store, operationIdSource: new DefaultOperationIdSource());

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(store.CommitCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies an input payload that decodes to the wrong type is rejected before persistence.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitAsyncRejectsWrongDecodedInputTypeBeforeStore()
    {
        var store = new ScriptedLocalStore();
        var serializer = new ScriptedPayloadSerializer { DeserializeInputAsState = true };
        var committer = await CreateRecoveredCommitterAsync(store, serializer);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(store.CommitCallCount).IsEqualTo(InitialCommitCallCount);
    }

    /// <summary>Verifies commit requires successful recovery first.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitAsyncBeforeRecoveryFailsClosed()
    {
        var committer = CreateCommitter(new());

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("RecoverAsync");
    }

    /// <summary>Verifies sequence overflow is rejected before any store transaction.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitAsyncRejectsSequenceOverflowBeforeStore()
    {
        var snapshot = await CreateSnapshotAsync(new(InitialSum));
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, pendingOperations: [], nextClientSequence: long.MaxValue) };
        var committer = CreateCommitter(store);
        await committer.RecoverAsync(CancellationToken.None);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = 1 }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(store.CommitCallCount).IsEqualTo(0);
    }

    /// <summary>Verifies revision overflow is rejected before any store transaction.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitAsyncRejectsRevisionOverflowBeforeStore()
    {
        var snapshot = await CreateSnapshotAsync(new(InitialSum), revision: long.MaxValue);
        var store = new ScriptedLocalStore { Recovery = CreateRecoveredStream(snapshot, pendingOperations: [], nextClientSequence: RecoveredNextSequence) };
        var committer = CreateCommitter(store);
        _ = await committer.RecoverAsync(CancellationToken.None);

        _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = FirstReadingValue }, OperationPolicy.Default, CancellationToken.None).AsTask());

        await Assert.That(store.CommitCallCount).IsEqualTo(InitialCommitCallCount);
    }

    /// <summary>Verifies durable publishing rejects volatile operation policies.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CommitAsyncRejectsVolatilePolicy()
    {
        var committer = CreateCommitter(new());
        var policy = OperationPolicy.Default with { Durability = OperationDurability.Volatile };

        var exception = await Assert.ThrowsExactlyAsync<InvalidOperationException>(
            () => committer.CommitAsync(new MutableReading { Value = 1 }, policy, CancellationToken.None).AsTask());

        await Assert.That(exception?.Message).Contains("durable");
    }

    /// <summary>Verifies malformed committer options fail during construction.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsMalformedOptions()
    {
        await Assert.That(static () => CreateLocalCommitter(
            CreateOptions(new(), new()) with { StreamId = default })).ThrowsExactly<InvalidOperationException>();
        await Assert.That(static () => CreateLocalCommitter(
            CreateOptions(new(), new()) with { SubscriptionId = new(Guid.Empty) })).ThrowsExactly<InvalidOperationException>();
        await Assert.That(static () => CreateLocalCommitter(
            CreateOptions(new(), new()) with
            {
                MinimumPriority = InvalidMinimumPriority,
                MaximumPriority = InvalidMaximumPriority,
            })).ThrowsExactly<InvalidOperationException>();
        await Assert.That(static () => CreateLocalCommitter(
            CreateOptions(new(), new()) with { Contracts = null! })).ThrowsExactly<InvalidOperationException>();
        await Assert.That(static () => CreateLocalCommitter(
            CreateOptions(new(), new()) with { Dependencies = null! })).ThrowsExactly<InvalidOperationException>();
        await Assert.That(static () => CreateLocalCommitter(
            CreateOptions(new(), new()) with
            {
                Contracts = CreateContracts() with { InputContractId = string.Empty },
            })).ThrowsExactly<InvalidOperationException>();
        await Assert.That(static () => CreateLocalCommitter(
            CreateOptions(new(), new()) with
            {
                Contracts = CreateContracts() with { InputSchemaVersion = InitialSum },
            })).ThrowsExactly<InvalidOperationException>();
        await Assert.That(static () => CreateLocalCommitter(
            CreateOptions(new(), new()) with
            {
                Dependencies = CreateDependencies(new(), new(), null) with { Store = null! },
            })).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Creates a local committer.</summary>
    /// <param name="options">The committer options.</param>
    /// <returns>The local committer.</returns>
    private static LocalStreamCommitter<ReadingState, MutableReading> CreateLocalCommitter(
        LocalStreamCommitterOptions<ReadingState, MutableReading> options) =>
        new(options);

    /// <summary>Creates a configured committer.</summary>
    /// <param name="store">The fake store.</param>
    /// <param name="serializer">The serializer.</param>
    /// <param name="operationIdSource">The operation identifier source.</param>
    /// <returns>The committer.</returns>
    private static LocalStreamCommitter<ReadingState, MutableReading> CreateCommitter(
        ScriptedLocalStore store,
        ScriptedPayloadSerializer? serializer = null,
        IOperationIdSource? operationIdSource = null) =>
        new(CreateOptions(store, serializer ?? new ScriptedPayloadSerializer(), operationIdSource ?? new SequenceOperationIdSource()));

    /// <summary>Creates committer options.</summary>
    /// <param name="store">The fake store.</param>
    /// <param name="serializer">The serializer.</param>
    /// <param name="operationIdSource">The operation identifier source.</param>
    /// <returns>The committer options.</returns>
    private static LocalStreamCommitterOptions<ReadingState, MutableReading> CreateOptions(
        ScriptedLocalStore store,
        ScriptedPayloadSerializer serializer,
        IOperationIdSource? operationIdSource = null) =>
        new() { StreamId = Stream, SubscriptionId = Subscription, Contracts = CreateContracts(), Dependencies = CreateDependencies(store, serializer, operationIdSource) };

    /// <summary>Creates committer contracts.</summary>
    /// <returns>The committer contracts.</returns>
    private static LocalStreamCommitterContracts CreateContracts() =>
        new()
        {
            InputContractId = InputContract,
            InputSchemaVersion = InputSchemaVersion,
            StateContractId = StateContract,
            StateSchemaVersion = StateSchemaVersion,
            SnapshotFormatVersion = SnapshotFormatVersion,
        };

    /// <summary>Creates committer dependencies.</summary>
    /// <param name="store">The fake store.</param>
    /// <param name="serializer">The serializer.</param>
    /// <param name="operationIdSource">The operation identifier source.</param>
    /// <returns>The committer dependencies.</returns>
    private static LocalStreamCommitterDependencies<ReadingState, MutableReading> CreateDependencies(
        ScriptedLocalStore store,
        ScriptedPayloadSerializer serializer,
        IOperationIdSource? operationIdSource)
    {
        var dependencies = new LocalStreamCommitterDependencies<ReadingState, MutableReading>
        {
            Store = store,
            Serializer = serializer,
            Projection = new SumProjection(),
            TimeProvider = new FixedTimeProvider(CommittedUtc),
        };

        return operationIdSource is null
            ? dependencies
            : dependencies with { OperationIdSource = operationIdSource };
    }

    /// <summary>Creates and recovers a configured committer.</summary>
    /// <param name="store">The fake store.</param>
    /// <param name="serializer">The serializer.</param>
    /// <param name="operationIdSource">The operation identifier source.</param>
    /// <returns>The recovered committer.</returns>
    private static async ValueTask<LocalStreamCommitter<ReadingState, MutableReading>> CreateRecoveredCommitterAsync(
        ScriptedLocalStore store,
        ScriptedPayloadSerializer? serializer = null,
        IOperationIdSource? operationIdSource = null)
    {
        var committer = CreateCommitter(store, serializer, operationIdSource);
        _ = await committer.RecoverAsync(CancellationToken.None);
        return committer;
    }

    /// <summary>Creates a recovered stream for tests.</summary>
    /// <param name="snapshot">The recovered snapshot.</param>
    /// <param name="pendingOperations">The pending operations.</param>
    /// <param name="nextClientSequence">The next sequence.</param>
    /// <param name="deadLetters">The dead-letter records.</param>
    /// <param name="serverCursor">The recovered server cursor.</param>
    /// <returns>The recovered stream.</returns>
    private static RecoveredStream CreateRecoveredStream(
        LocalSnapshot? snapshot,
        IReadOnlyList<SyncOperation> pendingOperations,
        long nextClientSequence,
        IReadOnlyList<DeadLetterRecord>? deadLetters = null,
        string? serverCursor = null) =>
        new(
            Subscription,
            serverCursor ?? (snapshot is null ? null : RecoveryCursor),
            snapshot,
            pendingOperations,
            deadLetters ?? [],
            nextClientSequence);

    /// <summary>Creates a local snapshot payload.</summary>
    /// <param name="state">The decoded state.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="formatVersion">The snapshot format.</param>
    /// <param name="revision">The snapshot revision.</param>
    /// <param name="stateSchemaVersion">The state schema version.</param>
    /// <returns>The local snapshot.</returns>
    private static async ValueTask<LocalSnapshot> CreateSnapshotAsync(
        ReadingState state,
        StreamId? streamId = null,
        int formatVersion = SnapshotFormatVersion,
        long revision = RecoveredSnapshotRevision,
        int stateSchemaVersion = StateSchemaVersion)
    {
        var serializer = new ScriptedPayloadSerializer();
        var envelope = await serializer.SerializeAsync(StateContract, stateSchemaVersion, state, CancellationToken.None);
        return new(streamId ?? Stream, formatVersion, RecoveryCursor, envelope, revision, CommittedUtc);
    }

    /// <summary>Creates a fake asynchronous pause callback.</summary>
    /// <param name="entered">The completion signaled before pausing.</param>
    /// <param name="release">The completion that releases the paused operation.</param>
    /// <returns>The pause callback.</returns>
    private static Func<Task> PauseAfterSignal(TaskCompletionSource entered, TaskCompletionSource release) =>
        () =>
        {
            _ = entered.TrySetResult();
            return release.Task;
        };

    /// <summary>Represents mutable caller input.</summary>
    private sealed class MutableReading
    {
        /// <summary>Gets or sets the reading value.</summary>
        public int Value { get; set; }
    }

    /// <summary>Projects readings into a running sum.</summary>
    private sealed class SumProjection : ILocalProjection<ReadingState, MutableReading>
    {
        /// <inheritdoc/>
        public ReadingState InitialState { get; } = new(InitialSum);

        /// <inheritdoc/>
        public ReadingState ApplyLocal(ReadingState state, MutableReading input, SyncOperation operation) =>
            new(state.Sum + input.Value);

        /// <inheritdoc/>
        public ReadingState ApplyRemote(ReadingState state, MutableReading input, RemoteEvent remoteEvent) =>
            new(state.Sum + input.Value);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ReadingState Reconcile(ReadingState state, ConflictResolutionResult result) => state;
    }

    /// <summary>Represents local state.</summary>
    /// <param name="sum">The sum of committed readings.</param>
    private sealed class ReadingState(int sum)
    {
        /// <summary>Gets the sum of committed readings.</summary>
        public int Sum { get; } = sum;
    }

    /// <summary>Produces deterministic operation identifiers.</summary>
    private sealed class SequenceOperationIdSource : IOperationIdSource
    {
        /// <summary>The next operation identifier value.</summary>
        private int _next = 1;

        /// <inheritdoc/>
        public OperationId New()
        {
            var current = _next;
            _next++;
            return new(new Guid(current, 0, 0, [0, 0, 0, 0, 0, 0, 0, 1]));
        }
    }

    /// <summary>Produces the default operation identifier.</summary>
    private sealed class DefaultOperationIdSource : IOperationIdSource
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public OperationId New() => default;
    }

    /// <summary>A fixed time provider.</summary>
    /// <param name="utcNow">The fixed timestamp.</param>
    private sealed class FixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow() => utcNow;
    }

    /// <summary>A scripted payload serializer for transaction tests.</summary>
    private sealed class ScriptedPayloadSerializer : IPayloadSerializer
    {
        /// <inheritdoc/>
        public string ContentType => TestContentType;

        /// <summary>Gets or sets a value indicating whether caller input is mutated after encoding.</summary>
        public bool MutateInputAfterSerialization { get; set; }

        /// <summary>Gets or sets the token source canceled after state serialization.</summary>
        public CancellationTokenSource? CancelAfterStateSerialization { get; set; }

        /// <summary>Gets or sets the token source canceled after state deserialization.</summary>
        public CancellationTokenSource? CancelAfterStateDeserialization { get; set; }

        /// <summary>Gets or sets a value indicating whether input deserialization returns a state.</summary>
        public bool DeserializeInputAsState { get; set; }

        /// <summary>Gets or sets a value indicating whether state deserialization returns an input.</summary>
        public bool DeserializeStateAsInput { get; set; }

        /// <summary>Gets the number of remote input payloads decoded.</summary>
        public int RemoteInputDeserializeCount { get; private set; }

        /// <inheritdoc/>
        public ValueTask<PayloadEnvelope> SerializeAsync<T>(
            string contractId,
            int schemaVersion,
            T value,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var text = value switch
            {
                MutableReading reading => reading.Value.ToString(CultureInfo.InvariantCulture),
                ReadingState state => state.Sum.ToString(CultureInfo.InvariantCulture),
                _ => throw new InvalidOperationException("Unexpected payload type."),
            };

            var payload = System.Text.Encoding.UTF8.GetBytes(text);
            var envelope = new PayloadEnvelope(contractId, schemaVersion, ContentType, payload, $"hash-{text}");
            if (MutateInputAfterSerialization && value is MutableReading mutable)
            {
                mutable.Value = MutatedReadingValue;
            }

            if (contractId == StateContract)
            {
                _ = CancelAfterStateSerialization?.CancelAsync();
            }

            return ValueTask.FromResult(envelope);
        }

        /// <inheritdoc/>
        public ValueTask<object> DeserializeAsync(PayloadEnvelope envelope, Type targetType, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var text = System.Text.Encoding.UTF8.GetString(envelope.Payload.Span);
            var value = int.Parse(text, CultureInfo.InvariantCulture);
            if (targetType == typeof(MutableReading))
            {
                if (envelope.ContractId == InputContract)
                {
                    RemoteInputDeserializeCount++;
                }

                return DeserializeInputAsState
                    ? ValueTask.FromResult<object>(new ReadingState(value))
                    : ValueTask.FromResult<object>(new MutableReading { Value = value });
            }

            if (targetType == typeof(ReadingState))
            {
                if (DeserializeStateAsInput)
                {
                    return ValueTask.FromResult<object>(new MutableReading { Value = value });
                }

                CancelAfterStateDeserialization?.Cancel();
                return ValueTask.FromResult<object>(new ReadingState(value));
            }

            throw new InvalidOperationException("Unexpected target type.");
        }
    }

    /// <summary>A scripted fake atomic store.</summary>
    private sealed class ScriptedLocalStore : ILocalStoreAdapter
    {
        /// <summary>The event identifiers recorded in the durable inbox.</summary>
        private readonly HashSet<Guid> _appliedEventIds = [];

        /// <inheritdoc/>
        public LocalStoreCapabilities Capabilities { get; } =
            LocalStoreCapabilities.AtomicLocalCommit
            | LocalStoreCapabilities.AtomicRemoteApply
            | LocalStoreCapabilities.DurableInbox
            | LocalStoreCapabilities.LeasedOutbox;

        /// <summary>Gets or sets the recovered stream returned by recovery.</summary>
        public RecoveredStream Recovery { get; set; } = CreateRecoveredStream(null, [], 1);

        /// <summary>Gets or sets the exception thrown by local commit.</summary>
        public Exception? CommitException { get; set; }

        /// <summary>Gets or sets asynchronous work to run before commit.</summary>
        public Func<Task>? BeforeCommitAsync { get; set; }

        /// <summary>Gets or sets asynchronous work before the remote transaction.</summary>
        public Func<Task>? BeforeRemoteCommitAsync { get; set; }

        /// <summary>Gets or sets a remote transaction failure before persistence.</summary>
        public Exception? RemoteCommitException { get; set; }

        /// <summary>Gets or sets a transformation simulating a malformed adapter receipt.</summary>
        public Func<RemoteApplyResult, RemoteApplyResult>? TransformRemoteReceipt { get; set; }

        /// <summary>Gets or sets asynchronous work to run before recovery.</summary>
        public Func<Task>? BeforeRecoveryAsync { get; set; }

        /// <summary>Gets or sets the token source canceled after successful commit.</summary>
        public CancellationTokenSource? CancelAfterSuccessfulCommit { get; set; }

        /// <summary>Gets or sets the token source canceled after successful remote apply.</summary>
        public CancellationTokenSource? CancelAfterSuccessfulRemoteApply { get; set; }

        /// <summary>Gets or sets the sequence offset applied to the returned receipt.</summary>
        public long ReceiptSequenceOffset { get; set; }

        /// <summary>Gets or sets the revision offset applied to the returned remote receipt.</summary>
        public long RemoteReceiptRevisionOffset { get; set; }

        /// <summary>Gets or sets a value indicating whether commit returns a null receipt.</summary>
        public bool ReturnNullCommitResult { get; set; }

        /// <summary>Gets or sets a value indicating whether remote apply returns a null receipt.</summary>
        public bool ReturnNullRemoteApplyResult { get; set; }

        /// <summary>Gets or sets the event identifiers returned by the inbox lookup.</summary>
        public IReadOnlyList<Guid>? UnappliedEventIdsOverride { get; set; }

        /// <summary>Gets or sets a value indicating whether the inbox lookup returns no result.</summary>
        public bool ReturnNullUnappliedLookupResult { get; set; }

        /// <summary>Gets the last committed operation.</summary>
        public SyncOperation? CommittedOperation { get; private set; }

        /// <summary>Gets the last committed snapshot mutation.</summary>
        public SnapshotMutation? CommittedSnapshot { get; private set; }

        /// <summary>Gets the last applied remote batch.</summary>
        public RemoteEventBatch? AppliedRemoteBatch { get; private set; }

        /// <summary>Gets the last applied remote snapshot mutation.</summary>
        public SnapshotMutation? AppliedRemoteSnapshot { get; private set; }

        /// <summary>Gets the commit call count.</summary>
        public int CommitCallCount { get; private set; }

        /// <summary>Gets the unapplied inbox lookup call count.</summary>
        public int UnappliedLookupCallCount { get; private set; }

        /// <summary>Gets the remote apply call count.</summary>
        public int RemoteApplyCallCount { get; private set; }

        /// <summary>Marks a remote event identifier as already applied.</summary>
        /// <param name="eventId">The remote event identifier.</param>
        /// <returns><see langword="true"/> when the identifier was not already present.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MarkEventApplied(Guid eventId) => _appliedEventIds.Add(eventId);

        /// <inheritdoc/>
        public ValueTask<SubscriptionId> GetOrCreateSubscriptionIdAsync(
            StreamId streamId,
            SubscriptionId? preferredId,
            CancellationToken cancellationToken) => throw new NotSupportedException();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask InitializeAsync(LocalStoreInitialization initialization, CancellationToken cancellationToken) =>
            ValueTask.CompletedTask;

        /// <inheritdoc/>
        public async ValueTask<RecoveredStream> RecoverStreamAsync(
            StreamId streamId,
            SubscriptionId subscriptionId,
            CancellationToken cancellationToken)
        {
            if (BeforeRecoveryAsync is not null)
            {
                await BeforeRecoveryAsync().ConfigureAwait(false);
            }

            cancellationToken.ThrowIfCancellationRequested();
            return Recovery;
        }

        /// <inheritdoc/>
        public async ValueTask<LocalCommitResult> CommitLocalOperationAsync(
            SyncOperation operation,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken)
        {
            CommitCallCount++;
            if (BeforeCommitAsync is not null)
            {
                await BeforeCommitAsync().ConfigureAwait(false);
            }

            if (CommitException is not null)
            {
                throw CommitException;
            }

            cancellationToken.ThrowIfCancellationRequested();
            if (operation.ClientSequence != Recovery.NextClientSequence
                || snapshotMutation.ExpectedRevision != (Recovery.Snapshot?.Revision ?? 0))
            {
                throw new InvalidOperationException("The stream revision or client sequence is stale.");
            }

            CommittedOperation = operation;
            CommittedSnapshot = snapshotMutation;
            List<SyncOperation> pending = new(Recovery.PendingOperations) { operation };
            var snapshot = new LocalSnapshot(
                operation.StreamId,
                snapshotMutation.FormatVersion,
                Recovery.ServerCursor,
                snapshotMutation.State,
                snapshotMutation.ExpectedRevision + 1,
                CommittedUtc);
            Recovery = new(Subscription, Recovery.ServerCursor, snapshot, pending, Recovery.DeadLetters, operation.ClientSequence + 1);
            _ = CancelAfterSuccessfulCommit?.CancelAsync();
            return ReturnNullCommitResult
                ? null!
                : new(
                operation.OperationId,
                operation.ClientSequence + ReceiptSequenceOffset,
                snapshotMutation.ExpectedRevision + 1,
                CommittedUtc);
        }

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
        public ValueTask<IReadOnlyList<Guid>> GetUnappliedEventIdsAsync(
            StreamId streamId,
            IReadOnlyList<Guid> eventIds,
            CancellationToken cancellationToken)
        {
            UnappliedLookupCallCount++;
            cancellationToken.ThrowIfCancellationRequested();
            if (ReturnNullUnappliedLookupResult)
            {
                return ValueTask.FromResult<IReadOnlyList<Guid>>(null!);
            }

            if (UnappliedEventIdsOverride is not null)
            {
                return ValueTask.FromResult(UnappliedEventIdsOverride);
            }

            List<Guid> unapplied = [];
            for (var index = 0; index < eventIds.Count; index++)
            {
                var eventId = eventIds[index];
                if (!_appliedEventIds.Contains(eventId))
                {
                    unapplied.Add(eventId);
                }
            }

            return ValueTask.FromResult<IReadOnlyList<Guid>>(new ReadOnlyCollection<Guid>(unapplied));
        }

        /// <inheritdoc/>
        public async ValueTask<RemoteApplyResult> ApplyRemoteBatchAsync(
            RemoteEventBatch batch,
            SnapshotMutation snapshotMutation,
            CancellationToken cancellationToken)
        {
            RemoteApplyCallCount++;
            if (BeforeRemoteCommitAsync is not null)
            {
                await BeforeRemoteCommitAsync();
            }

            cancellationToken.ThrowIfCancellationRequested();
            if (RemoteCommitException is not null)
            {
                throw RemoteCommitException;
            }

            if (batch.StreamId != Stream
                || !string.Equals(batch.PreviousCursor, Recovery.ServerCursor, StringComparison.Ordinal)
                || snapshotMutation.ExpectedRevision != (Recovery.Snapshot?.Revision ?? 0))
            {
                throw new InvalidOperationException("The stream cursor or snapshot revision is stale.");
            }

            AppliedRemoteBatch = batch;
            AppliedRemoteSnapshot = snapshotMutation;
            for (var index = 0; index < batch.Events.Count; index++)
            {
                _ = _appliedEventIds.Add(batch.Events[index].EventId);
            }

            var snapshot = new LocalSnapshot(
                batch.StreamId,
                snapshotMutation.FormatVersion,
                batch.NextCursor,
                snapshotMutation.State,
                snapshotMutation.ExpectedRevision + 1,
                CommittedUtc);
            Recovery = new(Subscription, batch.NextCursor, snapshot, Recovery.PendingOperations, Recovery.DeadLetters, Recovery.NextClientSequence);
            _ = CancelAfterSuccessfulRemoteApply?.CancelAsync();
            return CreateRemoteReceipt(batch, snapshotMutation.ExpectedRevision);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken) =>
            ValueTask.FromResult<SyncOperationStatus?>(null);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<RetryState?> GetRetryStateAsync(OperationId operationId, CancellationToken cancellationToken) =>
            ValueTask.FromResult<RetryState?>(null);

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

        /// <summary>Creates the configurable remote receipt after persistence.</summary>
        /// <param name="batch">The persisted batch.</param>
        /// <param name="expectedRevision">The preceding revision.</param>
        /// <returns>The configured adapter receipt.</returns>
        private RemoteApplyResult CreateRemoteReceipt(RemoteEventBatch batch, long expectedRevision)
        {
            var receipt = ReturnNullRemoteApplyResult
                ? null!
                : new RemoteApplyResult(
                    batch.NextCursor,
                    batch.Events.Count,
                    DuplicateCount: 0,
                    expectedRevision + 1 + RemoteReceiptRevisionOffset);
            return TransformRemoteReceipt is null ? receipt : TransformRemoteReceipt(receipt);
        }
    }
}
