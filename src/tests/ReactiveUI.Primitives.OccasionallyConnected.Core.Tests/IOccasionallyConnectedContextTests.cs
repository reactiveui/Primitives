// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="IOccasionallyConnectedContext"/>.</summary>
public sealed class IOccasionallyConnectedContextTests
{
    /// <summary>Verifies the context contract exposes its engine, state stream, and typed stream factory.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ContractExposesEngineStatesAndTypedStreamFactory()
    {
        var context = new Context();
        IOccasionallyConnectedContext contract = context;
        var definition = CreateDefinition();

        var stream = contract.GetOrCreateStream(definition);

        await Assert.That(contract.SyncEngine).IsSameReferenceAs(context.Engine);
        await Assert.That(contract.SyncStates).IsSameReferenceAs(context.States);
        await Assert.That(stream).IsSameReferenceAs(context.Stream);
        await Assert.That(context.Definition).IsSameReferenceAs(definition);
        await Assert.That(context.GetOrCreateCalls).IsEqualTo(1);
    }

    /// <summary>Creates a valid typed stream definition for the context factory.</summary>
    /// <returns>The typed stream definition.</returns>
    private static StreamDefinition<CounterState, CounterInput> CreateDefinition() => new()
    {
        StreamId = new("counter"),
        Projection = new Projection(),
        InputContractId = "counter.input",
        StateContractId = "counter.state",
    };

    /// <summary>Represents the stream state used by the test factory.</summary>
    /// <param name="Value">The current counter value.</param>
    private readonly record struct CounterState(int Value);

    /// <summary>Represents the stream input used by the test factory.</summary>
    /// <param name="Value">The requested counter value.</param>
    private readonly record struct CounterInput(int Value);

    /// <summary>Records context operations without providing a synchronization runtime.</summary>
    private sealed class Context : IOccasionallyConnectedContext
    {
        /// <summary>Gets the engine exposed through the contract.</summary>
        public Engine Engine { get; } = new();

        /// <summary>Gets the state stream exposed through the contract.</summary>
        public EmptyObservable<SyncState> States { get; } = new();

        /// <summary>Gets the stream returned by the factory.</summary>
        public Stream Stream { get; } = new();

        /// <summary>Gets the definition supplied to the factory.</summary>
        public StreamDefinition<CounterState, CounterInput>? Definition { get; private set; }

        /// <summary>Gets the number of factory calls.</summary>
        public int GetOrCreateCalls { get; private set; }

        /// <inheritdoc />
        public ISyncEngine SyncEngine => Engine;

        /// <inheritdoc />
        public IObservable<SyncState> SyncStates => States;

        /// <inheritdoc />
        public IOccasionallyConnectedStream<TState, TInput> GetOrCreateStream<TState, TInput>(
            StreamDefinition<TState, TInput> definition)
        {
            GetOrCreateCalls++;
            Definition = (StreamDefinition<CounterState, CounterInput>)(object)definition;
            return (IOccasionallyConnectedStream<TState, TInput>)(object)Stream;
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask StartAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask StopAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Provides a valid counter projection for a stream definition.</summary>
    private sealed class Projection : ILocalProjection<CounterState, CounterInput>
    {
        /// <inheritdoc />
        public CounterState InitialState => default;

        /// <inheritdoc />
        public CounterState ApplyLocal(CounterState state, CounterInput input, SyncOperation operation) => new(state.Value + input.Value);

        /// <inheritdoc />
        public CounterState ApplyRemote(CounterState state, CounterInput input, RemoteEvent remoteEvent) => new(state.Value + input.Value);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public CounterState Reconcile(CounterState state, ConflictResolutionResult result) => state;
    }

    /// <summary>Provides a stream factory return value.</summary>
    private sealed class Stream : IOccasionallyConnectedStream<CounterState, CounterInput>
    {
        /// <inheritdoc />
        public StreamId StreamId { get; } = new("counter");

        /// <inheritdoc />
        public SubscriptionId SubscriptionId { get; } = SubscriptionId.New();

        /// <inheritdoc />
        public IObservable<CounterState> Local { get; } = new EmptyObservable<CounterState>();

        /// <inheritdoc />
        public IObservable<RemoteMessage<CounterInput>> Remote { get; } = new EmptyObservable<RemoteMessage<CounterInput>>();

        /// <inheritdoc />
        public IObservable<SyncState> SyncStates { get; } = new EmptyObservable<SyncState>();

        /// <inheritdoc />
        public IObservable<SyncOperationStatus> OperationStates { get; } = new EmptyObservable<SyncOperationStatus>();

        /// <inheritdoc />
        public IObservable<OccasionallyConnectedFault> Faults { get; } = new EmptyObservable<OccasionallyConnectedFault>();

        /// <inheritdoc />
        public IObserver<CounterInput> Input { get; } = new EmptyObserver<CounterInput>();

        /// <inheritdoc />
        public ValueTask<PublishReceipt> PublishAsync(CounterInput value, RemotePublishOptions? options, CancellationToken cancellationToken) => throw new NotSupportedException();

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask StartAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask StopAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Provides a synchronization engine for the contract property.</summary>
    private sealed class Engine : ISyncEngine
    {
        /// <inheritdoc />
        public IObservable<SyncState> SyncStates { get; } = new EmptyObservable<SyncState>();

        /// <inheritdoc />
        public IObservable<SyncOperationStatus> OperationStates { get; } = new EmptyObservable<SyncOperationStatus>();

        /// <inheritdoc />
        public IObservable<OccasionallyConnectedFault> Faults { get; } = new EmptyObservable<OccasionallyConnectedFault>();

        /// <inheritdoc />
        public ValueTask<PublishReceipt> EnqueueOperationAsync(SyncOperation operation, CancellationToken cancellationToken) => throw new NotSupportedException();

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(
            OperationId operationId,
            CancellationToken cancellationToken) => new((SyncOperationStatus?)null);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask StartAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask StopAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask TriggerSyncAsync(CancellationToken cancellationToken) => ValueTask.CompletedTask;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Provides an observable that does not publish values.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class EmptyObservable<T> : IObservable<T>
    {
        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Subscribe(IObserver<T> observer) => EmptySubscription.Instance;
    }

    /// <summary>Ignores observed values.</summary>
    /// <typeparam name="T">The observed value type.</typeparam>
    private sealed class EmptyObserver<T> : IObserver<T>
    {
        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        public void OnError(Exception error)
        {
        }

        /// <inheritdoc />
        public void OnNext(T value)
        {
        }
    }

    /// <summary>Provides a stable empty subscription.</summary>
    private sealed class EmptySubscription : IDisposable
    {
        /// <summary>Gets the singleton empty subscription.</summary>
        public static EmptySubscription Instance { get; } = new();

        /// <inheritdoc />
        public void Dispose()
        {
        }
    }
}
