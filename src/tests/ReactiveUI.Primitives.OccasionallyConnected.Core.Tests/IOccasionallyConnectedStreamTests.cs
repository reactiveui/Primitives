// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="IOccasionallyConnectedStream{TState, TInput}"/>.</summary>
public sealed class IOccasionallyConnectedStreamTests
{
    /// <summary>The stream name used by the contract surface.</summary>
    private const string StreamName = "sensor/temperature";

    /// <summary>Verifies the two-type and single-type interfaces expose the required typed property surface.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ContractsExposeTypedPropertySurfaceAndSingleTypeAlias()
    {
        var twoTypeStream = new SurfaceStream<TemperatureState, TemperatureInput>();
        var singleTypeStream = new SingleTypeStream();

        await AssertTwoTypePropertySurface(
            (IOccasionallyConnectedStream<TemperatureState, TemperatureInput>)twoTypeStream,
            twoTypeStream);
        await AssertSingleTypeAlias(
            (IOccasionallyConnectedStream<TemperatureState>)singleTypeStream,
            singleTypeStream);
    }

    /// <summary>Asserts the two-type contract maps each property to its declared type.</summary>
    /// <param name="contract">The two-type stream contract.</param>
    /// <param name="stream">The corresponding property surface.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertTwoTypePropertySurface(
        IOccasionallyConnectedStream<TemperatureState, TemperatureInput> contract,
        SurfaceStream<TemperatureState, TemperatureInput> stream)
    {
        await Assert.That(contract.StreamId).IsEqualTo(stream.StreamId);
        await Assert.That(contract.SubscriptionId).IsEqualTo(stream.SubscriptionId);
        await AssertObservableProperties(contract.Local, contract.Remote, contract.Input, stream);
        await Assert.That(contract.SyncStates).IsSameReferenceAs(stream.SyncStates);
        await Assert.That(contract.OperationStates).IsSameReferenceAs(stream.OperationStates);
        await Assert.That(contract.Faults).IsSameReferenceAs(stream.Faults);
    }

    /// <summary>Asserts the single-type alias preserves matching local, remote, and input types.</summary>
    /// <param name="contract">The single-type stream contract.</param>
    /// <param name="stream">The corresponding property surface.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertSingleTypeAlias(
        IOccasionallyConnectedStream<TemperatureState> contract,
        SingleTypeStream stream)
    {
        await Assert.That(contract.Local).IsSameReferenceAs(stream.Local);
        await Assert.That(contract.Remote).IsSameReferenceAs(stream.Remote);
        await Assert.That(contract.Input).IsSameReferenceAs(stream.Input);
    }

    /// <summary>Asserts observable and observer reference identity with the distinct input type.</summary>
    /// <param name="local">The typed local observable.</param>
    /// <param name="remote">The typed remote observable.</param>
    /// <param name="input">The typed input observer.</param>
    /// <param name="stream">The corresponding property surface.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task AssertObservableProperties(
        IObservable<TemperatureState> local,
        IObservable<RemoteMessage<TemperatureInput>> remote,
        IObserver<TemperatureInput> input,
        SurfaceStream<TemperatureState, TemperatureInput> stream)
    {
        await Assert.That(local).IsSameReferenceAs(stream.Local);
        await Assert.That(remote).IsSameReferenceAs(stream.Remote);
        await Assert.That(input).IsSameReferenceAs(stream.Input);
    }

    /// <summary>Represents local temperature state.</summary>
    /// <param name="Value">The measured temperature.</param>
    private readonly record struct TemperatureState(int Value);

    /// <summary>Represents an input mutation distinct from local state.</summary>
    /// <param name="Delta">The requested temperature change.</param>
    private readonly record struct TemperatureInput(int Delta);

    /// <summary>Provides a two-type contract implementation used only to inspect member typing.</summary>
    /// <typeparam name="TState">The local state type.</typeparam>
    /// <typeparam name="TInput">The input type.</typeparam>
    private sealed class SurfaceStream<TState, TInput> : IOccasionallyConnectedStream<TState, TInput>
    {
        /// <inheritdoc />
        public StreamId StreamId { get; } = new(StreamName);

        /// <inheritdoc />
        public SubscriptionId SubscriptionId { get; } = SubscriptionId.New();

        /// <inheritdoc />
        public IObservable<TState> Local { get; } = new EmptyObservable<TState>();

        /// <inheritdoc />
        public IObservable<RemoteMessage<TInput>> Remote { get; } = new EmptyObservable<RemoteMessage<TInput>>();

        /// <inheritdoc />
        public IObservable<SyncState> SyncStates { get; } = new EmptyObservable<SyncState>();

        /// <inheritdoc />
        public IObservable<SyncOperationStatus> OperationStates { get; } = new EmptyObservable<SyncOperationStatus>();

        /// <inheritdoc />
        public IObservable<OccasionallyConnectedFault> Faults { get; } = new EmptyObservable<OccasionallyConnectedFault>();

        /// <inheritdoc />
        public IObserver<TInput> Input { get; } = new EmptyObserver<TInput>();

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PublishReceipt> PublishAsync(
            TInput value,
            RemotePublishOptions? options,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new PublishReceipt(
                OperationId.New(),
                1,
                SyncOperationState.SavedLocally,
                DateTimeOffset.UnixEpoch));

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

    /// <summary>Provides the single-type alias contract.</summary>
    private sealed class SingleTypeStream : IOccasionallyConnectedStream<TemperatureState>
    {
        /// <inheritdoc />
        public StreamId StreamId { get; } = new(StreamName);

        /// <inheritdoc />
        public SubscriptionId SubscriptionId { get; } = SubscriptionId.New();

        /// <inheritdoc />
        public IObservable<TemperatureState> Local { get; } = new EmptyObservable<TemperatureState>();

        /// <inheritdoc />
        public IObservable<RemoteMessage<TemperatureState>> Remote { get; } = new EmptyObservable<RemoteMessage<TemperatureState>>();

        /// <inheritdoc />
        public IObservable<SyncState> SyncStates { get; } = new EmptyObservable<SyncState>();

        /// <inheritdoc />
        public IObservable<SyncOperationStatus> OperationStates { get; } = new EmptyObservable<SyncOperationStatus>();

        /// <inheritdoc />
        public IObservable<OccasionallyConnectedFault> Faults { get; } = new EmptyObservable<OccasionallyConnectedFault>();

        /// <inheritdoc />
        public IObserver<TemperatureState> Input { get; } = new EmptyObserver<TemperatureState>();

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<PublishReceipt> PublishAsync(
            TemperatureState value,
            RemotePublishOptions? options,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new PublishReceipt(
                OperationId.New(),
                1,
                SyncOperationState.SavedLocally,
                DateTimeOffset.UnixEpoch));

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
