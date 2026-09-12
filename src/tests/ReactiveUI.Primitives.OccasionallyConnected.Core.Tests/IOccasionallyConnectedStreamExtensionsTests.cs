// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="IOccasionallyConnectedStreamExtensions"/>.</summary>
public sealed class IOccasionallyConnectedStreamExtensionsTests
{
    /// <summary>The first published input value.</summary>
    private const int FirstValue = 42;

    /// <summary>The second published input value.</summary>
    private const int SecondValue = 43;

    /// <summary>The failing published input value.</summary>
    private const int FailingValue = 45;

    /// <summary>The expected lifecycle call count including the failure invocation.</summary>
    private const int LifecycleCallCount = 2;

    /// <summary>Verifies publishing without optional arguments forwards null options and the default token.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PublishAsyncForwardsNullOptionsDefaultTokenAndReceipt()
    {
        var stream = new ContractStream<CounterState, CounterInput>();
        var value = new CounterInput(FirstValue);

        var receipt = await stream.PublishAsync(value);

        await Assert.That(stream.PublishedValue).IsEqualTo(value);
        await Assert.That(stream.PublishOptions).IsNull();
        await Assert.That(stream.PublishCancellationToken).IsEqualTo(CancellationToken.None);
        await Assert.That(receipt).IsSameReferenceAs(stream.Receipt);
        await Assert.That(stream.PublishCalls).IsEqualTo(1);
    }

    /// <summary>Verifies publishing with explicit options preserves the supplied option instance.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PublishAsyncForwardsOptionsAndDefaultToken()
    {
        var stream = new ContractStream<CounterState, CounterInput>();
        var value = new CounterInput(SecondValue);
        var options = new RemotePublishOptions { StreamId = stream.StreamId };

        _ = await stream.PublishAsync(value, options);

        await Assert.That(stream.PublishedValue).IsEqualTo(value);
        await Assert.That(stream.PublishOptions).IsSameReferenceAs(options);
        await Assert.That(stream.PublishCancellationToken).IsEqualTo(CancellationToken.None);
        await Assert.That(stream.PublishCalls).IsEqualTo(1);
    }

    /// <summary>Verifies all publishing overloads preserve a reference input and their returned receipt.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PublishAsyncPreservesReferenceInputAndCancellation()
    {
        var stream = new ContractStream<CounterState, object>();
        object value = new();
        using var cancellationSource = new CancellationTokenSource();
        var token = cancellationSource.Token;
        var options = new RemotePublishOptions { StreamId = stream.StreamId };

        var first = await stream.PublishAsync(value);
        await Assert.That(stream.PublishedValue).IsSameReferenceAs(value);
        await Assert.That(first).IsSameReferenceAs(stream.Receipt);

        var second = await stream.PublishAsync(value, options);
        await Assert.That(stream.PublishedValue).IsSameReferenceAs(value);
        await Assert.That(stream.PublishOptions).IsSameReferenceAs(options);
        await Assert.That(second).IsSameReferenceAs(stream.Receipt);

        var third = await stream.PublishAsync(value, token);
        await Assert.That(stream.PublishedValue).IsSameReferenceAs(value);
        await Assert.That(stream.PublishOptions).IsNull();
        await Assert.That(stream.PublishCancellationToken).IsEqualTo(token);
        await Assert.That(third).IsSameReferenceAs(stream.Receipt);
    }

    /// <summary>Verifies publishing propagates the underlying failure unchanged.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PublishAsyncPreservesFailure()
    {
        var error = CreateFailure("publish failure");
        var stream = new ContractStream<CounterState, CounterInput> { PublishError = error };
        Func<Task> action = async () => await stream.PublishAsync(new(FailingValue));

        var thrown = await Assert.That(action).ThrowsExactly<InvalidOperationException>();

        await Assert.That(thrown).IsSameReferenceAs(error);
        await Assert.That(stream.PublishCalls).IsEqualTo(1);
    }

    /// <summary>Verifies startup forwards the default token and preserves failure identity.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StartAsyncForwardsDefaultTokenAndPreservesFailure()
    {
        var stream = new ContractStream<CounterState, CounterInput>();
        await stream.StartAsync();
        await Assert.That(stream.StartCancellationToken).IsEqualTo(CancellationToken.None);

        var error = CreateFailure("start failure");
        stream.StartError = error;
        Func<Task> action = async () => await stream.StartAsync();
        var thrown = await Assert.That(action).ThrowsExactly<InvalidOperationException>();

        await Assert.That(thrown).IsSameReferenceAs(error);
        await Assert.That(stream.StartCalls).IsEqualTo(LifecycleCallCount);
    }

    /// <summary>Verifies shutdown forwards the default token and preserves failure identity.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StopAsyncForwardsDefaultTokenAndPreservesFailure()
    {
        var stream = new ContractStream<CounterState, CounterInput>();
        await stream.StopAsync();
        await Assert.That(stream.StopCancellationToken).IsEqualTo(CancellationToken.None);

        var error = CreateFailure("stop failure");
        stream.StopError = error;
        Func<Task> action = async () => await stream.StopAsync();
        var thrown = await Assert.That(action).ThrowsExactly<InvalidOperationException>();

        await Assert.That(thrown).IsSameReferenceAs(error);
        await Assert.That(stream.StopCalls).IsEqualTo(LifecycleCallCount);
    }

    /// <summary>Creates a failure used to verify exception identity.</summary>
    /// <param name="message">The failure message.</param>
    /// <returns>The created failure.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static InvalidOperationException CreateFailure(string message) => new(message);

    /// <summary>Represents a typed local state value.</summary>
    /// <param name="Value">The value carried by the state.</param>
    private readonly record struct CounterState(int Value);

    /// <summary>Represents a typed input value.</summary>
    /// <param name="Value">The value carried by the input.</param>
    private readonly record struct CounterInput(int Value);

    /// <summary>Records contract calls without providing a stream runtime implementation.</summary>
    /// <typeparam name="TState">The local state type.</typeparam>
    /// <typeparam name="TInput">The input value type.</typeparam>
    private sealed class ContractStream<TState, TInput> : IOccasionallyConnectedStream<TState, TInput>
    {
        /// <summary>The client sequence used by the expected receipt.</summary>
        private const int ClientSequence = 1;

        /// <summary>The stream name used by the contract test double.</summary>
        private const string StreamName = "sensor/temperature";

        /// <summary>Gets the expected local admission receipt.</summary>
        public PublishReceipt Receipt { get; } = new(
            OperationId.New(),
            ClientSequence,
            SyncOperationState.SavedLocally,
            DateTimeOffset.UnixEpoch);

        /// <summary>Gets the stream identifier.</summary>
        public StreamId StreamId { get; } = new(StreamName);

        /// <summary>Gets the durable subscription identifier.</summary>
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

        /// <summary>Gets the value supplied to publication.</summary>
        public TInput? PublishedValue { get; private set; }

        /// <summary>Gets the options supplied to publication.</summary>
        public RemotePublishOptions? PublishOptions { get; private set; }

        /// <summary>Gets the cancellation token supplied to publication.</summary>
        public CancellationToken PublishCancellationToken { get; private set; }

        /// <summary>Gets the cancellation token supplied to startup.</summary>
        public CancellationToken StartCancellationToken { get; private set; }

        /// <summary>Gets the cancellation token supplied to shutdown.</summary>
        public CancellationToken StopCancellationToken { get; private set; }

        /// <summary>Gets the number of publication calls.</summary>
        public int PublishCalls { get; private set; }

        /// <summary>Gets the number of startup calls.</summary>
        public int StartCalls { get; private set; }

        /// <summary>Gets the number of shutdown calls.</summary>
        public int StopCalls { get; private set; }

        /// <summary>Gets or sets the startup error.</summary>
        public Exception? StartError { get; set; }

        /// <summary>Gets or sets the publication error.</summary>
        public Exception? PublishError { get; set; }

        /// <summary>Gets or sets the shutdown error.</summary>
        public Exception? StopError { get; set; }

        /// <inheritdoc />
        public ValueTask<PublishReceipt> PublishAsync(
            TInput value,
            RemotePublishOptions? options,
            CancellationToken cancellationToken)
        {
            PublishCalls++;
            PublishedValue = value;
            PublishOptions = options;
            PublishCancellationToken = cancellationToken;
            return PublishError is null ? new(Receipt) : ValueTask.FromException<PublishReceipt>(PublishError);
        }

        /// <inheritdoc />
        public ValueTask StartAsync(CancellationToken cancellationToken)
        {
            StartCalls++;
            StartCancellationToken = cancellationToken;
            return StartError is null ? ValueTask.CompletedTask : ValueTask.FromException(StartError);
        }

        /// <inheritdoc />
        public ValueTask StopAsync(CancellationToken cancellationToken)
        {
            StopCalls++;
            StopCancellationToken = cancellationToken;
            return StopError is null ? ValueTask.CompletedTask : ValueTask.FromException(StopError);
        }

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
