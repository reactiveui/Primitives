// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="IRemoteObserverExtensions"/>.</summary>
public sealed class IRemoteObserverExtensionsTests
{
    /// <summary>The published value.</summary>
    private const int PublishedValue = 42;

    /// <summary>The stream identifier used by the remote observer test double.</summary>
    private const string StreamName = "sensor/temperature";

    /// <summary>Verifies the publish convenience overload forwards its exact arguments and cancellation token.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PublishAsyncForwardsValueOptionsCancellationAndReceipt()
    {
        var remote = new RemoteObserver();
        var options = new RemotePublishOptions { StreamId = new(StreamName) };

        var receipt = await remote.PublishAsync(PublishedValue, options);

        await Assert.That(remote.Value).IsEqualTo(PublishedValue);
        await Assert.That(remote.Options).IsSameReferenceAs(options);
        await Assert.That(remote.CancellationToken).IsEqualTo(CancellationToken.None);
        await Assert.That(receipt).IsSameReferenceAs(remote.Receipt);
        await Assert.That(remote.PublishCalls).IsEqualTo(1);
    }

    /// <summary>Verifies the observer convenience overload preserves the bridge identity and forwards default input options.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AsObserverForwardsOptionsAndPreservesBridgeIdentity()
    {
        var remote = new RemoteObserver();
        var options = new RemotePublishOptions { StreamId = new(StreamName) };

        var bridge = remote.AsObserver(options);

        await Assert.That(remote.Options).IsSameReferenceAs(options);
        await Assert.That(remote.InputOptions).IsNull();
        await Assert.That(bridge).IsSameReferenceAs(remote.Bridge);
        await Assert.That(remote.BridgeCalls).IsEqualTo(1);
    }

    /// <summary>Verifies the interface preserves explicitly supplied observer input options.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AsObserverPreservesExplicitInputOptions()
    {
        var remote = new RemoteObserver();
        var inputOptions = new ObserverInputOptions { BufferCapacity = 1 };

        _ = ((IRemoteObserver<int>)remote).AsObserver(new() { StreamId = new(StreamName) }, inputOptions);

        await Assert.That(remote.InputOptions).IsSameReferenceAs(inputOptions);
    }

    /// <summary>Verifies publish errors are propagated from the underlying remote observer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PublishAsyncPropagatesRemoteObserverError()
    {
        var error = new InvalidOperationException("publish failure");
        var remote = new RemoteObserver { Error = error };
        Func<Task> action = async () => await remote.PublishAsync(PublishedValue, new() { StreamId = new(StreamName) });

        var thrown = await Assert.That(action).ThrowsExactly<InvalidOperationException>();

        await Assert.That(thrown).IsSameReferenceAs(error);
    }

    /// <summary>Verifies bridge creation failures propagate without wrapping or retry.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task AsObserverPropagatesBridgeCreationError()
    {
        var error = new InvalidOperationException("bridge failure");
        var remote = new RemoteObserver { BridgeError = error };
        Action action = () => remote.AsObserver(new() { StreamId = new(StreamName) });

        var thrown = await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        await Assert.That(thrown).IsSameReferenceAs(error);
        await Assert.That(remote.BridgeCalls).IsEqualTo(1);
    }

    /// <summary>Records remote observer calls.</summary>
    private sealed class RemoteObserver : IRemoteObserver<int>
    {
        /// <summary>Gets the expected publish receipt.</summary>
        public PublishReceipt Receipt { get; } = new(OperationId.New(), 1, SyncOperationState.SavedLocally, DateTimeOffset.UnixEpoch);

        /// <summary>Gets the observer bridge.</summary>
        public IObserver<int> Bridge { get; } = new BridgeObserver();

        /// <summary>Gets the published value.</summary>
        public int Value { get; private set; }

        /// <summary>Gets the publish options.</summary>
        public RemotePublishOptions? Options { get; private set; }

        /// <summary>Gets the supplied observer input options.</summary>
        public ObserverInputOptions? InputOptions { get; private set; }

        /// <summary>Gets the supplied cancellation token.</summary>
        public CancellationToken CancellationToken { get; private set; }

        /// <summary>Gets the error returned by publishing.</summary>
        public Exception? Error { get; init; }

        /// <summary>Gets a bridge creation failure.</summary>
        public Exception? BridgeError { get; init; }

        /// <summary>Gets the number of publication attempts.</summary>
        public int PublishCalls { get; private set; }

        /// <summary>Gets the number of bridge creation attempts.</summary>
        public int BridgeCalls { get; private set; }

        /// <inheritdoc />
        public ValueTask<PublishReceipt> PublishAsync(int value, RemotePublishOptions options, CancellationToken cancellationToken)
        {
            PublishCalls++;
            Value = value;
            Options = options;
            CancellationToken = cancellationToken;
            return Error is null ? new(Receipt) : ValueTask.FromException<PublishReceipt>(Error);
        }

        /// <inheritdoc />
        public IObserver<int> AsObserver(RemotePublishOptions options, ObserverInputOptions? inputOptions)
        {
            BridgeCalls++;
            Options = options;
            InputOptions = inputOptions;
            if (BridgeError is not null)
            {
                throw BridgeError;
            }

            return Bridge;
        }
    }

    /// <summary>Provides a stable observer bridge identity.</summary>
    private sealed class BridgeObserver : IObserver<int>
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
        public void OnNext(int value)
        {
        }
    }
}
