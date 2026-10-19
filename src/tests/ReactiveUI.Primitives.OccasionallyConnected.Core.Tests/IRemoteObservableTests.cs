// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="IRemoteObservable{T}"/>.</summary>
public sealed class IRemoteObservableTests
{
    /// <summary>The stream identifier used by the remote test double.</summary>
    private const string StreamName = "sensor/temperature";

    /// <summary>The expected received message count.</summary>
    private const int ReceivedMessageCount = 2;

    /// <summary>The second decoded value.</summary>
    private const int SecondValue = 2;

    /// <summary>Verifies a remote observable forwards the configured subscription and delivers messages in order.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeRemoteForwardsOptionsAndDeliversCommittedMessagesInOrder()
    {
        var remote = new RemoteObservable();
        var options = new RemoteSubscriptionOptions { StreamId = new(StreamName) };
        var received = new List<RemoteMessage<int>>();
        using var subscription = remote.SubscribeRemote(options).Subscribe(new RecordingObserver(received));

        remote.Emit(CreateMessage(1));
        remote.Emit(CreateMessage(SecondValue));

        await Assert.That(remote.Options).IsSameReferenceAs(options);
        await Assert.That(received).Count().IsEqualTo(ReceivedMessageCount);
        await Assert.That(received[0].Value).IsEqualTo(1);
        await Assert.That(received[1].Value).IsEqualTo(SecondValue);
    }

    /// <summary>Verifies errors from the remote observable propagate to its subscriber.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubscribeRemotePropagatesProducerError()
    {
        var remote = new RemoteObservable();
        Exception? receivedError = null;
        using var subscription = remote.SubscribeRemote(new() { StreamId = new(StreamName) }).Subscribe(
            new RecordingObserver([], error => receivedError = error));
        var error = new InvalidOperationException("remote fault");

        remote.Fail(error);

        await Assert.That(receivedError).IsSameReferenceAs(error);
    }

    /// <summary>Creates a representative committed remote message.</summary>
    /// <param name="value">The decoded value.</param>
    /// <returns>The created message.</returns>
    private static RemoteMessage<int> CreateMessage(int value) => new(
        Guid.NewGuid(),
        new(StreamName),
        $"cursor-{value}",
        DateTimeOffset.UnixEpoch,
        value);

    /// <summary>Exposes a minimal remote observable test double.</summary>
    private sealed class RemoteObservable : IRemoteObservable<int>
    {
        /// <summary>Gets the subscription options passed to the remote observable.</summary>
        public RemoteSubscriptionOptions? Options { get; private set; }

        /// <summary>Gets or sets the active observer.</summary>
        private IObserver<RemoteMessage<int>>? Observer { get; set; }

        /// <inheritdoc />
        public IObservable<RemoteMessage<int>> SubscribeRemote(RemoteSubscriptionOptions options)
        {
            Options = options;
            return new TestObservable(this);
        }

        /// <summary>Emits a committed message.</summary>
        /// <param name="message">The committed message.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Emit(RemoteMessage<int> message) => Observer?.OnNext(message);

        /// <summary>Emits a producer error.</summary>
        /// <param name="error">The producer error.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Fail(Exception error) => Observer?.OnError(error);

        /// <summary>Subscribes observers to the test double.</summary>
        /// <param name="owner">The owning remote observable.</param>
        private sealed class TestObservable(RemoteObservable owner) : IObservable<RemoteMessage<int>>
        {
            /// <inheritdoc />
            public IDisposable Subscribe(IObserver<RemoteMessage<int>> observer)
            {
                owner.Observer = observer;
                return new Subscription(owner, observer);
            }
        }

        /// <summary>Removes a subscribed observer.</summary>
        /// <param name="owner">The owning remote observable.</param>
        /// <param name="observer">The subscribed observer.</param>
        private sealed class Subscription(RemoteObservable owner, IObserver<RemoteMessage<int>> observer) : IDisposable
        {
            /// <inheritdoc />
            public void Dispose()
            {
                if (!ReferenceEquals(owner.Observer, observer))
                {
                    return;
                }

                owner.Observer = null;
            }
        }
    }

    /// <summary>Records observer notifications.</summary>
    /// <param name="messages">The message collection to update.</param>
    /// <param name="onError">The optional error handler.</param>
    private sealed class RecordingObserver(List<RemoteMessage<int>> messages, Action<Exception>? onError = null) : IObserver<RemoteMessage<int>>
    {
        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => onError?.Invoke(error);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(RemoteMessage<int> value) => messages.Add(value);
    }
}
