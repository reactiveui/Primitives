// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="IRemoteTransportAdapterExtensions"/>.</summary>
public sealed class IRemoteTransportAdapterExtensionsTests
{
    /// <summary>Verifies the connect overload forwards the request and cancellation token once.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConnectAsyncForwardsRequestOnceAndReturnsSession()
    {
        var session = new Session();
        var adapter = new Adapter(session);
        var request = new TransportConnectRequest(CreateVersionRange(), new("client"), [DeliveryGuarantee.AtLeastOnce]);
        var actual = await adapter.ConnectAsync(request);
        await Assert.That(actual).IsSameReferenceAs(session);
        await Assert.That(adapter.Calls).IsEqualTo(1);
        await Assert.That(adapter.Request).IsEqualTo(request);
        await Assert.That(adapter.Token).IsEqualTo(CancellationToken.None);
    }

    /// <summary>Verifies the connect overload propagates adapter failures.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConnectAsyncPropagatesAdapterFailure()
    {
        var error = new InvalidOperationException("failure");
        var adapter = new Adapter(new Session()) { Error = error };
        Func<Task> action = async () => await adapter.ConnectAsync(new(CreateVersionRange(), new("client"), []));
        var thrown = await Assert.That(action).ThrowsExactly<InvalidOperationException>();
        await Assert.That(thrown).IsSameReferenceAs(error);
        await Assert.That(adapter.Calls).IsEqualTo(1);
        await Assert.That(adapter.Token).IsEqualTo(CancellationToken.None);
    }

    /// <summary>Creates a supported protocol version range.</summary>
    /// <returns>The protocol version range.</returns>
    private static VersionRange CreateVersionRange() => new(new(1, 0), new(1, 1));

    /// <summary>Records remote transport adapter calls.</summary>
    /// <param name="session">The session returned by the adapter.</param>
    private sealed class Adapter(IRemoteTransportSession session) : IRemoteTransportAdapter
    {
        /// <summary>Gets the call count.</summary>
        public int Calls { get; private set; }

        /// <summary>Gets the recorded connect request.</summary>
        public TransportConnectRequest? Request { get; private set; }

        /// <summary>Gets the recorded cancellation token.</summary>
        public CancellationToken Token { get; private set; }

        /// <summary>Gets the exception to throw from connect.</summary>
        public Exception? Error { get; init; }

        /// <inheritdoc />
        public RemoteTransportCapabilities Capabilities => RemoteTransportCapabilities.None;

        /// <inheritdoc />
        public ValueTask<IRemoteTransportSession> ConnectAsync(
            TransportConnectRequest request,
            CancellationToken cancellationToken)
        {
            Calls++;
            Request = request;
            Token = cancellationToken;
            return Error is null ? new(session) : ValueTask.FromException<IRemoteTransportSession>(Error);
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    /// <summary>Remote transport session stub.</summary>
    private sealed class Session : IRemoteTransportSession
    {
        /// <inheritdoc />
        public NegotiatedCapabilities NegotiatedCapabilities =>
            new(new Version(1, 0), RemoteTransportCapabilities.None, 1, 1, TimeSpan.Zero, TimeSpan.Zero);

        /// <inheritdoc />
        public ValueTask<RemoteSyncResult> PushAsync(SyncBatch batch, CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc />
        public IAsyncEnumerable<RemoteEventBatch> SubscribeAsync(
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc />
        public ValueTask AcknowledgeAsync(
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken) =>
            throw new NotSupportedException();

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
