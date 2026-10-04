// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Authorization policy helpers for <see cref="ServerStreamHubTests"/>.</summary>
public sealed partial class ServerStreamHubTests
{
    /// <summary>Blocks publish authorization until released by the test.</summary>
    private sealed class BlockingPublishPolicy : IServerStreamAuthorizationPolicy
    {
        /// <summary>Tracks when publish authorization starts.</summary>
        private readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Releases publish authorization.</summary>
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Tracks when disposal cancellation reaches publish authorization.</summary>
        private readonly TaskCompletionSource _canceled = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        public async ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
            ServerAuthenticatedClient client,
            SyncBatch batch,
            CancellationToken cancellationToken)
        {
            _ = _started.TrySetResult();
            await using var registration = cancellationToken.UnsafeRegister(CompleteCancellation, _canceled);
            await _released.Task.ConfigureAwait(false);
            return new(Tenant, client.ClientId);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
            ServerAuthenticatedClient client,
            SyncOperation operation,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
            ServerAuthenticatedClient client,
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
            ServerAuthenticatedClient client,
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, client.ClientId));

        /// <summary>Releases the blocking publish authorization.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Release() => _ = _released.TrySetResult();

        /// <summary>Waits until publish authorization has observed cancellation.</summary>
        /// <returns>The wait task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task WaitUntilCanceledAsync() => _canceled.Task;

        /// <summary>Waits until publish authorization has started.</summary>
        /// <returns>The wait task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task WaitUntilStartedAsync() => _started.Task;

        /// <summary>Completes the cancellation signal.</summary>
        /// <param name="state">The cancellation completion source.</param>
        private static void CompleteCancellation(object? state)
        {
            if (state is not TaskCompletionSource completion)
            {
                return;
            }

            _ = completion.TrySetResult();
        }
    }

    /// <summary>Blocks subscribe authorization until released by the test.</summary>
    private sealed class BlockingSubscribePolicy : IServerStreamAuthorizationPolicy
    {
        /// <summary>Tracks when subscribe authorization starts.</summary>
        private readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Releases subscribe authorization.</summary>
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Tracks when subscribe authorization observes cancellation.</summary>
        private readonly TaskCompletionSource _canceled = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
            ServerAuthenticatedClient client,
            SyncBatch batch,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
            ServerAuthenticatedClient client,
            SyncOperation operation,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, client.ClientId));

        /// <inheritdoc/>
        public async ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
            ServerAuthenticatedClient client,
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken)
        {
            _ = _started.TrySetResult();
            await using var registration = cancellationToken.UnsafeRegister(CompleteCancellation, _canceled);
            await _released.Task.ConfigureAwait(false);
            return new(Tenant, client.ClientId);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
            ServerAuthenticatedClient client,
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, client.ClientId));

        /// <summary>Releases the blocking subscribe authorization.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Release() => _ = _released.TrySetResult();

        /// <summary>Waits until subscribe authorization has observed cancellation.</summary>
        /// <returns>The wait task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task WaitUntilCanceledAsync() => _canceled.Task;

        /// <summary>Waits until subscribe authorization has started.</summary>
        /// <returns>The wait task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task WaitUntilStartedAsync() => _started.Task;

        /// <summary>Completes the cancellation signal.</summary>
        /// <param name="state">The cancellation completion source.</param>
        private static void CompleteCancellation(object? state)
        {
            if (state is not TaskCompletionSource completion)
            {
                return;
            }

            _ = completion.TrySetResult();
        }
    }

    /// <summary>Blocks subscribe authorization and holds a throwing cancellation callback until released by the test.</summary>
    private sealed class ThrowingBlockingSubscribePolicy : IServerStreamAuthorizationPolicy
    {
        /// <summary>The callback failure reason.</summary>
        private const string CallbackFailureReason = "subscribe-callback-failed";

        /// <summary>Tracks when subscribe authorization starts.</summary>
        private readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Tracks when subscribe authorization observes cancellation.</summary>
        private readonly TaskCompletionSource _canceled = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Releases subscribe authorization.</summary>
        private readonly TaskCompletionSource _authorizationReleased = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Whether the cancellation callback may continue.</summary>
        private int _callbackReleased;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
            ServerAuthenticatedClient client,
            SyncBatch batch,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
            ServerAuthenticatedClient client,
            SyncOperation operation,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, client.ClientId));

        /// <inheritdoc/>
        public async ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
            ServerAuthenticatedClient client,
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken)
        {
            _ = _started.TrySetResult();
            await using var callback = cancellationToken.UnsafeRegister(HoldThenThrowCancellation, this);
            await _authorizationReleased.Task.ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            return new(Tenant, client.ClientId);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
            ServerAuthenticatedClient client,
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, client.ClientId));

        /// <summary>Releases the blocking subscribe authorization.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ReleaseAuthorization() => _ = _authorizationReleased.TrySetResult();

        /// <summary>Releases the blocking cancellation callback.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void ReleaseCallback() => _ = Interlocked.Exchange(ref _callbackReleased, 1);

        /// <summary>Waits until subscribe authorization has observed cancellation.</summary>
        /// <returns>The wait task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task WaitUntilCanceledAsync() => _canceled.Task;

        /// <summary>Waits until subscribe authorization has started.</summary>
        /// <returns>The wait task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task WaitUntilStartedAsync() => _started.Task;

        /// <summary>Holds cancellation and then throws.</summary>
        /// <param name="state">The callback policy state.</param>
        /// <exception cref="InvalidOperationException">Always thrown by this test callback.</exception>
        private static void HoldThenThrowCancellation(object? state)
        {
            if (state is not ThrowingBlockingSubscribePolicy policy)
            {
                return;
            }

            _ = policy._canceled.TrySetResult();
            SpinWait.SpinUntil(() => Volatile.Read(ref policy._callbackReleased) != 0);
            throw new InvalidOperationException(CallbackFailureReason);
        }
    }

    /// <summary>Registers a throwing cancellation callback during publish authorization.</summary>
    private sealed class ThrowingCancellationPolicy : IServerStreamAuthorizationPolicy
    {
        /// <summary>The callback failure reason.</summary>
        private const string CallbackFailureReason = "callback-failed";

        /// <summary>Tracks when publish authorization starts.</summary>
        private readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Releases publish authorization.</summary>
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Tracks when disposal cancellation reaches publish authorization.</summary>
        private readonly TaskCompletionSource _canceled = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        public async ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
            ServerAuthenticatedClient client,
            SyncBatch batch,
            CancellationToken cancellationToken)
        {
            _ = _started.TrySetResult();
            await using var cancellationSignal = cancellationToken.UnsafeRegister(CompleteCancellation, _canceled);
            await using var throwingRegistration = cancellationToken.UnsafeRegister(ThrowCancellation, null);
            await _released.Task.ConfigureAwait(false);
            return new(Tenant, client.ClientId);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
            ServerAuthenticatedClient client,
            SyncOperation operation,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
            ServerAuthenticatedClient client,
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
            ServerAuthenticatedClient client,
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, client.ClientId));

        /// <summary>Releases the blocking publish authorization.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Release() => _ = _released.TrySetResult();

        /// <summary>Waits until publish authorization has observed cancellation.</summary>
        /// <returns>The wait task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task WaitUntilCanceledAsync() => _canceled.Task;

        /// <summary>Waits until publish authorization has started.</summary>
        /// <returns>The wait task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task WaitUntilStartedAsync() => _started.Task;

        /// <summary>Completes the cancellation signal.</summary>
        /// <param name="state">The cancellation completion source.</param>
        private static void CompleteCancellation(object? state)
        {
            if (state is not TaskCompletionSource completion)
            {
                return;
            }

            _ = completion.TrySetResult();
        }

        /// <summary>Throws from the cancellation callback.</summary>
        /// <param name="state">The unused callback state.</param>
        /// <exception cref="InvalidOperationException">Always thrown by this test callback.</exception>
        private static void ThrowCancellation(object? state) =>
            throw new InvalidOperationException(CallbackFailureReason);
    }

    /// <summary>Blocks acknowledgement authorization until released by the test.</summary>
    private sealed class BlockingAcknowledgePolicy : IServerStreamAuthorizationPolicy
    {
        /// <summary>Tracks when acknowledgement authorization starts.</summary>
        private readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Releases acknowledgement authorization.</summary>
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Tracks when disposal cancellation reaches acknowledgement authorization.</summary>
        private readonly TaskCompletionSource _canceled = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
            ServerAuthenticatedClient client,
            SyncBatch batch,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
            ServerAuthenticatedClient client,
            SyncOperation operation,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
            ServerAuthenticatedClient client,
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, client.ClientId));

        /// <inheritdoc/>
        public async ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
            ServerAuthenticatedClient client,
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken)
        {
            _ = _started.TrySetResult();
            await using var registration = cancellationToken.UnsafeRegister(CompleteCancellation, _canceled);
            await _released.Task.ConfigureAwait(false);
            cancellationToken.ThrowIfCancellationRequested();
            return new(Tenant, client.ClientId);
        }

        /// <summary>Releases the blocking acknowledgement authorization.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Release() => _ = _released.TrySetResult();

        /// <summary>Waits until acknowledgement authorization has observed cancellation.</summary>
        /// <returns>The wait task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task WaitUntilCanceledAsync() => _canceled.Task;

        /// <summary>Waits until acknowledgement authorization has started.</summary>
        /// <returns>The wait task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task WaitUntilStartedAsync() => _started.Task;

        /// <summary>Completes the cancellation signal.</summary>
        /// <param name="state">The cancellation completion source.</param>
        private static void CompleteCancellation(object? state)
        {
            if (state is not TaskCompletionSource completion)
            {
                return;
            }

            _ = completion.TrySetResult();
        }
    }

    /// <summary>Returns a different tenant for operation authorization than for batch authorization.</summary>
    private sealed class OperationTenantMismatchPolicy : IServerStreamAuthorizationPolicy
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizePublishAsync(
            ServerAuthenticatedClient client,
            SyncBatch batch,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeOperationAsync(
            ServerAuthenticatedClient client,
            SyncOperation operation,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(OtherTenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeSubscribeAsync(
            ServerAuthenticatedClient client,
            RemoteSubscribeRequest request,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, client.ClientId));

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerStreamAuthorizationScope> AuthorizeAcknowledgeAsync(
            ServerAuthenticatedClient client,
            ReceiveAcknowledgement acknowledgement,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerStreamAuthorizationScope(Tenant, client.ClientId));
    }
}
