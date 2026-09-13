// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Domain and time helpers for <see cref="ServerStreamHubTests"/>.</summary>
public sealed partial class ServerStreamHubTests
{
    /// <summary>Creates deterministic initial stream state.</summary>
    private sealed class InitialStateFactory : IServerInitialStateFactory
    {
        /// <summary>The initial state payload value.</summary>
        private const string InitialPayload = "initial";

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerState> CreateInitialStateAsync(
            StreamId streamId,
            CancellationToken cancellationToken) =>
            ValueTask.FromResult(new ServerState(streamId, InitialVersion, Payload(InitialPayload)));
    }

    /// <summary>Creates deterministic increasing server versions.</summary>
    private sealed class IncrementingVersionFactory : IServerConflictVersionFactory
    {
        /// <summary>The second committed server version used by conflict tests.</summary>
        private const string SecondVersion = "v2";

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public string CreateNextVersion(ConflictContext context, SyncOperation operation) =>
            context.Current.Version == InitialVersion ? FirstVersion : SecondVersion;
    }

    /// <summary>Records domain apply calls and accepts operations.</summary>
    private sealed class RecordingDomainHandler : IServerDomainHandler
    {
        /// <summary>Gets the number of domain calls.</summary>
        internal int CallCount { get; private set; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask<ServerDomainApplyResult> ApplyAsync(
            ServerDomainApplyContext context,
            CancellationToken cancellationToken)
        {
            CallCount++;
            return ValueTask.FromResult(new ServerDomainApplyResult
            {
                NewState = new(context.Operation.StreamId, context.Resolution.ServerVersion, context.Operation.Payload),
                Events = [new() { EventId = context.Operation.OperationId.Value, Payload = context.Operation.Payload }],
            });
        }
    }

    /// <summary>Blocks a domain call until released by the test.</summary>
    private sealed class BlockingDomainHandler : IServerDomainHandler
    {
        /// <summary>Tracks when the blocking call starts.</summary>
        private readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Releases the blocking domain call.</summary>
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        public async ValueTask<ServerDomainApplyResult> ApplyAsync(
            ServerDomainApplyContext context,
            CancellationToken cancellationToken)
        {
            _ = _started.TrySetResult();
            await _released.Task.ConfigureAwait(false);
            return new()
            {
                NewState = new(context.Operation.StreamId, context.Resolution.ServerVersion, context.Operation.Payload),
                Events = [new() { EventId = context.Operation.OperationId.Value, Payload = context.Operation.Payload }],
            };
        }

        /// <summary>Releases the blocking domain handler.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Release() => _ = _released.TrySetResult();

        /// <summary>Waits until the blocking domain call has started.</summary>
        /// <returns>The wait task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task WaitUntilStartedAsync() => _started.Task;
    }

    /// <summary>Accepts the first domain call and blocks subsequent domain calls until released by the test.</summary>
    private sealed class SecondCallBlockingDomainHandler : IServerDomainHandler
    {
        /// <summary>Tracks when a later blocking call starts.</summary>
        private readonly TaskCompletionSource _started = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Releases the blocking domain call.</summary>
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The number of domain calls.</summary>
        private int _calls;

        /// <inheritdoc/>
        public async ValueTask<ServerDomainApplyResult> ApplyAsync(
            ServerDomainApplyContext context,
            CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _calls) > 1)
            {
                _ = _started.TrySetResult();
                await _released.Task.ConfigureAwait(false);
            }

            return new()
            {
                NewState = new(context.Operation.StreamId, context.Resolution.ServerVersion, context.Operation.Payload),
                Events = [new() { EventId = context.Operation.OperationId.Value, Payload = context.Operation.Payload }],
            };
        }

        /// <summary>Releases the blocking domain handler.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Release() => _ = _released.TrySetResult();

        /// <summary>Waits until the blocking domain call has started.</summary>
        /// <returns>The wait task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task WaitUntilStartedAsync() => _started.Task;
    }

    /// <summary>Provides deterministic UTC time.</summary>
    /// <param name="utcNow">The fixed UTC time.</param>
    private sealed class FixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override DateTimeOffset GetUtcNow() => utcNow;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
            new FixedTimer();
    }

    /// <summary>Provides deterministic UTC time and signals when a timer is created.</summary>
    /// <param name="utcNow">The fixed UTC time.</param>
    private sealed class SignalingFixedTimeProvider(DateTimeOffset utcNow) : TimeProvider
    {
        /// <summary>Tracks timer creation.</summary>
        private readonly TaskCompletionSource _timerCreated = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override DateTimeOffset GetUtcNow() => utcNow;

        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            _ = _timerCreated.TrySetResult();
            return new FixedTimer();
        }

        /// <summary>Waits until the empty-poll timer has been created.</summary>
        /// <returns>The wait task.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task WaitUntilTimerCreatedAsync() => _timerCreated.Task;
    }

    /// <summary>Provides deterministic UTC time and cancels the caller after completing a timer.</summary>
    /// <param name="utcNow">The fixed UTC time.</param>
    /// <param name="cancellation">The caller cancellation source.</param>
    private sealed class ImmediateCancelingTimeProvider(DateTimeOffset utcNow, CancellationTokenSource cancellation) : TimeProvider
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override DateTimeOffset GetUtcNow() => utcNow;

        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            callback(state);
            _ = cancellation.CancelAsync();
            return new FixedTimer();
        }
    }

    /// <summary>Timer used by the deterministic test time provider.</summary>
    private sealed class FixedTimer : ITimer
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Change(TimeSpan dueTime, TimeSpan period) => true;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose()
        {
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
