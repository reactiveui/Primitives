// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.Tests;

/// <summary>Manual timer helpers for <see cref="HttpServerEndpointTests"/>.</summary>
public sealed partial class HttpServerEndpointTests
{
    /// <summary>Provides deterministic timer creation for deadline tests.</summary>
    private sealed class ManualTimeProvider : TimeProvider
    {
        /// <summary>The signal completed when the endpoint creates a timer.</summary>
        private readonly TaskCompletionSource<ManualTimer> _created = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = new ManualTimer(callback, state);
            _ = _created.TrySetResult(timer);
            return timer;
        }

        /// <summary>Waits until the endpoint creates its poll deadline timer.</summary>
        /// <returns>The created timer.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<ManualTimer> WaitForTimerAsync() =>
            _created.Task.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds));
    }

    /// <summary>Captures manual timer callback and disposal behavior.</summary>
    /// <param name="callback">The timer callback.</param>
    /// <param name="state">The callback state.</param>
    private sealed class ManualTimer(TimerCallback callback, object? state) : ITimer
    {
        /// <summary>The callback supplied by the endpoint.</summary>
        private readonly TimerCallback _callback = callback;

        /// <summary>The signal that allows a held callback to complete.</summary>
        private readonly TaskCompletionSource<object?> _callbackCanComplete = CreateSignal();

        /// <summary>The signal completed after a manual fire starts callback invocation.</summary>
        private readonly TaskCompletionSource<object?> _callbackStarted = CreateSignal();

        /// <summary>The signal completed when asynchronous disposal starts.</summary>
        private readonly TaskCompletionSource<object?> _disposeAsyncStarted = CreateSignal();

        /// <summary>The callback state supplied by the endpoint.</summary>
        private readonly object? _state = state;

        /// <summary>The in-flight callback completion published before invoking callback code.</summary>
        private Task? _callbackTask;

        /// <summary>The callback runner observed by timer disposal.</summary>
        private Task? _callbackRunner;

        /// <summary>Gets the callback exception captured by the manual timer.</summary>
        public Exception? CallbackException { get; private set; }

        /// <summary>Gets a task completed when asynchronous disposal starts.</summary>
        public Task DisposeAsyncStarted => _disposeAsyncStarted.Task;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Change(TimeSpan dueTime, TimeSpan period) => true;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => ReleaseCallback();

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => new(DrainCallbackAsync());

        /// <summary>Fires the timer and waits until the callback is complete.</summary>
        /// <returns>The asynchronous fire operation.</returns>
        public async Task FireAsync()
        {
            var callbackTask = StartCallback(holdCallback: false);
            await callbackTask.ConfigureAwait(false);
        }

        /// <summary>Fires the timer and keeps its callback in flight after invoking endpoint code.</summary>
        /// <returns>The asynchronous fire operation.</returns>
        public async Task FireAndHoldCallbackAsync()
        {
            var callbackTask = StartCallback(holdCallback: true);
            await _callbackStarted.Task.WaitAsync(TimeSpan.FromSeconds(AsyncWaitTimeoutSeconds)).ConfigureAwait(false);
            if (!callbackTask.IsFaulted && !callbackTask.IsCanceled)
            {
                return;
            }

            await callbackTask.ConfigureAwait(false);
        }

        /// <summary>Releases a held callback.</summary>
        /// <returns><see langword="true"/> when the callback was released by this call.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool ReleaseCallback() => _callbackCanComplete.TrySetResult(null);

        /// <summary>Starts the callback after publishing the completion task drained by disposal.</summary>
        /// <param name="holdCallback">Whether the callback remains in flight until released.</param>
        /// <returns>The published callback completion task.</returns>
        private Task<object?> StartCallback(bool holdCallback)
        {
            var callbackCompletion = CreateSignal();
            _callbackTask = callbackCompletion.Task;
            _callbackRunner = CompleteCallbackAsync(holdCallback, callbackCompletion);
            return callbackCompletion.Task;
        }

        /// <summary>Signals asynchronous disposal and waits for an in-flight callback to complete.</summary>
        /// <returns>The asynchronous drain operation.</returns>
        private async Task DrainCallbackAsync()
        {
            _ = _disposeAsyncStarted.TrySetResult(null);
            var callbackTask = _callbackTask;
            if (callbackTask is null)
            {
                return;
            }

            try
            {
                await callbackTask.ConfigureAwait(false);
            }
            finally
            {
                var callbackRunner = _callbackRunner;
                if (callbackRunner is not null)
                {
                    await callbackRunner.ConfigureAwait(false);
                }
            }
        }

        /// <summary>Completes the published callback task after the callback lifetime ends.</summary>
        /// <param name="holdCallback">Whether the callback remains in flight until released.</param>
        /// <param name="callbackCompletion">The published callback completion source.</param>
        /// <returns>The asynchronous completion operation.</returns>
        private async Task CompleteCallbackAsync(bool holdCallback, TaskCompletionSource<object?> callbackCompletion)
        {
            try
            {
                await RunCallbackAsync(holdCallback).ConfigureAwait(false);
                _ = callbackCompletion.TrySetResult(null);
            }
            catch (Exception exception)
            {
                _ = callbackCompletion.TrySetException(exception);
            }
        }

        /// <summary>Runs the timer callback, optionally holding it in flight after endpoint callback code returns.</summary>
        /// <param name="holdCallback">Whether the callback remains in flight until released.</param>
        /// <returns>The asynchronous callback operation.</returns>
        private async Task RunCallbackAsync(bool holdCallback)
        {
            _ = _callbackStarted.TrySetResult(null);
            try
            {
                _callback(_state);
            }
            catch (Exception exception)
            {
                CallbackException = exception;
            }

            if (!holdCallback)
            {
                return;
            }

            await _callbackCanComplete.Task.ConfigureAwait(false);
        }
    }
}
