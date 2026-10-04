// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Handles portable HTTP server requests for occasionally connected synchronization.</summary>
public sealed partial class HttpServerEndpoint
{
    /// <summary>Owns the finite long-poll deadline token and timer callback completion.</summary>
    /// <param name="source">The endpoint-owned linked poll cancellation source.</param>
    private sealed class PollDeadline(CancellationTokenSource source) : IAsyncDisposable
    {
        /// <summary>The stable diagnostic reason code used when startup cleanup also fails.</summary>
        private const string StartupCleanupFailureReasonCode = "StartupCleanupFailed";

        /// <summary>The exception data key used for the cleanup failure reason code.</summary>
        private const string StartupCleanupFailureReasonKey =
            "ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.PollDeadline.StartupCleanupFailureReason";

        /// <summary>The exception data key used for the cleanup failure type.</summary>
        private const string StartupCleanupFailureTypeKey =
            "ReactiveUI.Primitives.OccasionallyConnected.Transport.Http.PollDeadline.StartupCleanupFailureType";

        /// <summary>The signal completed by the timer callback or by disposal before the timer fires.</summary>
        private readonly TaskCompletionSource<bool> _deadlineRequested = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The endpoint-owned linked poll cancellation source.</summary>
        private readonly CancellationTokenSource _source = source;

        /// <summary>The owned asynchronous cancellation task.</summary>
        private Task _cancellationTask = Task.CompletedTask;

        /// <summary>The endpoint-owned deadline timer when timer creation completed.</summary>
        private ITimer? _timer;

        /// <summary>The cancellation callback failure captured by this deadline.</summary>
        private Exception? _callbackFailure;

        /// <summary>Gets the token canceled by caller cancellation, endpoint disposal, or the finite poll deadline.</summary>
        public CancellationToken Token => _source.Token;

        /// <summary>Creates and starts an endpoint-owned poll deadline.</summary>
        /// <param name="timeProvider">The endpoint clock.</param>
        /// <param name="timeout">The finite poll deadline.</param>
        /// <param name="requestToken">The request lifetime token.</param>
        /// <returns>The started deadline.</returns>
        /// <exception cref="InvalidOperationException">The poll deadline timer cannot be armed.</exception>
        public static async ValueTask<PollDeadline> StartAsync(TimeProvider timeProvider, TimeSpan timeout, CancellationToken requestToken)
        {
            var source = CancellationTokenSource.CreateLinkedTokenSource(requestToken);
            var deadline = new PollDeadline(source);
            Exception? startupFailure = null;
            deadline.StartCancellationTask();
            try
            {
                var timer = timeProvider.CreateTimer(deadline.Cancel, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
                deadline._timer = timer;
                if (!timer.Change(timeout, Timeout.InfiniteTimeSpan))
                {
                    throw new InvalidOperationException("The poll deadline timer could not be armed.");
                }
            }
            catch (Exception exception)
            {
                startupFailure = exception;
            }

            if (startupFailure is not null)
            {
                Exception? cleanupFailure = null;
                try
                {
                    await deadline.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception disposeException)
                {
                    cleanupFailure = disposeException;
                }

                if (cleanupFailure is not null)
                {
                    _ = TryPreserveStartupCleanupFailure(startupFailure, cleanupFailure);
                }

                return await Task.FromException<PollDeadline>(startupFailure).ConfigureAwait(false);
            }

            return deadline;
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            Exception? failure = null;
            if (_timer is not null)
            {
                try
                {
                    await _timer.DisposeAsync().ConfigureAwait(false);
                }
                catch (Exception exception)
                {
                    failure = exception;
                }
            }

            _ = _deadlineRequested.TrySetResult(false);
            await _cancellationTask.ConfigureAwait(false);
            failure = PreserveFailure(failure, _callbackFailure);
            _source.Dispose();
            if (failure is not null)
            {
                ExceptionDispatchInfo.Capture(failure).Throw();
            }
        }

        /// <summary>Tries to preserve a startup cleanup failure on the primary startup exception.</summary>
        /// <param name="startupFailure">The primary startup failure.</param>
        /// <param name="cleanupFailure">The cleanup failure.</param>
        /// <returns><see langword="true"/> when the diagnostic metadata was attached.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool TryPreserveStartupCleanupFailure(Exception startupFailure, Exception cleanupFailure)
        {
            try
            {
                var cleanupFailureType = cleanupFailure.GetType();
                startupFailure.Data[StartupCleanupFailureReasonKey] = StartupCleanupFailureReasonCode;
                startupFailure.Data[StartupCleanupFailureTypeKey] = cleanupFailureType.Name;
                return true;
            }
            catch (Exception)
            {
                return false;
            }
        }

        /// <summary>Requests deadline cancellation from the timer callback without executing cancellation inline.</summary>
        /// <param name="state">The unused timer callback state.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void Cancel(object? state) => _ = _deadlineRequested.TrySetResult(true);

        /// <summary>Starts the owned cancellation task that is drained by deadline disposal.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void StartCancellationTask() => _cancellationTask = RunCancellationAsync();

        /// <summary>Cancels the poll source asynchronously while capturing callback failures inside the deadline owner.</summary>
        /// <returns>The asynchronous cancellation operation.</returns>
        private async Task RunCancellationAsync()
        {
            var shouldCancel = await _deadlineRequested.Task.ConfigureAwait(false);
            if (!shouldCancel)
            {
                return;
            }

            try
            {
                await _source.CancelAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                _callbackFailure = exception;
            }
        }
    }
}
