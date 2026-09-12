// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>Task and observable cancellation extension operators.</summary>
public static partial class SignalExtensions
{
    /// <summary>Cancellation-handling operators for an observable source sequence.</summary>
    /// <typeparam name="TResult">The type.</typeparam>
    /// <param name="asyncTask">The asynchronous task.</param>
    extension<TResult>(IObservable<TResult> asyncTask)
    {
        /// <summary>Awaits the source and returns its final value, or the default value when the token cancels the wait.</summary>
        /// <param name="token">The token that cancels the wait.</param>
        /// <returns>A task carrying the final source value, or the default value on cancellation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<TResult?> HandleCancellation(CancellationToken token) =>
            asyncTask.HandleCancellation(null, token);

        /// <summary>Awaits the source and returns its final value; on cancellation invokes the action and returns the default value.</summary>
        /// <param name="action">Invoked when the wait is cancelled.</param>
        /// <param name="token">The token that cancels the wait.</param>
        /// <returns>A task carrying the final source value, or the default value on cancellation.</returns>
        public async Task<TResult?> HandleCancellation(Action? action, CancellationToken token)
        {
            try
            {
                token.ThrowIfCancellationRequested();
                return await Task.Run(async () => await asyncTask, token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                action?.Invoke();
            }

            return default;
        }
    }

    /// <summary>Cancellation-handling operators for a task.</summary>
    /// <param name="asyncTask">The asynchronous task.</param>
    extension(Task asyncTask)
    {
        /// <summary>Awaits the task and completes normally when it is cancelled.</summary>
        /// <returns>A task that completes once the awaited task finishes or is cancelled.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task HandleCancellation() => asyncTask.HandleCancellation(null);

        /// <summary>Awaits the task and invokes the action instead of throwing when it is cancelled.</summary>
        /// <param name="action">Invoked when the task is cancelled.</param>
        /// <returns>A task that completes once the awaited task finishes or is cancelled.</returns>
        public async Task HandleCancellation(Action? action)
        {
            try
            {
                await asyncTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                action?.Invoke();
            }
        }
    }

    /// <summary>Cancellation-handling operators for a result-producing task.</summary>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="asyncTask">The asynchronous task.</param>
    extension<TResult>(Task<TResult> asyncTask)
    {
        /// <summary>Awaits the task and returns its result, or the default value when it is cancelled.</summary>
        /// <returns>A task carrying the result, or the default value on cancellation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<TResult?> HandleCancellation() => asyncTask.HandleCancellation(null);

        /// <summary>Awaits the task and returns its result; on cancellation invokes the action and returns the default value.</summary>
        /// <param name="action">Invoked when the task is cancelled.</param>
        /// <returns>A task carrying the result, or the default value on cancellation.</returns>
        public async Task<TResult?> HandleCancellation(Action? action)
        {
            try
            {
                return await asyncTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                action?.Invoke();
            }

            return default;
        }

        /// <summary>Awaits whichever completes first, the task or the token, and reports which one it was.</summary>
        /// <param name="cancellationToken">The token raced against the task.</param>
        /// <returns>The task result, and whether the wait ended in cancellation.</returns>
        internal async Task<(TResult Value, bool IsCanceled)> WhenCancelled(CancellationToken cancellationToken)
        {
            TaskCompletionSource<TResult> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            var registration = cancellationToken.UnsafeRegister(
                static (state, token) => ((TaskCompletionSource<TResult>)state!).TrySetCanceled(token),
                tcs);
            var cancellationTask = tcs.Task;

            try
            {
                // Create a task that completes when either the async operation completes,
                // or cancellation is requested.
                var readyTask = await Task.WhenAny(asyncTask, cancellationTask).ConfigureAwait(false);

                // In case of cancellation, register a continuation to observe any unhandled
                // exceptions from the asynchronous operation once it completes.
                if (readyTask == cancellationTask)
                {
                    _ = asyncTask.ContinueWith(
                        static task => _ = task.Exception,
                        CancellationToken.None,
                        TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                        TaskScheduler.Default);
                }

                return (await readyTask.ConfigureAwait(false), tcs.Task.IsCanceled || readyTask.IsCanceled);
            }
            finally
            {
#if NET8_0_OR_GREATER
                await registration.DisposeAsync().ConfigureAwait(false);
#else
                registration.Dispose();
#endif
            }
        }
    }
}
