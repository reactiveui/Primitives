// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>Task and observable cancellation extension operators.</summary>
public static partial class SignalExtensions
{
    /// <summary>Cancellation-handling operators for an observable source sequence.</summary>
    /// <param name="asyncTask">The asynchronous task.</param>
    /// <typeparam name="TResult">The type.</typeparam>
    extension<TResult>(IObservable<TResult> asyncTask)
    {
        /// <summary>Handles the cancellation.</summary>
        /// <param name="token">The token.</param>
        /// <returns>
        /// A Task.
        /// </returns>
        public Task<TResult?> HandleCancellation(CancellationToken token) =>
            asyncTask.HandleCancellation(null, token);

        /// <summary>Handles the cancellation.</summary>
        /// <param name="action">The action.</param>
        /// <param name="token">The token.</param>
        /// <returns>
        /// A Task.
        /// </returns>
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
        /// <summary>Handles the cancellation.</summary>
        /// <returns>A Task.</returns>
        public Task HandleCancellation() => asyncTask.HandleCancellation(null);

        /// <summary>Handles the cancellation.</summary>
        /// <param name="action">The action.</param>
        /// <returns>A Task.</returns>
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
    /// <param name="asyncTask">The asynchronous task.</param>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    extension<TResult>(Task<TResult> asyncTask)
    {
        /// <summary>Handles the cancellation.</summary>
        /// <returns>A Task of TResult.</returns>
        public Task<TResult?> HandleCancellation() => asyncTask.HandleCancellation(null);

        /// <summary>Handles the cancellation.</summary>
        /// <param name="action">The action.</param>
        /// <returns>A Task of TResult.</returns>
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

        /// <summary>Executes the WhenCancelled operation.</summary>
        /// <param name="cancellationToken">The cancellationToken value.</param>
        /// <returns>The result.</returns>
        internal async Task<(TResult Value, bool IsCanceled)> WhenCancelled(CancellationToken cancellationToken)
        {
            TaskCompletionSource<TResult> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            var registration = cancellationToken.Register(
                static state => ((TaskCompletionSource<TResult>)state!).TrySetCanceled(),
                tcs,
                false);
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
