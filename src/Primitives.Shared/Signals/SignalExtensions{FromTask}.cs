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
    /// <typeparam name="TResult">The source element type.</typeparam>
    /// <param name="asyncTask">The asynchronous task.</param>
    extension<TResult>(IObservable<TResult> asyncTask)
    {
        /// <summary>Awaits the source and returns its final value, or the default value when the token cancels the wait.</summary>
        /// <param name="token">The token that cancels the wait.</param>
        /// <returns>A task carrying the final source value, or the default value on cancellation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<TResult?> HandleCancellation(CancellationToken token) =>
            TaskFaultObservation.HandleCancellationAsync(asyncTask, token);

        /// <summary>Awaits the source and returns its final value; on cancellation invokes the action and returns the default value.</summary>
        /// <param name="action">Invoked when the wait is cancelled.</param>
        /// <param name="token">The token that cancels the wait.</param>
        /// <returns>A task carrying the final source value, or the default value on cancellation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<TResult?> HandleCancellation(Action? action, CancellationToken token) =>
            TaskFaultObservation.HandleCancellationAsync(asyncTask, action, token);
    }

    /// <summary>Cancellation-handling operators for a task.</summary>
    /// <param name="asyncTask">The asynchronous task.</param>
    extension(Task asyncTask)
    {
        /// <summary>Awaits the task and completes normally when it is cancelled.</summary>
        /// <returns>A task that completes once the awaited task finishes or is cancelled.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task HandleCancellation() => TaskFaultObservation.HandleCancellationAsync(asyncTask);

        /// <summary>Awaits the task and invokes the action instead of throwing when it is cancelled.</summary>
        /// <param name="action">Invoked when the task is cancelled.</param>
        /// <returns>A task that completes once the awaited task finishes or is cancelled.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task HandleCancellation(Action? action) =>
            TaskFaultObservation.HandleCancellationAsync(asyncTask, action);
    }

    /// <summary>Cancellation-handling operators for a result-producing task.</summary>
    /// <typeparam name="TResult">The type of the result.</typeparam>
    /// <param name="asyncTask">The asynchronous task.</param>
    extension<TResult>(Task<TResult> asyncTask)
    {
        /// <summary>Awaits the task and returns its result, or the default value when it is cancelled.</summary>
        /// <returns>A task carrying the result, or the default value on cancellation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<TResult?> HandleCancellation() => TaskFaultObservation.HandleCancellationAsync(asyncTask);

        /// <summary>Awaits the task and returns its result; on cancellation invokes the action and returns the default value.</summary>
        /// <param name="action">Invoked when the task is cancelled.</param>
        /// <returns>A task carrying the result, or the default value on cancellation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task<TResult?> HandleCancellation(Action? action) =>
            TaskFaultObservation.HandleCancellationAsync(asyncTask, action);

        /// <summary>Awaits whichever completes first, the task or <paramref name="cancellationToken"/>, and reports which one won.</summary>
        /// <param name="cancellationToken">The token raced against the task.</param>
        /// <returns>The task result and whether cancellation won.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal Task<(TResult Value, bool IsCanceled)> WhenCancelled(CancellationToken cancellationToken) =>
            TaskFaultObservation.WhenCancelledAsync(asyncTask, cancellationToken);
    }
}
