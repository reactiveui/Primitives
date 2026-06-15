// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive;
#else
namespace ReactiveUI.Primitives.Async;
#endif

/// <summary>Provides shim-typed observable-conversion operators for tasks that signal completion only.</summary>
public static partial class SignalAsyncReactiveExtensions
{
    /// <summary>Observable-conversion operators for a task that signals completion only.</summary>
    /// <param name="this">The task to be observed. Cannot be null.</param>
    extension(Task @this)
    {
        /// <summary>Converts the specified task into an asynchronous observable sequence that signals completion when the task finishes.</summary>
        /// <remarks>The returned observable emits a single unit value upon task completion and then signals
        /// completion. If the task is canceled or fails, the observable will propagate the corresponding error. This method
        /// is useful for integrating task-based operations into observable workflows.</remarks>
        /// <returns>An asynchronous observable sequence that emits a single value when the task completes successfully, followed by
        /// a completion notification.</returns>
        [SuppressMessage(
            "Roslynator",
            "RCS1047:Non-asynchronous method name should not end with \'Async\'",
            Justification = "This is an existing method")]
        public IObservableAsync<RxVoid> ToAsyncSignal() => SignalAsync.CreateAsBackgroundJob<RxVoid>(
            async (obs, cancellationToken) =>
            {
                await @this.WaitAsync(System.Threading.Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
                await obs.OnNextAsync(RxVoid.Default, cancellationToken).ConfigureAwait(false);
                await obs.OnCompletedAsync(Result.Success).ConfigureAwait(false);
            },
            true);
    }
}
