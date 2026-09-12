// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
using TaskToAsyncSignal = ReactiveUI.Primitives.Async.Reactive.Advanced.TaskToAsyncSignal;
#else
using TaskToAsyncSignal = ReactiveUI.Primitives.Async.Advanced.TaskToAsyncSignal;
#endif

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive;
#else
namespace ReactiveUI.Primitives.Async;
#endif

/// <summary>Provides shim-typed observable-conversion operators for tasks that signal completion only.</summary>
public static partial class SignalAsyncReactiveExtensions
{
    /// <summary>Observable-conversion operators for a task that signals completion only.</summary>
    /// <param name="task">The task to be observed. Cannot be null.</param>
    extension(Task task)
    {
        /// <summary>Converts the specified task into an asynchronous observable sequence that signals completion when the task finishes.</summary>
        /// <returns>An asynchronous observable sequence that emits a single value when the task completes successfully, followed by
        /// a completion notification.</returns>
        /// <remarks>Task failure or cancellation terminates the sequence with the corresponding error.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        [SuppressMessage(
            "Roslynator",
            "RCS1047:Non-asynchronous method name should not end with \'Async\'",
            Justification = "Names the asynchronous observable conversion.")]
        public IObservableAsync<RxVoid> ToAsyncSignal() => new TaskToAsyncSignal(task);
    }
}
