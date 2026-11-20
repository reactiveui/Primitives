// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides static methods for creating and manipulating asynchronous observable sequences.</summary>
public static partial class SignalAsync
{
    /// <summary>Creates an observable sequence that executes the supplied function and emits its result.</summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="function">The function to execute.</param>
    /// <returns>An observable sequence that emits the function result and then completes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<TResult> Start<TResult>(Func<TResult> function) =>
        new StartSignal<TResult>(function);

    /// <summary>Creates an observable sequence that executes the supplied function and emits its result.</summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="function">The function to execute.</param>
    /// <param name="taskScheduler">An optional scheduler that runs the function.</param>
    /// <returns>An observable sequence that emits the function result and then completes.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservableAsync<TResult> Start<TResult>(Func<TResult> function, TaskScheduler? taskScheduler) =>
        new StartSignal<TResult>(function, taskScheduler);
}
