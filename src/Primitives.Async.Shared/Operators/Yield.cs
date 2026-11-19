// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
#if REACTIVE_SHIM
using ReactiveUI.Primitives.Async.Reactive.Advanced;

namespace ReactiveUI.Primitives.Async.Reactive;
#else
using ReactiveUI.Primitives.Async.Advanced;

namespace ReactiveUI.Primitives.Async;
#endif

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncReactiveExtensions
{
    /// <summary>Scheduler-yielding operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the observable sequence.</typeparam>
    /// <param name="source">The source observable sequence to yield from.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Returns an observable sequence that yields control to the current thread's scheduler before emitting items from the source sequence.</summary>
        /// <returns>An observable sequence that emits the same elements as the source, but yields control to the scheduler before
        /// each emission.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Yield() => new YieldSignal<T>(source);
    }
}
