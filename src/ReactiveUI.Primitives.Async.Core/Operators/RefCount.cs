// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Reference-counting operators for a connectable observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the observable sequence.</typeparam>
    /// <param name="source">The connectable observable sequence to ref count. Cannot be null.</param>
    extension<T>(ConnectableSignalAsync<T> source)
    {
        /// <summary>
        /// Returns an observable sequence that connects to the underlying connectable observable when the first observer
        /// subscribes, and disconnects when the last observer unsubscribes.
        /// </summary>
        /// <returns>An observable sequence that stays connected to the source as long as there is at least one subscription.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> RefCount() =>
            new RefCountSignal<T>(source);
    }
}
