// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async;

/// <summary>Adapts a signal to an observer, and projects a signal's value sequence without changing where values are published.</summary>
public static class SignalExtensions
{
    /// <summary>Observer-wrapping and value-mapping operators for a signal source.</summary>
    /// <typeparam name="T">The type of the elements processed by the signal.</typeparam>
    /// <param name="source">The signal to wrap as an asynchronous observer. Cannot be null.</param>
    extension<T>(ISignalAsync<T> source)
    {
        /// <summary>Creates an asynchronous observer wrapper for the specified signal.</summary>
        /// <returns>An asynchronous observer that forwards notifications to the specified signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObserverAsync<T> AsObserverAsync() =>
            new SignalAsyncWitness<T>(source);

        /// <summary>Creates a signal whose observers see <paramref name="mapper"/> applied to the source's value sequence.</summary>
        /// <param name="mapper">Projects the source's value sequence into the sequence observers receive.</param>
        /// <returns>A signal that publishes into <paramref name="source"/> but exposes the mapped sequence to its own
        /// subscribers.</returns>
        /// <remarks><paramref name="mapper"/> runs once, against the source's value sequence, rather than per
        /// subscriber.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ISignalAsync<T> MapValues(Func<IObservableAsync<T>, IObservableAsync<T>> mapper) =>
            new MappedSignal<T>(source, mapper);
    }
}
