// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for working with asynchronous observers.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Wrapping operators for an asynchronous observer.</summary>
    /// <param name="observer">The asynchronous observer to wrap.</param>
    /// <typeparam name="T">The type of the elements observed by the asynchronous observer.</typeparam>
    extension<T>(IObserverAsync<T> observer)
    {
        /// <summary>Wraps the specified asynchronous observer in a decorator that ensures consistent behavior and interface compliance.</summary>
        /// <returns>A wrapped asynchronous observer that delegates calls to the specified observer.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="observer"/> is null.</exception>
        public IObserverAsync<T> Wrap() => observer is null
            ? throw new ArgumentNullException(nameof(observer))
            : new RelayWitnessAsync<T>(observer);
    }
}
