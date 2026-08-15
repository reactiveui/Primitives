// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides rescue aliases for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Rescue operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Recovers from a terminal failure with a replacement sequence.</summary>
        /// <param name="handler">The handler that produces a replacement sequence from the error.</param>
        /// <returns>An observable sequence that recovers from failures.</returns>
        public IObservableAsync<T> Rescue(Func<Exception, IObservableAsync<T>> handler)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(handler);

            return new CatchSignal<T>(source, handler, null);
        }
    }
}
