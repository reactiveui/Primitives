// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Rx-compatible parity helper names for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Null-filtering parity helper operators for a nullable reference-type observable source sequence.</summary>
    /// <param name="source">The source sequence.</param>
    /// <typeparam name="T">The reference type element.</typeparam>
    extension<T>(IObservableAsync<T?> source)
        where T : class
    {
        /// <summary>Keeps non-null reference values.</summary>
        /// <returns>An observable sequence of non-null values.</returns>
        public IObservableAsync<T> KeepNotNull()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new WhereIsNotNullSignal<T>(source);
        }
    }
}
