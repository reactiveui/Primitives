// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives;

/// <summary>Enumerable source overloads for blend operators.</summary>
public static partial class LinqExtensions
{
    /// <summary>Operators for enumerable observable sources.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="sources">The observable sources.</param>
    extension<T>(IEnumerable<IObservable<T>> sources)
    {
        /// <summary>Concurrently merges the supplied observable sources.</summary>
        /// <returns>An observable that forwards values from every source.</returns>
        public IObservable<T> Blend()
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            return Signal.FromEnumerable(sources).Blend();
        }
    }
}
