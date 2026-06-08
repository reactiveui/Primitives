// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions;

/// <summary>Extension methods for <see cref="IObserver{T}"/>.</summary>
public static class ObserverExtensions
{
    /// <summary>Fast-path iteration helpers for emitting collections to an observer.</summary>
    /// <typeparam name="T">The type of elements in the source collection.</typeparam>
    /// <param name="observer">The observer to emit elements to.</param>
    extension<T>(IObserver<T> observer)
    {
        /// <summary>
        /// Emits each element from <paramref name="source"/> to the observer
        /// via <see cref="IObserver{T}.OnNext(T)"/> using a fast-path iteration for known
        /// concrete collection types (<see cref="List{T}"/>, arrays, <see cref="IList{T}"/>).
        /// Avoids the per-iteration enumerator allocation that <c>foreach</c> over a bare
        /// <see cref="IEnumerable{T}"/> would incur on these common shapes.
        /// </summary>
        /// <param name="source">The source collection to iterate.</param>
        public void FastForEach(IEnumerable<T> source)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);
            ArgumentExceptionHelper.ThrowIfNull(source);

            if (source is T[] array)
            {
                for (var i = 0; i < array.Length; i++)
                {
                    observer.OnNext(array[i]);
                }

                return;
            }

            if (source is List<T> fullList)
            {
                for (var i = 0; i < fullList.Count; i++)
                {
                    observer.OnNext(fullList[i]);
                }

                return;
            }

            if (source is IList<T> list)
            {
                for (var i = 0; i < list.Count; i++)
                {
                    observer.OnNext(list[i]);
                }

                return;
            }

            if (source is IReadOnlyList<T> readOnlyList)
            {
                for (var i = 0; i < readOnlyList.Count; i++)
                {
                    observer.OnNext(readOnlyList[i]);
                }

                return;
            }

            foreach (var item in source)
            {
                observer.OnNext(item);
            }
        }
    }
}
