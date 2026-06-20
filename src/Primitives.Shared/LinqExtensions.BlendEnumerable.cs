// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

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

            return new EnumerableBlendSignal<T>(sources);
        }

        /// <summary>Concurrently merges the supplied observable sources with a maximum number of active subscriptions.</summary>
        /// <param name="maxConcurrent">The maximum number of sources to subscribe to at the same time.</param>
        /// <returns>An observable that forwards values from every source.</returns>
        public IObservable<T> Blend(int maxConcurrent)
        {
            ArgumentExceptionHelper.ThrowIfNull(sources);

            ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(maxConcurrent);

            return maxConcurrent == int.MaxValue ? sources.Blend() : new MaxConcurrentEnumerableBlendSignal<T>(sources, maxConcurrent);
        }
    }

    /// <summary>Dedicated signal for enumerable <c>Blend</c> sources.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class EnumerableBlendSignal<T> : IObservable<T>
    {
        /// <summary>The sources to merge.</summary>
        private readonly IEnumerable<IObservable<T>> _sources;

        /// <summary>Initializes a new instance of the <see cref="EnumerableBlendSignal{T}"/> class.</summary>
        /// <param name="sources">The sources to merge.</param>
        internal EnumerableBlendSignal(IEnumerable<IObservable<T>> sources) => _sources = sources;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);

            return new BlendCoordinator<T>(observer).Run(_sources);
        }
    }
}
