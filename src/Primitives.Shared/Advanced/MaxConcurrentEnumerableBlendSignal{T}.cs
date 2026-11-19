// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Enumerable <c>Blend</c> signal with bounded concurrency.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("MaxConcurrentEnumerableBlendSignal: Sources = {_sources}, MaxConcurrent = {_maxConcurrent}")]
public sealed class MaxConcurrentEnumerableBlendSignal<T> : IObservable<T>
{
    /// <summary>The sources to merge.</summary>
    private readonly IEnumerable<IObservable<T>> _sources;

    /// <summary>The maximum number of active inner subscriptions.</summary>
    private readonly int _maxConcurrent;

    /// <summary>Initializes a new instance of the <see cref="MaxConcurrentEnumerableBlendSignal{T}"/> class.</summary>
    /// <param name="sources">The sources to merge.</param>
    /// <param name="maxConcurrent">The maximum number of active inner subscriptions.</param>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxConcurrent"/> is zero or negative.</exception>
    public MaxConcurrentEnumerableBlendSignal(IEnumerable<IObservable<T>> sources, int maxConcurrent)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(maxConcurrent);

        _sources = sources;
        _maxConcurrent = maxConcurrent;
    }

    /// <summary>Creates a bounded-concurrency blend signal, simplifying to <see cref="EnumerableBlendSignal{T}"/> when <paramref name="maxConcurrent"/> is <see cref="int.MaxValue"/>.</summary>
    /// <param name="sources">The sources to merge.</param>
    /// <param name="maxConcurrent">The maximum number of active inner subscriptions.</param>
    /// <returns>The merged observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="sources"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxConcurrent"/> is zero or negative.</exception>
    public static IObservable<T> Create(IEnumerable<IObservable<T>> sources, int maxConcurrent)
    {
        ArgumentExceptionHelper.ThrowIfNull(sources);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(maxConcurrent);

        return maxConcurrent == int.MaxValue
            ? new EnumerableBlendSignal<T>(sources)
            : new MaxConcurrentEnumerableBlendSignal<T>(sources, maxConcurrent);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return new MaxConcurrentBlendCoordinator<T>(observer).Run(_sources, _maxConcurrent);
    }
}
