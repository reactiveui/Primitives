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
/// <param name="sources">The sources to merge.</param>
/// <param name="maxConcurrent">The maximum number of active inner subscriptions.</param>
[System.Diagnostics.DebuggerDisplay("MaxConcurrentEnumerableBlendSignal: Sources = {_sources}, MaxConcurrent = {_maxConcurrent}")]
public sealed class MaxConcurrentEnumerableBlendSignal<T>(IEnumerable<IObservable<T>> sources, int maxConcurrent) : IObservable<T>
{
    /// <summary>The sources to merge.</summary>
    private readonly IEnumerable<IObservable<T>> _sources = sources;

    /// <summary>The maximum number of active inner subscriptions.</summary>
    private readonly int _maxConcurrent = maxConcurrent;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return new MaxConcurrentBlendCoordinator<T>(observer).Run(_sources, _maxConcurrent);
    }
}
