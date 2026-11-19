// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Emits the number of source values once the source completes.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("CountSignal: Source = {_source}")]
public sealed class CountSignal<T> : IRequireCurrentThread<int>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>Initializes a new instance of the <see cref="CountSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public CountSignal(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        _source = source;
    }

    /// <summary>Creates a count sequence, fusing a preceding <see cref="FusedKeepSignal{T}"/> into <see cref="CountPredicateSignal{T}"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>The count observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<int> Create(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return source is FusedKeepSignal<T> keep
            ? new CountPredicateSignal<T>(keep.Source, keep.Predicate)
            : new CountSignal<T>(source);
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<int> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (_source is RangeSignal range)
        {
            observer.OnNext(range.Count);
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        if (_source is ICountSource countSource)
        {
            return countSource.SubscribeCount(observer);
        }

        AggregateWitness<T, int, CountAggregator<T>> sink = new(observer, default);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
