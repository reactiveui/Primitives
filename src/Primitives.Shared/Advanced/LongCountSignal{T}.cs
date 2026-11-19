// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Emits the number of source values as a 64-bit count.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("LongCountSignal: Source = {_source}")]
public sealed class LongCountSignal<T> : IRequireCurrentThread<long>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>Initializes a new instance of the <see cref="LongCountSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public LongCountSignal(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        _source = source;
    }

    /// <summary>Creates a 64-bit count sequence, fusing a preceding <see cref="FusedKeepSignal{T}"/> into <see cref="LongCountPredicateSignal{T}"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>The 64-bit count observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<long> Create(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return source is FusedKeepSignal<T> keep
            ? new LongCountPredicateSignal<T>(keep.Source, keep.Predicate)
            : new LongCountSignal<T>(source);
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<long> observer)
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
            return countSource.SubscribeLongCount(observer);
        }

        AggregateWitness<T, long, LongCountAggregator<T>> sink = new(observer, default);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
