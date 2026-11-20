// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Emits the number of source values that match a predicate.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("CountPredicateSignal: Source = {_source}")]
public sealed class CountPredicateSignal<T> : IRequireCurrentThread<int>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The predicate.</summary>
    private readonly Func<T, bool> _predicate;

    /// <summary>Initializes a new instance of the <see cref="CountPredicateSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="predicate">The predicate.</param>
    /// <exception cref="ArgumentNullException"><paramref name="predicate"/> or <paramref name="source"/> is <see langword="null"/>.</exception>
    public CountPredicateSignal(IObservable<T> source, Func<T, bool> predicate)
    {
        ArgumentExceptionHelper.ThrowIfNull(predicate);
        ArgumentExceptionHelper.ThrowIfNull(source);

        _source = source;
        _predicate = predicate;
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<int> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (_source is RangeSignal range && typeof(T) == typeof(int))
        {
            EmitCountRange(range, _predicate, observer);
            return EmptyDisposable.Instance;
        }

        AggregateWitness<T, int, CountPredicateAggregator<T>> sink = new(observer, new(_predicate));
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <summary>Counts matching range values directly and emits the result.</summary>
    /// <param name="range">The range source.</param>
    /// <param name="predicate">The predicate.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitCountRange(RangeSignal range, Func<T, bool> predicate, IObserver<int> observer)
    {
        try
        {
            var typedPredicate = (Func<int, bool>)(object)predicate;
            var count = 0;
            for (var i = 0; i < range.Count; i++)
            {
                if (typedPredicate(range.Start + i))
                {
                    count = checked(count + 1);
                }
            }

            observer.OnNext(count);
            observer.OnCompleted();
        }
        catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
        {
            observer.OnError(error);
        }
    }
}
