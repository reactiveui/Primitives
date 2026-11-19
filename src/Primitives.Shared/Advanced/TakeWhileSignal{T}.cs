// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Dedicated signal for <c>TakeWhile</c>.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("TakeWhileSignal: Source = {_source}")]
public sealed class TakeWhileSignal<T> : IObservable<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The predicate that determines whether to keep taking values.</summary>
    private readonly Func<T, bool> _predicate;

    /// <summary>Initializes a new instance of the <see cref="TakeWhileSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="predicate">The predicate that determines whether to keep taking values.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public TakeWhileSignal(IObservable<T> source, Func<T, bool> predicate)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(predicate);

        _source = source;
        _predicate = predicate;
    }

    /// <summary>Creates a <c>TakeWhile</c> sequence with empty/never short-circuiting.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="predicate">The predicate that determines whether to keep taking values.</param>
    /// <returns>The bounded observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> source, Func<T, bool> predicate)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(predicate);

        return source is ImmutableEmptySignal<T> or ImmutableNeverSignal<T>
            ? source
            : new TakeWhileSignal<T>(source, predicate);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (typeof(T) == typeof(int) && _source is RangeSignal range)
        {
            EmitRange(range, _predicate, observer);
            return EmptyDisposable.Instance;
        }

        if (_source is FromEnumerableSignal<T> enumerable && enumerable.TryGetReadOnlyValues(out var values))
        {
            EmitReadOnlyList(values, _predicate, observer);
            return EmptyDisposable.Instance;
        }

        return !CurrentThreadRequirement.IsRequired(_source) || !CurrentThreadSequencer.IsScheduleRequired
            ? SubscribeCore(observer)
            : CurrentThreadSequencer.RunTrampoline(
                (Self: this, observer),
                static s => s.Self.SubscribeCore(s.observer));
    }

    /// <summary>Emits leading range values while <paramref name="predicate"/> holds.</summary>
    /// <param name="range">The range source.</param>
    /// <param name="predicate">The taking predicate.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitRange(RangeSignal range, Func<T, bool> predicate, IObserver<T> observer)
    {
        var intPredicate = Unsafe.As<Func<int, bool>>(predicate);
        var intObserver = Unsafe.As<IObserver<int>>(observer);
        var start = range.Start;
        var count = range.Count;
        for (var i = 0; i < count; i++)
        {
            var value = start + i;
            if (!intPredicate(value))
            {
                break;
            }

            intObserver.OnNext(value);
        }

        observer.OnCompleted();
    }

    /// <summary>Emits leading list values while <paramref name="predicate"/> holds.</summary>
    /// <param name="values">The indexable values.</param>
    /// <param name="predicate">The taking predicate.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitReadOnlyList(IReadOnlyList<T> values, Func<T, bool> predicate, IObserver<T> observer)
    {
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (!predicate(value))
            {
                break;
            }

            observer.OnNext(value);
        }

        observer.OnCompleted();
    }

    /// <summary>Subscribes the predicate sink to the source.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The sink that owns the upstream subscription.</returns>
    private TakeWhileWitness<T> SubscribeCore(IObserver<T> observer)
    {
        TakeWhileWitness<T> sink = new(observer, _predicate);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
