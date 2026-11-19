// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Dedicated signal for <c>Take</c>.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("TakeSignal: Source = {_source}")]
public sealed class TakeSignal<T> : IObservable<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The maximum number of values to forward.</summary>
    private readonly int _count;

    /// <summary>Initializes a new instance of the <see cref="TakeSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="count">The maximum number of values to forward.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
    public TakeSignal(IObservable<T> source, int count)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(count);

        _source = source;
        _count = count;
    }

    /// <summary>Gets the source observable.</summary>
    internal IObservable<T> Source => _source;

    /// <summary>Gets the maximum number of values to forward.</summary>
    internal int Count => _count;

    /// <summary>Applies <c>Take</c> reductions and fusions.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="count">The maximum number of values to emit.</param>
    /// <returns>The reduced or fused observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
    public static IObservable<T> Create(IObservable<T> source, int count)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(count);

        if (count == 0)
        {
            return ImmutableEmptySignal<T>.Instance;
        }

        return source switch
        {
            ImmutableEmptySignal<T> or ImmutableNeverSignal<T> => source,
            LoopSignal<T> loop => new RepeatSignal<T>(loop.Value, count),
            RangeSignal range when typeof(T) == typeof(int) =>
                count >= range.Count ? source : Unsafe.As<IObservable<T>>(new RangeSignal(range.Start, count)),
            TakeSignal<T> prev =>
                count >= prev.Count ? source : new TakeSignal<T>(prev.Source, count),
            _ => new TakeSignal<T>(source, count),
        };
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (_count == 0)
        {
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        if (_source is FromEnumerableSignal<T> enumerable && enumerable.TryGetReadOnlyValues(out var values))
        {
            EmitReadOnlyList(values, _count, observer);
            return EmptyDisposable.Instance;
        }

        return !CurrentThreadRequirement.IsRequired(_source) || !CurrentThreadSequencer.IsScheduleRequired
            ? SubscribeCore(observer)
            : CurrentThreadSequencer.RunTrampoline(
                (Self: this, observer),
                static s => s.Self.SubscribeCore(s.observer));
    }

    /// <summary>Emits at most <paramref name="maxCount"/> values from an indexable synchronous list.</summary>
    /// <param name="values">The indexable values.</param>
    /// <param name="maxCount">The maximum number of values to emit.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitReadOnlyList(IReadOnlyList<T> values, int maxCount, IObserver<T> observer)
    {
        var limit = Math.Min(values.Count, maxCount);
        for (var i = 0; i < limit; i++)
        {
            observer.OnNext(values[i]);
        }

        observer.OnCompleted();
    }

    /// <summary>Subscribes the counting sink to the source.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The sink that owns the upstream subscription.</returns>
    private TakeWitness<T> SubscribeCore(IObserver<T> observer)
    {
        TakeWitness<T> sink = new(observer, _count);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
