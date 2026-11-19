// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Dedicated signal for <c>Skip</c>.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("SkipSignal: Source = {_source}")]
public sealed class SkipSignal<T> : IObservable<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The number of leading values to drop.</summary>
    private readonly int _count;

    /// <summary>Initializes a new instance of the <see cref="SkipSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="count">The number of leading values to drop.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
    public SkipSignal(IObservable<T> source, int count)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(count);

        _source = source;
        _count = count;
    }

    /// <summary>Gets the source observable.</summary>
    internal IObservable<T> Source => _source;

    /// <summary>Gets the number of leading values to drop.</summary>
    internal int Count => _count;

    /// <summary>Applies <c>Skip</c> reductions and fusions.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="count">The number of leading values to skip.</param>
    /// <returns>The reduced or fused observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is negative.</exception>
    public static IObservable<T> Create(IObservable<T> source, int count)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(count);

        if (count == 0)
        {
            return source;
        }

        return source switch
        {
            ImmutableEmptySignal<T> or ImmutableNeverSignal<T> => source,
            RangeSignal range when typeof(T) == typeof(int) =>
                count >= range.Count
                    ? ImmutableEmptySignal<T>.Instance
                    : Unsafe.As<IObservable<T>>(new RangeSignal(range.Start + count, range.Count - count)),
            SkipSignal<T> prev =>
                new SkipSignal<T>(prev.Source, prev.Count >= int.MaxValue - count ? int.MaxValue : prev.Count + count),
            _ => new SkipSignal<T>(source, count),
        };
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (_source is FromEnumerableSignal<T> enumerable && enumerable.TryGetReadOnlyValues(out var values))
        {
            EmitReadOnlyList(values, _count, observer);
            return EmptyDisposable.Instance;
        }

        SkipWitness<T> sink = new(observer, _count);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <summary>Skips <paramref name="skipCount"/> values from an indexable synchronous list and forwards the rest.</summary>
    /// <param name="values">The indexable values.</param>
    /// <param name="skipCount">The number of leading values to skip.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitReadOnlyList(IReadOnlyList<T> values, int skipCount, IObserver<T> observer)
    {
        for (var i = skipCount; i < values.Count; i++)
        {
            observer.OnNext(values[i]);
        }

        observer.OnCompleted();
    }
}
