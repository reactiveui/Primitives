// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Dedicated signal for <c>SkipWhile</c>.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("SkipWhileSignal: Source = {_source}")]
public sealed class SkipWhileSignal<T> : IObservable<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The predicate that determines whether to keep skipping values.</summary>
    private readonly Func<T, bool> _predicate;

    /// <summary>Initializes a new instance of the <see cref="SkipWhileSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="predicate">The predicate that determines whether to keep skipping values.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public SkipWhileSignal(IObservable<T> source, Func<T, bool> predicate)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(predicate);

        _source = source;
        _predicate = predicate;
    }

    /// <summary>Creates a <c>SkipWhile</c> sequence with empty/never short-circuiting.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="predicate">The predicate that determines whether to keep skipping values.</param>
    /// <returns>The skipped observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="predicate"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> source, Func<T, bool> predicate)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(predicate);

        return source is ImmutableEmptySignal<T> or ImmutableNeverSignal<T>
            ? source
            : new SkipWhileSignal<T>(source, predicate);
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

        SkipWhileWitness<T> sink = new(observer, _predicate);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <summary>Skips leading range values while <paramref name="predicate"/> holds, then forwards the rest.</summary>
    /// <param name="range">The range source.</param>
    /// <param name="predicate">The skipping predicate.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitRange(RangeSignal range, Func<T, bool> predicate, IObserver<T> observer)
    {
        var intPredicate = Unsafe.As<Func<int, bool>>(predicate);
        var intObserver = Unsafe.As<IObserver<int>>(observer);
        var start = range.Start;
        var count = range.Count;
        var i = 0;
        while (i < count && intPredicate(start + i))
        {
            i++;
        }

        for (; i < count; i++)
        {
            intObserver.OnNext(start + i);
        }

        observer.OnCompleted();
    }

    /// <summary>Skips leading list values while <paramref name="predicate"/> holds, then forwards the rest.</summary>
    /// <param name="values">The indexable values.</param>
    /// <param name="predicate">The skipping predicate.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitReadOnlyList(IReadOnlyList<T> values, Func<T, bool> predicate, IObserver<T> observer)
    {
        var i = 0;
        while (i < values.Count && predicate(values[i]))
        {
            i++;
        }

        for (; i < values.Count; i++)
        {
            observer.OnNext(values[i]);
        }

        observer.OnCompleted();
    }
}
