// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Dedicated signal for <c>Distinct</c>.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("DistinctSignal: Source = {_source}")]
public sealed class DistinctSignal<T> : IObservable<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The comparer used to identify duplicates.</summary>
    private readonly IEqualityComparer<T>? _comparer;

    /// <summary>Initializes a new instance of the <see cref="DistinctSignal{T}"/> class using the default equality comparer.</summary>
    /// <param name="source">The source observable.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public DistinctSignal(IObservable<T> source)
        : this(source, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="DistinctSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="comparer">The comparer used to identify duplicates.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public DistinctSignal(IObservable<T> source, IEqualityComparer<T>? comparer)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        _source = source;
        _comparer = comparer;
    }

    /// <summary>Applies <c>Distinct</c> reductions using the default equality comparer.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>The distinct observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> Create(IObservable<T> source) => Create(source, null);

    /// <summary>Applies <c>Distinct</c> reductions using <paramref name="comparer"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="comparer">The comparer used to identify duplicates.</param>
    /// <returns>The distinct observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> source, IEqualityComparer<T>? comparer)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        if (source is ImmutableEmptySignal<T> or ImmutableNeverSignal<T> or ImmediateReturnSignal<T>)
        {
            return source;
        }

        return typeof(T) == typeof(int)
               && source is RangeSignal
               && (comparer is null || ReferenceEquals(comparer, EqualityComparer<T>.Default))
            ? source
            : new DistinctSignal<T>(source, comparer);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        DistinctWitness<T> sink = new(observer, CreateSeen());
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <summary>Creates the duplicate-tracking set, pre-sized when the source has a known element count.</summary>
    /// <returns>The set that tracks the values seen so far.</returns>
    private HashSet<T> CreateSeen() =>
#if NET8_0_OR_GREATER
        (_source is RangeSignal range ? range.Count : 0) switch
        {
            var capacity when capacity > 0 => [with(capacity, _comparer)],
            _ when _comparer is null => [],
            _ => [with(_comparer)],
        };
#else
        [with(_comparer)];
#endif
}
