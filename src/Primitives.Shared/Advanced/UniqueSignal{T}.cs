// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Signal that drops a value equal to the one before it.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("UniqueSignal: Source = {_source}")]
public sealed class UniqueSignal<T> : IObservable<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The comparer used to compare adjacent values.</summary>
    private readonly IEqualityComparer<T> _comparer;

    /// <summary>Initializes a new instance of the <see cref="UniqueSignal{T}"/> class using the default equality comparer.</summary>
    /// <param name="source">The source observable.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public UniqueSignal(IObservable<T> source)
        : this(source, EqualityComparer<T>.Default)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="UniqueSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="comparer">The comparer used to compare adjacent values.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="comparer"/> is <see langword="null"/>.</exception>
    public UniqueSignal(IObservable<T> source, IEqualityComparer<T> comparer)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(comparer);

        _source = source;
        _comparer = comparer;
    }

    /// <summary>Gets the comparer used to compare adjacent values.</summary>
    internal IEqualityComparer<T> Comparer => _comparer;

    /// <summary>Applies <c>Unique</c>/<c>DistinctUntilChanged</c> reductions and idempotence using the default comparer.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>The reduced observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> Create(IObservable<T> source) => Create(source, null);

    /// <summary>Applies <c>Unique</c>/<c>DistinctUntilChanged</c> reductions and idempotence.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="comparer">The optional equality comparer.</param>
    /// <returns>The reduced observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> source, IEqualityComparer<T>? comparer)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        var resolved = comparer ?? EqualityComparer<T>.Default;
        if (source is ImmutableEmptySignal<T> or ImmutableNeverSignal<T> or ImmediateReturnSignal<T>)
        {
            return source;
        }

        if (typeof(T) == typeof(int) && source is RangeSignal && ReferenceEquals(resolved, EqualityComparer<T>.Default))
        {
            return source;
        }

        return source is UniqueSignal<T> existing && ReferenceEquals(existing.Comparer, resolved)
            ? source
            : new UniqueSignal<T>(source, resolved);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (_source is FromEnumerableSignal<T> enumerable && enumerable.TryGetReadOnlyValues(out var values))
        {
            EmitReadOnlyList(values, _comparer, observer);
            return EmptyDisposable.Instance;
        }

        UniqueWitness<T> sink = new(observer, _comparer);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <summary>Filters adjacent duplicates from an indexable synchronous list straight into <paramref name="observer"/>.</summary>
    /// <param name="values">The indexable values.</param>
    /// <param name="comparer">The comparer applied to adjacent values.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitReadOnlyList(IReadOnlyList<T> values, IEqualityComparer<T> comparer, IObserver<T> observer)
    {
        var hasLast = false;
        var last = default(T);
        for (var i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (hasLast && comparer.Equals(last!, value))
            {
                continue;
            }

            hasLast = true;
            last = value;
            observer.OnNext(value);
        }

        observer.OnCompleted();
    }
}
