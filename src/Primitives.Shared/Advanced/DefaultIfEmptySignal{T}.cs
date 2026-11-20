// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Emits a default value when the source completes without values.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("DefaultIfEmptySignal: Source = {_source}")]
public sealed class DefaultIfEmptySignal<T> : IObservable<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>Value emitted for an empty source.</summary>
    private readonly T _defaultValue;

    /// <summary>Initializes a new instance of the <see cref="DefaultIfEmptySignal{T}"/> class using <see langword="default"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public DefaultIfEmptySignal(IObservable<T> source)
        : this(source, default!)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="DefaultIfEmptySignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="defaultValue">Value emitted for an empty source.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public DefaultIfEmptySignal(IObservable<T> source, T defaultValue)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        _source = source;
        _defaultValue = defaultValue;
    }

    /// <summary>Creates a sequence that emits <see langword="default"/> when <paramref name="source"/> is empty, with empty/never/range reductions.</summary>
    /// <param name="source">The source observable.</param>
    /// <returns>The non-empty observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> Create(IObservable<T> source) => Create(source, default!);

    /// <summary>Creates a sequence that emits <paramref name="defaultValue"/> when <paramref name="source"/> is empty, with empty/never/range reductions.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="defaultValue">Value emitted for an empty source.</param>
    /// <returns>The non-empty observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> source, T defaultValue)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        if (source is ImmutableEmptySignal<T>)
        {
            return new ImmediateReturnSignal<T>(defaultValue);
        }

        return source is RangeSignal { Count: > 0 } or ImmutableNeverSignal<T>
            ? source
            : new DefaultIfEmptySignal<T>(source, defaultValue);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        DefaultIfEmptyWitness<T> sink = new(observer, _defaultValue);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
