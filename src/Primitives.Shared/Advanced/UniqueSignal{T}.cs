// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

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

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        UniqueWitness<T> sink = new(observer, _comparer);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
