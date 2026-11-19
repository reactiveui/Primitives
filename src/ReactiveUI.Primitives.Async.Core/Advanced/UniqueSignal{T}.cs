// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Drops each value that the comparer judges equal to the most-recently-forwarded one.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("UniqueSignal: Source = {_source}")]
public sealed class UniqueSignal<T> : IObservableAsync<T>
{
    /// <summary>The upstream observable.</summary>
    private readonly IObservableAsync<T> _source;

    /// <summary>The equality comparer used to detect duplicates.</summary>
    private readonly IEqualityComparer<T> _comparer;

    /// <summary>Initializes a new instance of the <see cref="UniqueSignal{T}"/> class using <see cref="EqualityComparer{T}.Default"/>.</summary>
    /// <param name="source">The upstream observable.</param>
    public UniqueSignal(IObservableAsync<T> source)
        : this(source, EqualityComparer<T>.Default)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="UniqueSignal{T}"/> class.</summary>
    /// <param name="source">The upstream observable.</param>
    /// <param name="comparer">The equality comparer used to detect duplicates.</param>
    public UniqueSignal(IObservableAsync<T> source, IEqualityComparer<T> comparer)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(comparer);
        _source = source;
        _comparer = comparer;
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            _source,
            new UniqueWitness<T>(observer, _comparer, cancellationToken),
            observer,
            cancellationToken);
}
