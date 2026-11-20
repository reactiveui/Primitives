// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Forwards values until the count is exhausted, then signals completion downstream.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="source">The upstream observable.</param>
/// <param name="count">The maximum number of values to forward, always greater than zero.</param>
[System.Diagnostics.DebuggerDisplay("TakeSignal: Source = {Source}")]
public sealed class TakeSignal<T>(IObservableAsync<T> source, int count) : IObservableAsync<T>
{
    /// <summary>Gets the upstream observable.</summary>
    internal IObservableAsync<T> Source => source;

    /// <summary>Gets the maximum number of values to forward.</summary>
    internal int Count => count;

    /// <summary>Creates or fuses a <see cref="TakeSignal{T}"/> over <paramref name="source"/>.</summary>
    /// <param name="source">The upstream observable sequence.</param>
    /// <param name="count">The non-negative maximum number of elements to forward.</param>
    /// <returns>An observable sequence that forwards at most <paramref name="count"/> elements.</returns>
    public static IObservableAsync<T> Create(IObservableAsync<T> source, int count)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(count);

        if (count == 0)
        {
            return SignalAsync.EmptySignalAsync<T>.Instance;
        }

        if (source is SignalAsync.EmptySignalAsync<T> or SignalAsync.NeverSignalAsync<T>)
        {
            return source;
        }

        return source is TakeSignal<T> existing
            ? new TakeSignal<T>(existing.Source, Math.Min(existing.Count, count))
            : new TakeSignal<T>(source, count);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            source,
            new TakeWitness<T>(observer, count, cancellationToken),
            observer,
            cancellationToken);
}
