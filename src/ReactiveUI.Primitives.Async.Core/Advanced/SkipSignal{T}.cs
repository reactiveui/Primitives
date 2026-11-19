// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Drops the first <c>count</c> emissions, then forwards every later value.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="source">The upstream observable.</param>
/// <param name="count">The number of leading emissions to drop, always greater than zero.</param>
[System.Diagnostics.DebuggerDisplay("SkipSignal: Source = {Source}")]
public sealed class SkipSignal<T>(IObservableAsync<T> source, int count) : IObservableAsync<T>
{
    /// <summary>Gets the upstream observable.</summary>
    internal IObservableAsync<T> Source => source;

    /// <summary>Gets the number of leading emissions to drop.</summary>
    internal int Count => count;

    /// <summary>Creates or fuses a <see cref="SkipSignal{T}"/> over <paramref name="source"/>.</summary>
    /// <param name="source">The upstream observable sequence.</param>
    /// <param name="count">The non-negative number of elements to skip.</param>
    /// <returns>An observable sequence that skips the first <paramref name="count"/> elements.</returns>
    public static IObservableAsync<T> Create(IObservableAsync<T> source, int count)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(count);

        if (count == 0 || source is SignalAsync.EmptySignalAsync<T> or SignalAsync.NeverSignalAsync<T>)
        {
            return source;
        }

        return source is SkipSignal<T> existing
            ? new SkipSignal<T>(existing.Source, (int)Math.Min((long)existing.Count + count, int.MaxValue))
            : new SkipSignal<T>(source, count);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            source,
            new SkipWitness<T>(observer, count, cancellationToken),
            observer,
            cancellationToken);
}
