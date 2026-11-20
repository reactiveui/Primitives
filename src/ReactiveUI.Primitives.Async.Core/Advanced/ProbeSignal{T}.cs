// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Async observable that forwards the latest source element once each sampling period elapses.</summary>
/// <typeparam name="T">The type of elements in the sequence.</typeparam>
/// <param name="source">The source observable sequence to sample.</param>
/// <param name="period">The sampling period.</param>
/// <param name="timeProvider">The time provider used for the sampling timer.</param>
[System.Diagnostics.DebuggerDisplay("ProbeSignal<{typeof(T).Name,nq}>")]
public sealed class ProbeSignal<T>(IObservableAsync<T> source, TimeSpan period, TimeProvider timeProvider) : IObservableAsync<T>
{
    /// <summary>Creates a <see cref="ProbeSignal{T}"/> using <see cref="TimeProvider.System"/>.</summary>
    /// <param name="source">The source observable sequence to sample.</param>
    /// <param name="period">The non-negative sampling period.</param>
    /// <returns>A configured <see cref="ProbeSignal{T}"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ProbeSignal<T> Create(IObservableAsync<T> source, TimeSpan period) =>
        Create(source, period, null);

    /// <summary>Creates a <see cref="ProbeSignal{T}"/> using the supplied time provider.</summary>
    /// <param name="source">The source observable sequence to sample.</param>
    /// <param name="period">The non-negative sampling period.</param>
    /// <param name="timeProvider">The optional time provider; <see langword="null"/> uses <see cref="TimeProvider.System"/>.</param>
    /// <returns>A configured <see cref="ProbeSignal{T}"/>.</returns>
    public static ProbeSignal<T> Create(IObservableAsync<T> source, TimeSpan period, TimeProvider? timeProvider)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(period, TimeSpan.Zero);

        return new(source, period, timeProvider ?? TimeProvider.System);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            source,
            new ProbeWitness<T>(observer, period, timeProvider),
            observer,
            cancellationToken);
}
