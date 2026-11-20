// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Forwards a value immediately when the condition holds, and otherwise after the debounce window, discarding values a newer one supersedes.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="source">The upstream observable.</param>
/// <param name="debounce">The debounce window applied to bypass-false values.</param>
/// <param name="condition">When <see langword="true"/> the value bypasses the delay and is forwarded immediately.</param>
/// <param name="timeProvider">The time provider for the debounce timer.</param>
[System.Diagnostics.DebuggerDisplay("DebounceUntilSignal<{typeof(T).Name,nq}>")]
public sealed class DebounceUntilSignal<T>(
    IObservableAsync<T> source,
    TimeSpan debounce,
    Func<T, bool> condition,
    TimeProvider timeProvider) : IObservableAsync<T>
{
    /// <summary>Creates a <see cref="DebounceUntilSignal{T}"/> using <see cref="TimeProvider.System"/>.</summary>
    /// <param name="source">The upstream observable.</param>
    /// <param name="debounce">The non-negative debounce window.</param>
    /// <param name="condition">The condition that bypasses the delay when <see langword="true"/>.</param>
    /// <returns>A configured <see cref="DebounceUntilSignal{T}"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static DebounceUntilSignal<T> Create(
        IObservableAsync<T> source,
        TimeSpan debounce,
        Func<T, bool> condition) =>
        Create(source, debounce, condition, null);

    /// <summary>Creates a <see cref="DebounceUntilSignal{T}"/> using the supplied time provider.</summary>
    /// <param name="source">The upstream observable.</param>
    /// <param name="debounce">The non-negative debounce window.</param>
    /// <param name="condition">The condition that bypasses the delay when <see langword="true"/>.</param>
    /// <param name="timeProvider">The optional time provider; <see langword="null"/> uses <see cref="TimeProvider.System"/>.</param>
    /// <returns>A configured <see cref="DebounceUntilSignal{T}"/>.</returns>
    public static DebounceUntilSignal<T> Create(
        IObservableAsync<T> source,
        TimeSpan debounce,
        Func<T, bool> condition,
        TimeProvider? timeProvider)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(condition);
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(debounce, TimeSpan.Zero);

        return new(source, debounce, condition, timeProvider ?? TimeProvider.System);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            source,
            new DebounceUntilWitness<T>(observer, debounce, condition, timeProvider, cancellationToken),
            observer,
            cancellationToken);
}
