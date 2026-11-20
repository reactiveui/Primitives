// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Applies a stateful synchronous predicate without closure allocation.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <typeparam name="TState">The caller-supplied state type.</typeparam>
/// <param name="source">The upstream observable sequence.</param>
/// <param name="state">The caller-supplied state.</param>
/// <param name="predicate">The stateful predicate.</param>
[System.Diagnostics.DebuggerDisplay("KeepWithSyncSignal<{typeof(T).Name,nq}>")]
public sealed class KeepWithSyncSignal<T, TState>(
    IObservableAsync<T> source,
    TState state,
    Func<TState, T, bool> predicate) : IObservableAsync<T>
{
    /// <summary>Creates a stateful filter sequence over <paramref name="source"/>, short-circuiting empty and never singletons.</summary>
    /// <param name="source">The upstream observable sequence.</param>
    /// <param name="state">The caller-supplied state.</param>
    /// <param name="predicate">The stateful predicate.</param>
    /// <returns>An observable sequence of matching values.</returns>
    public static IObservableAsync<T> Create(
        IObservableAsync<T> source,
        TState state,
        Func<TState, T, bool> predicate)
    {
        ArgumentExceptionHelper.ThrowIfNull(predicate);

        return source is SignalAsync.EmptySignalAsync<T> or SignalAsync.NeverSignalAsync<T>
            ? source
            : new KeepWithSyncSignal<T, TState>(source, state, predicate);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        WitnessSubscription.SubscribeAsync(
            source,
            new KeepWithSyncWitness<T, TState>(observer, state, predicate, cancellationToken),
            observer,
            cancellationToken);
}
