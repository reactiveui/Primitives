// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;

namespace ReactiveUI.Primitives.Async.Signals;

/// <summary>
/// Shared serial-broadcast loops for the Serial* Signal family. The body of each Signal's
/// <c>OnNextAsyncCore</c> / <c>OnErrorResumeAsyncCore</c> / <c>OnCompletedAsyncCore</c> is identical:
/// iterate the observer snapshot and await each call in turn. Centralising the loops here keeps the
/// hot-path single-observer fast-path inlined at the call site while removing the duplicated
/// multi-observer body across four Signal classes. Methods are static so there is no virtual
/// dispatch and no extra heap allocation per emission.
/// </summary>
internal static class SerialBroadcastHelpers
{
    /// <summary>
    /// Single-observer fast path delegates directly to the observer's <c>OnNextAsync</c>; the
    /// multi-observer case forwards to <see cref="BroadcastOnNextAsyncMulti{T}"/>, where the async
    /// state-machine box is only paid when there is more than one subscriber.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="observers">The current observer snapshot.</param>
    /// <param name="value">The value being broadcast.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the notification operation.</param>
    /// <returns>A task that represents the asynchronous notification operation.</returns>
    public static ValueTask BroadcastOnNextAsync<T>(
        ImmutableArray<IObserverAsync<T>> observers,
        T value,
        CancellationToken cancellationToken) =>
        observers.Length == 1
            ? observers[0].OnNextAsync(value, cancellationToken)
            : BroadcastOnNextAsyncMulti(observers, value, cancellationToken);

    /// <summary>Sequentially forwards <paramref name="value"/> to each observer in turn.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="observers">The current observer snapshot.</param>
    /// <param name="value">The value being broadcast.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the notification operation.</param>
    /// <returns>A task that represents the asynchronous notification operation.</returns>
    public static async ValueTask BroadcastOnNextAsyncMulti<T>(
        ImmutableArray<IObserverAsync<T>> observers,
        T value,
        CancellationToken cancellationToken)
    {
        for (var i = 0; i < observers.Length; i++)
        {
            await observers[i].OnNextAsync(value, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Sequentially forwards <paramref name="error"/> to each observer's resumable error handler.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="observers">The current observer snapshot.</param>
    /// <param name="error">The error being broadcast.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the notification operation.</param>
    /// <returns>A task that represents the asynchronous notification operation.</returns>
    public static async ValueTask BroadcastOnErrorResumeAsync<T>(
        ImmutableArray<IObserverAsync<T>> observers,
        Exception error,
        CancellationToken cancellationToken)
    {
        for (var i = 0; i < observers.Length; i++)
        {
            await observers[i].OnErrorResumeAsync(error, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Sequentially forwards <paramref name="result"/> to each observer's completion handler.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="observers">The current observer snapshot.</param>
    /// <param name="result">The terminal result being broadcast.</param>
    /// <returns>A task that represents the asynchronous notification operation.</returns>
    public static async ValueTask BroadcastOnCompletedAsync<T>(
        ImmutableArray<IObserverAsync<T>> observers,
        Result result)
    {
        for (var i = 0; i < observers.Length; i++)
        {
            await observers[i].OnCompletedAsync(result).ConfigureAwait(false);
        }
    }
}
