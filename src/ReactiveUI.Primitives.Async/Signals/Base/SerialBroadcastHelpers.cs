// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Immutable;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

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
    /// state machine is only used when an observer actually suspends.
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
    public static ValueTask BroadcastOnNextAsyncMulti<T>(
        ImmutableArray<IObserverAsync<T>> observers,
        T value,
        CancellationToken cancellationToken)
    {
        for (var i = 0; i < observers.Length; i++)
        {
            var pending = observers[i].OnNextAsync(value, cancellationToken);
            if (!pending.IsCompletedSuccessfully)
            {
                return AwaitOnNextRemainderAsync(pending, observers, i + 1, value, cancellationToken);
            }

            ConsumeCompleted(pending);
        }

        return default;
    }

    /// <summary>Sequentially forwards <paramref name="error"/> to each observer's resumable error handler.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="observers">The current observer snapshot.</param>
    /// <param name="error">The error being broadcast.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the notification operation.</param>
    /// <returns>A task that represents the asynchronous notification operation.</returns>
    public static ValueTask BroadcastOnErrorResumeAsync<T>(
        ImmutableArray<IObserverAsync<T>> observers,
        Exception error,
        CancellationToken cancellationToken)
    {
        for (var i = 0; i < observers.Length; i++)
        {
            var pending = observers[i].OnErrorResumeAsync(error, cancellationToken);
            if (!pending.IsCompletedSuccessfully)
            {
                return AwaitOnErrorRemainderAsync(pending, observers, i + 1, error, cancellationToken);
            }

            ConsumeCompleted(pending);
        }

        return default;
    }

    /// <summary>Sequentially forwards <paramref name="result"/> to each observer's completion handler.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="observers">The current observer snapshot.</param>
    /// <param name="result">The terminal result being broadcast.</param>
    /// <returns>A task that represents the asynchronous notification operation.</returns>
    public static ValueTask BroadcastOnCompletedAsync<T>(
        ImmutableArray<IObserverAsync<T>> observers,
        Result result)
    {
        for (var i = 0; i < observers.Length; i++)
        {
            var pending = observers[i].OnCompletedAsync(result);
            if (!pending.IsCompletedSuccessfully)
            {
                return AwaitCompletionRemainderAsync(pending, observers, i + 1, result);
            }

            ConsumeCompleted(pending);
        }

        return default;
    }

    /// <summary>Consumes a synchronously completed <see cref="ValueTask"/> without allocating or blocking.</summary>
    /// <param name="pending">The synchronously completed task.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SuppressMessage(
        "Performance",
        "CA1849:Call async methods when in an async method",
        Justification = "The ValueTask is already completed successfully; GetResult consumes IValueTaskSource-backed results without blocking.")]
#pragma warning disable S5034 // Callers guard with IsCompletedSuccessfully before consuming the ValueTask.
    private static void ConsumeCompleted(ValueTask pending) =>
        pending.GetAwaiter().GetResult();
#pragma warning restore S5034

    /// <summary>Awaits the first asynchronous OnNext notification, then continues the serial broadcast.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="pending">The first incomplete notification.</param>
    /// <param name="observers">The current observer snapshot.</param>
    /// <param name="nextIndex">The next observer index to notify.</param>
    /// <param name="value">The value being broadcast.</param>
    /// <param name="cancellationToken">The cancellation token used for notifications.</param>
    /// <returns>A task representing the asynchronous remainder.</returns>
    private static async ValueTask AwaitOnNextRemainderAsync<T>(
        ValueTask pending,
        ImmutableArray<IObserverAsync<T>> observers,
        int nextIndex,
        T value,
        CancellationToken cancellationToken)
    {
        await pending.ConfigureAwait(false);
        for (var i = nextIndex; i < observers.Length; i++)
        {
            await observers[i].OnNextAsync(value, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Awaits the first asynchronous error notification, then continues the serial broadcast.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="pending">The first incomplete notification.</param>
    /// <param name="observers">The current observer snapshot.</param>
    /// <param name="nextIndex">The next observer index to notify.</param>
    /// <param name="error">The error being broadcast.</param>
    /// <param name="cancellationToken">The cancellation token used for notifications.</param>
    /// <returns>A task representing the asynchronous remainder.</returns>
    private static async ValueTask AwaitOnErrorRemainderAsync<T>(
        ValueTask pending,
        ImmutableArray<IObserverAsync<T>> observers,
        int nextIndex,
        Exception error,
        CancellationToken cancellationToken)
    {
        await pending.ConfigureAwait(false);
        for (var i = nextIndex; i < observers.Length; i++)
        {
            await observers[i].OnErrorResumeAsync(error, cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Awaits the first asynchronous completion notification, then continues the serial broadcast.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="pending">The first incomplete notification.</param>
    /// <param name="observers">The current observer snapshot.</param>
    /// <param name="nextIndex">The next observer index to notify.</param>
    /// <param name="result">The terminal result being broadcast.</param>
    /// <returns>A task representing the asynchronous remainder.</returns>
    private static async ValueTask AwaitCompletionRemainderAsync<T>(
        ValueTask pending,
        ImmutableArray<IObserverAsync<T>> observers,
        int nextIndex,
        Result result)
    {
        await pending.ConfigureAwait(false);
        for (var i = nextIndex; i < observers.Length; i++)
        {
            await observers[i].OnCompletedAsync(result).ConfigureAwait(false);
        }
    }
}
