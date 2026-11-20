// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Pairs elements of two sources in arrival order and projects each pair through a selector.</summary>
/// <typeparam name="T1">The type of the elements in the first source sequence.</typeparam>
/// <typeparam name="T2">The type of the elements in the second source sequence.</typeparam>
/// <typeparam name="TResult">The type of the elements in the resulting sequence produced by the selector function.</typeparam>
[System.Diagnostics.DebuggerDisplay("ZipSignal: First = {_first}, Second = {_second}")]
public sealed class ZipSignal<T1, T2, TResult> : IObservableAsync<TResult>
{
    /// <summary>The first source sequence.</summary>
    private readonly IObservableAsync<T1> _first;

    /// <summary>The second source sequence.</summary>
    private readonly IObservableAsync<T2> _second;

    /// <summary>Combines paired elements into a result element.</summary>
    private readonly Func<T1, T2, TResult> _resultSelector;

    /// <summary>Initializes a new instance of the <see cref="ZipSignal{T1, T2, TResult}"/> class.</summary>
    /// <param name="first">The first asynchronous observable sequence to combine.</param>
    /// <param name="second">The second asynchronous observable sequence to combine.</param>
    /// <param name="resultSelector">A function that specifies how to combine elements from the first and second sequences into a result element.</param>
    public ZipSignal(
        IObservableAsync<T1> first,
        IObservableAsync<T2> second,
        Func<T1, T2, TResult> resultSelector)
    {
        ArgumentExceptionHelper.ThrowIfNull(first);
        ArgumentExceptionHelper.ThrowIfNull(second);
        ArgumentExceptionHelper.ThrowIfNull(resultSelector);
        _first = first;
        _second = second;
        _resultSelector = resultSelector;
    }

    /// <summary>Creates a tuple-producing zip sequence over <paramref name="first"/> and <paramref name="second"/>.</summary>
    /// <param name="first">The first asynchronous observable sequence.</param>
    /// <param name="second">The second asynchronous observable sequence.</param>
    /// <returns>An observable sequence of element pairs.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ZipSignal<T1, T2, (T1 First, T2 Second)> CreateTuple(
        IObservableAsync<T1> first,
        IObservableAsync<T2> second) =>
        new(first, second, static (firstValue, secondValue) => (firstValue, secondValue));

    /// <summary>Subscribes the specified observer by creating a shared <see cref="ZipState{T1, T2, TResult}"/> and subscribing to both source sequences.</summary>
    /// <param name="observer">The observer to receive zipped result elements.</param>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>An async disposable that tears down both subscriptions when disposed.</returns>
    async ValueTask<IAsyncDisposable> IObservableAsync<TResult>.SubscribeAsync(
        IObserverAsync<TResult> observer,
        CancellationToken cancellationToken)
    {
        ZipState<T1, T2, TResult> state = new(observer, _resultSelector);
        state.LinkExternalCancellation(cancellationToken);

        ExceptionDispatchInfo? failure = null;
        try
        {
            var sub1 = await _first.SubscribeAsync(
                new ZipFirstWitness<T1, T2, TResult>(state),
                cancellationToken).ConfigureAwait(false);
            await state.AttachFirstAsync(sub1).ConfigureAwait(false);

            if (!state.IsDone)
            {
                var sub2 = await _second.SubscribeAsync(
                    new ZipSecondWitness<T1, T2, TResult>(state),
                    cancellationToken).ConfigureAwait(false);
                await state.AttachSecondAsync(sub2).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            failure = ExceptionDispatchInfo.Capture(ex);
        }

        if (failure is not null)
        {
            await state.DisposeAsync().ConfigureAwait(false);
            failure.Throw();
        }

        return state;
    }
}
