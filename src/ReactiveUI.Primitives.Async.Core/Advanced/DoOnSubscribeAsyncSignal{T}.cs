// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that invokes an asynchronous side effect before subscribing to its source.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Source = {Source}, Action = {Action}")]
public sealed class DoOnSubscribeAsyncSignal<T> : IObservableAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="DoOnSubscribeAsyncSignal{T}"/> class.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="action">The asynchronous side effect to invoke before subscription.</param>
    public DoOnSubscribeAsyncSignal(
        IObservableAsync<T> source,
        Func<CancellationToken, ValueTask> action)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(action);

        Source = source;
        Action = action;
    }

    /// <summary>Gets the asynchronous side effect to invoke before subscription.</summary>
    private Func<CancellationToken, ValueTask> Action { get; }

    /// <summary>Gets the source observable sequence.</summary>
    private IObservableAsync<T> Source { get; }

    /// <inheritdoc/>
    async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        await Action(cancellationToken).ConfigureAwait(false);
        return await Source.SubscribeAsync(observer.Wrap(), cancellationToken).ConfigureAwait(false);
    }
}
