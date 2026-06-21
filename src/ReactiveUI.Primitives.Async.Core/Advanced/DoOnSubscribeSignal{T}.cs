// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that invokes a synchronous side effect before subscribing to its source.</summary>
/// <typeparam name="T">The element type.</typeparam>
public sealed class DoOnSubscribeSignal<T> : IObservableAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="DoOnSubscribeSignal{T}"/> class.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="action">The side effect to invoke before subscription.</param>
    public DoOnSubscribeSignal(IObservableAsync<T> source, Action action)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(action);

        Source = source;
        Action = action;
    }

    /// <summary>Gets the side effect to invoke before subscription.</summary>
    private Action Action { get; }

    /// <summary>Gets the source observable sequence.</summary>
    private IObservableAsync<T> Source { get; }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        Action();
        return Source.SubscribeAsync(observer.Wrap(), cancellationToken);
    }
}
