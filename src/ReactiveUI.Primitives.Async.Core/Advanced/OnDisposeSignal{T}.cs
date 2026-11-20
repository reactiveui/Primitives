// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Wraps a source observable with an observer that runs a callback when the subscription is disposed.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("OnDisposeSignal: Source = {_source}")]
public sealed class OnDisposeSignal<T> : IObservableAsync<T>
{
    /// <summary>The upstream observable.</summary>
    private readonly IObservableAsync<T> _source;

    /// <summary>The synchronous dispose action; <see langword="null"/> when the callback is asynchronous.</summary>
    private readonly Action? _finallySync;

    /// <summary>The asynchronous dispose callback; <see langword="null"/> when the callback is synchronous.</summary>
    private readonly Func<ValueTask>? _finallyAsync;

    /// <summary>Initializes a new instance of the <see cref="OnDisposeSignal{T}"/> class with an asynchronous callback.</summary>
    /// <param name="source">The upstream observable.</param>
    /// <param name="disposeAction">The asynchronous dispose callback.</param>
    public OnDisposeSignal(IObservableAsync<T> source, Func<ValueTask> disposeAction)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(disposeAction);
        _source = source;
        _finallyAsync = disposeAction;
    }

    /// <summary>Initializes a new instance of the <see cref="OnDisposeSignal{T}"/> class with a synchronous action.</summary>
    /// <param name="source">The upstream observable.</param>
    /// <param name="disposeAction">The synchronous dispose action.</param>
    public OnDisposeSignal(IObservableAsync<T> source, Action disposeAction)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(disposeAction);
        _source = source;
        _finallySync = disposeAction;
    }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        OnDisposeWitness<T> sink = new(observer, _finallySync, _finallyAsync);
        return WitnessSubscription.SubscribeAsync(_source, sink, observer, cancellationToken);
    }
}
