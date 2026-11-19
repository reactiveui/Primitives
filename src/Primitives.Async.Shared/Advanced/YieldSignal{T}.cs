// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Async.Advanced;
#endif

/// <summary>An observable that yields control to the current scheduler before forwarding source emissions.</summary>
/// <typeparam name="T">The type of elements in the observable sequence.</typeparam>
[System.Diagnostics.DebuggerDisplay("YieldSignal: Source = {_source}")]
public sealed class YieldSignal<T> : IObservableAsync<T>
{
    /// <summary>The source observable to yield from.</summary>
    private readonly IObservableAsync<T> _source;

    /// <summary>Initializes a new instance of the <see cref="YieldSignal{T}"/> class.</summary>
    /// <param name="source">The source observable to yield from.</param>
    public YieldSignal(IObservableAsync<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        _source = source;
    }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        var currentContext = AsyncContext.GetCurrent();
        return WitnessSubscription.SubscribeAsync(
            _source,
            new ContextSwitchSignalAsync<T>.ContextSwitchWitness(observer, currentContext, true),
            observer,
            cancellationToken);
    }
}
