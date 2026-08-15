// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that invokes an asynchronous value factory for each subscription.</summary>
/// <typeparam name="T">The value type emitted by the factory.</typeparam>
[System.Diagnostics.DebuggerDisplay("Factory = {Factory}")]
public sealed class FromAsyncSignal<T> : IObservableAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="FromAsyncSignal{T}"/> class.</summary>
    /// <param name="factory">The factory invoked for each subscription.</param>
    public FromAsyncSignal(Func<CancellationToken, ValueTask<T>> factory)
    {
        ArgumentExceptionHelper.ThrowIfNull(factory);

        Factory = factory;
    }

    /// <summary>Gets the factory invoked for each subscription.</summary>
    private Func<CancellationToken, ValueTask<T>> Factory { get; }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        _ = cancellationToken;

        FromAsyncSubscription<T> subscription = new(observer, Factory);
        subscription.Start();
        return new(subscription);
    }
}
