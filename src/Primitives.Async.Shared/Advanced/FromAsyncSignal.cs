// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Async.Advanced;
#endif

/// <summary>Invokes a task-like operation for each subscription and emits <see cref="RxVoid.Default"/> after it completes.</summary>
public sealed class FromAsyncSignal : IObservableAsync<RxVoid>
{
    /// <summary>Initializes a new instance of the <see cref="FromAsyncSignal"/> class.</summary>
    /// <param name="factory">The operation invoked for each subscription.</param>
    public FromAsyncSignal(Func<CancellationToken, ValueTask> factory)
    {
        ArgumentExceptionHelper.ThrowIfNull(factory);

        Factory = factory;
    }

    /// <summary>Gets the operation invoked for each subscription.</summary>
    private Func<CancellationToken, ValueTask> Factory { get; }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<RxVoid>.SubscribeAsync(
        IObserverAsync<RxVoid> observer,
        CancellationToken cancellationToken)
    {
        _ = cancellationToken;

        FromAsyncSubscription subscription = new(observer, Factory);
        subscription.Start();
        return new(subscription);
    }
}
