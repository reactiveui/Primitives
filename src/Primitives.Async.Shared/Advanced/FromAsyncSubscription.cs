// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Async.Advanced;
#endif

/// <summary>Invokes an asynchronous operation and forwards the signal notification for one subscription.</summary>
[System.Diagnostics.DebuggerDisplay("Factory = {Factory}")]
public sealed class FromAsyncSubscription : TaskSignalSubscription<RxVoid>
{
    /// <summary>Initializes a new instance of the <see cref="FromAsyncSubscription"/> class.</summary>
    /// <param name="observer">The observer receiving the signal notification.</param>
    /// <param name="factory">The operation invoked for this subscription.</param>
    public FromAsyncSubscription(IObserverAsync<RxVoid> observer, Func<CancellationToken, ValueTask> factory)
        : base(observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(factory);

        Factory = factory;
    }

    /// <summary>Gets the operation invoked for this subscription.</summary>
    private Func<CancellationToken, ValueTask> Factory { get; }

    /// <inheritdoc/>
    protected override async ValueTask ExecuteAsyncCore(
        IObserverAsync<RxVoid> observer,
        CancellationToken cancellationToken)
    {
        await Factory(cancellationToken).ConfigureAwait(false);
        await observer.OnNextAsync(RxVoid.Default, cancellationToken).ConfigureAwait(false);
        await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
    }
}
