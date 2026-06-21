// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A subscription that invokes an asynchronous value factory and emits its result.</summary>
/// <typeparam name="T">The value type emitted by the factory.</typeparam>
public sealed class FromAsyncSubscription<T> : TaskSignalSubscription<T>
{
    /// <summary>Initializes a new instance of the <see cref="FromAsyncSubscription{T}"/> class.</summary>
    /// <param name="observer">The observer receiving the produced value.</param>
    /// <param name="factory">The factory invoked for this subscription.</param>
    public FromAsyncSubscription(IObserverAsync<T> observer, Func<CancellationToken, ValueTask<T>> factory)
        : base(observer) =>
        Factory = factory;

    /// <summary>Gets the factory invoked for this subscription.</summary>
    private Func<CancellationToken, ValueTask<T>> Factory { get; }

    /// <inheritdoc/>
    protected override async ValueTask ExecuteAsyncCore(IObserverAsync<T> observer, CancellationToken cancellationToken)
    {
        var result = await Factory(cancellationToken).ConfigureAwait(false);
        await observer.OnNextAsync(result, cancellationToken).ConfigureAwait(false);
        await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
    }
}
