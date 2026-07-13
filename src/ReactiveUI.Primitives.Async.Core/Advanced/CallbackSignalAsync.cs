// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that invokes a callback to create each subscription.</summary>
/// <typeparam name = "T">The type of the elements in the observable sequence.</typeparam>
/// <param name = "subscribeAsync">The asynchronous function invoked when an observer subscribes.</param>
internal sealed class CallbackSignalAsync<T>(
    Func<IObserverAsync<T>, CancellationToken, ValueTask<IAsyncDisposable>> subscribeAsync) : IObservableAsync<T>
{
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) => subscribeAsync(observer, cancellationToken);
}
