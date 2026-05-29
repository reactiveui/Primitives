// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Internals;

/// <summary>
/// An observable that delegates subscription logic to a user-supplied asynchronous function.
/// </summary>
/// <typeparam name="T">The type of the elements in the observable sequence.</typeparam>
/// <param name="subscribeAsync">The asynchronous function invoked when an observer subscribes.</param>
internal sealed class AnonymousObservableAsync<T>(
    Func<IObserverAsync<T>, CancellationToken, ValueTask<IAsyncDisposable>> subscribeAsync) : ObservableAsync<T>
{
    /// <inheritdoc/>
    protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken) =>
        subscribeAsync(observer, cancellationToken);
}
