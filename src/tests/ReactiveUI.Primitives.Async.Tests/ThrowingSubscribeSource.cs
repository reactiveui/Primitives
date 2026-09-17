// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Source whose subscribe call throws before returning a subscription.</summary>
/// <typeparam name="T">The element type.</typeparam>
/// <param name="failure">The exception thrown by the subscribe call.</param>
internal sealed class ThrowingSubscribeSource<T>(Exception failure) : IObservableAsync<T>
{
    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(IObserverAsync<T> observer, CancellationToken cancellationToken) =>
        throw failure;
}
