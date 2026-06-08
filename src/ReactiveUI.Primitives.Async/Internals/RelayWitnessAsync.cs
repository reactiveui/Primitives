// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Internals;

/// <summary>Relays notifications from the base observer pipeline to another asynchronous observer.</summary>
/// <typeparam name="T">The type of elements received by the witness.</typeparam>
/// <param name="observer">The witness that receives the relayed notifications.</param>
internal sealed class RelayWitnessAsync<T>(IObserverAsync<T> observer) : ObserverAsync<T>
{
    /// <inheritdoc/>
    protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
        observer.OnNextAsync(value, cancellationToken);

    /// <inheritdoc/>
    protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
        observer.OnErrorResumeAsync(error, cancellationToken);

    /// <inheritdoc/>
    protected override ValueTask OnCompletedAsyncCore(Result result) => observer.OnCompletedAsync(result);
}
