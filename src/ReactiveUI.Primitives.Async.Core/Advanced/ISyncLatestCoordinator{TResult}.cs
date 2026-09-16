// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A <c>SyncLatest</c> coordinator that records each source's latest value and emits the projection once every source has one.</summary>
/// <typeparam name="TResult">The downstream element type.</typeparam>
public interface ISyncLatestCoordinator<TResult> : IAsyncDisposable
{
    /// <summary>Gets the shared subscription lifecycle: gate, dispose token, external link, completion bitmask and values lock.</summary>
    SyncLatestLifecycle<TResult> Lifecycle { get; }

    /// <summary>Projects the latest values and emits them downstream once every source has produced one.</summary>
    /// <returns>A task representing the asynchronous emit.</returns>
    ValueTask EmitLatestAsync();

    /// <summary>Subscribes to a single source by 0-based index.</summary>
    /// <param name="index">The 0-based source index.</param>
    /// <param name="cancellationToken">A token to cancel the subscription.</param>
    /// <returns>The subscription for the source at <paramref name="index"/>.</returns>
    ValueTask<IAsyncDisposable> SubscribeAtAsync(int index, CancellationToken cancellationToken);
}
