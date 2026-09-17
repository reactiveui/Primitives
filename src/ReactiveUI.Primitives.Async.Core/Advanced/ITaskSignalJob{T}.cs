// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>The cancellable asynchronous job a <see cref="TaskSignalState"/> runs for one subscription.</summary>
/// <typeparam name="T">The type of the elements the job delivers.</typeparam>
/// <remarks>Implement it explicitly on the subscription so the job is not part of the subscription's public surface.</remarks>
public interface ITaskSignalJob<T>
{
    /// <summary>Runs the job, delivering notifications to the observer until it finishes or the token is cancelled.</summary>
    /// <param name="observer">The observer that receives notifications.</param>
    /// <param name="cancellationToken">A token that cancels the job.</param>
    /// <returns>A task representing the asynchronous job.</returns>
    ValueTask ExecuteAsync(IObserverAsync<T> observer, CancellationToken cancellationToken);
}
