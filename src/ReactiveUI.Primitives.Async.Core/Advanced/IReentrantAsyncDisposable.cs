// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An async-disposable that can be disposed from within its own in-flight notification.</summary>
/// <remarks>Terminal sinks dispose themselves from inside the very <c>OnNext</c>/<c>OnCompleted</c> call that
/// produced their result. The normal <see cref="IAsyncDisposable.DisposeAsync"/> path waits for in-flight
/// calls on other threads to drain before completing; once that notification's continuation has hopped threads the
/// wait would block on the call that is itself awaiting the dispose. This entry point skips that self-join.</remarks>
public interface IReentrantAsyncDisposable
{
    /// <summary>Disposes from within the object's own in-flight notification, skipping the in-flight-call wait.</summary>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous dispose operation.</returns>
    ValueTask DisposeFromNotificationAsync();
}
