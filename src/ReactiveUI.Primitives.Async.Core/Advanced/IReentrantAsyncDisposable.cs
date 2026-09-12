// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An async-disposable that can be disposed from within its own in-flight notification.</summary>
/// <remarks>A terminal sink disposes itself from inside the <c>OnNext</c>/<c>OnCompleted</c> call that produced
/// its result, which <see cref="IAsyncDisposable.DisposeAsync"/> cannot serve: that path waits for in-flight calls
/// to drain, and the notification awaiting the dispose is one of them. This entry point skips the self-join.</remarks>
public interface IReentrantAsyncDisposable
{
    /// <summary>Disposes from within the object's own in-flight notification, skipping the in-flight-call wait.</summary>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous dispose operation.</returns>
    ValueTask DisposeFromNotificationAsync();
}
