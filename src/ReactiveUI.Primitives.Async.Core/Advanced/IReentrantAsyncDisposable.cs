// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An async-disposable that can be disposed from within its own in-flight notification.</summary>
/// <remarks>Disposal from inside an observer callback must not wait for that same callback to finish.</remarks>
public interface IReentrantAsyncDisposable
{
    /// <summary>Disposes from within the object's own in-flight notification, skipping the in-flight-call wait.</summary>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous dispose operation.</returns>
    ValueTask DisposeFromNotificationAsync();
}
