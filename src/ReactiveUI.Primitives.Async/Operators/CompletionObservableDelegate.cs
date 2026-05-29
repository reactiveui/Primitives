// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Represents a method that subscribes to completion notifications and returns an asynchronous disposable used to
/// unsubscribe.
/// </summary>
/// <remarks>The returned <see cref="IAsyncDisposable"/> should be disposed to stop receiving completion
/// notifications and to release any associated resources. The <paramref name="notifyStop"/> callback may be invoked on
/// a background thread.</remarks>
/// <param name="notifyStop">An action to be invoked with a <see cref="Result"/> when the completion event occurs. This callback is called to
/// notify the subscriber of the completion result.</param>
/// <returns>An <see cref="IAsyncDisposable"/> that unsubscribes the notification when disposed asynchronously.</returns>
public delegate IAsyncDisposable CompletionObservableDelegate(Action<Result> notifyStop);
