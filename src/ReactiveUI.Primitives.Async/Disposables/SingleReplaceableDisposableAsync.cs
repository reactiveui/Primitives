// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Disposables;

/// <summary>
/// Provides a thread-safe mechanism for managing a single asynchronously disposable resource that can be replaced or
/// disposed of serially.
/// </summary>
/// <remarks>When a new disposable is set using SetDisposableAsync, the previously held disposable (if any) is
/// asynchronously disposed. Disposing the SerialDisposableAsync instance disposes the current disposable and prevents
/// further disposables from being set. This class is useful for scenarios where a resource needs to be replaced or
/// updated over time, ensuring that only one resource is active and properly disposed of at any given moment. All
/// operations are safe to use concurrently from multiple threads.</remarks>
public class SingleReplaceableDisposableAsync : IAsyncDisposable
{
    /// <summary>The currently tracked disposable resource, or the disposed sentinel if already disposed.</summary>
    private IAsyncDisposable? _current;

    /// <summary>
    /// Replaces the currently tracked asynchronous disposable resource with a new one, disposing the previous resource
    /// if present.
    /// </summary>
    /// <remarks>If the object has already been disposed, <paramref name="value"/> is disposed immediately.
    /// Otherwise, the previously tracked resource, if any, is disposed asynchronously. This method is
    /// thread-safe.</remarks>
    /// <param name="value">The new <see cref="IAsyncDisposable"/> instance to track. Can be <see langword="null"/> to clear the current
    /// resource.</param>
    /// <returns>A <see cref="ValueTask"/> that represents the asynchronous dispose operation of the previously tracked resource,
    /// or of <paramref name="value"/> if the object has already been disposed. If there is no resource to dispose, the
    /// returned task is already completed.</returns>
    public ValueTask SetDisposableAsync(IAsyncDisposable? value) =>
        DisposableAsyncSlot.SwapAsync(ref _current, value);

    /// <summary>Asynchronously releases the resources used by the object.</summary>
    /// <remarks>Subsequent calls to this method after disposal will have no effect. This method is safe to
    /// call multiple times.</remarks>
    /// <returns>A ValueTask that represents the asynchronous dispose operation. The task will be completed when all resources
    /// have been released.</returns>
    public ValueTask DisposeAsync()
    {
        GC.SuppressFinalize(this);
        return DisposableAsyncSlot.DisposeAsync(ref _current);
    }
}
