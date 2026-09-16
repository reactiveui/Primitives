// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A subscription that runs a cancellable asynchronous job, supplied as a function, feeding a single observer, and joins that job on disposal.</summary>
/// <typeparam name="T">The type of the elements observed by the subscription.</typeparam>
/// <param name="executeAsyncCore">The asynchronous function that defines the subscription logic.</param>
/// <param name="downstream">The observer that receives notifications.</param>
/// <remarks>Disposal waits for the cancelled job except when called from inside that job.</remarks>
[System.Diagnostics.DebuggerDisplay("TaskSignalSubscription: {_task}")]
public sealed class TaskSignalSubscription<T>(
    Func<IObserverAsync<T>, CancellationToken, ValueTask> executeAsyncCore,
    IObserverAsync<T> downstream) : IAsyncDisposable, ITaskSignalJob<T>
{
    /// <summary>The function that runs the job.</summary>
    private readonly Func<IObserverAsync<T>, CancellationToken, ValueTask> _executeAsyncCore = executeAsyncCore;

    /// <summary>Runs the job and joins it on disposal.</summary>
    private readonly TaskSignalState _task = new();

    /// <summary>Starts the subscription's job and returns without waiting for it to finish.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Start() => _task.Start(this, downstream);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => _task.DisposeAsync();

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    ValueTask ITaskSignalJob<T>.ExecuteAsync(IObserverAsync<T> observer, CancellationToken cancellationToken) =>
        _executeAsyncCore(observer, cancellationToken);
}
