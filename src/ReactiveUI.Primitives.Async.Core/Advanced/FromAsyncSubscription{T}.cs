// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A subscription that invokes an asynchronous value factory and emits its result.</summary>
/// <typeparam name="T">The value type emitted by the factory.</typeparam>
[System.Diagnostics.DebuggerDisplay("FromAsyncSubscription: Factory = {Factory}")]
public sealed class FromAsyncSubscription<T> : IAsyncDisposable, ITaskSignalJob<T>
{
    /// <summary>The observer receiving the job's notifications.</summary>
    private readonly IObserverAsync<T> _observer;

    /// <summary>Runs the job and joins it on disposal.</summary>
    private readonly TaskSignalState _task = new();

    /// <summary>Initializes a new instance of the <see cref="FromAsyncSubscription{T}"/> class.</summary>
    /// <param name="observer">The observer receiving the produced value.</param>
    /// <param name="factory">The factory invoked for this subscription.</param>
    public FromAsyncSubscription(IObserverAsync<T> observer, Func<CancellationToken, ValueTask<T>> factory)
    {
        _observer = observer;
        Factory = factory;
    }

    /// <summary>Gets the factory invoked for this subscription.</summary>
    private Func<CancellationToken, ValueTask<T>> Factory { get; }

    /// <summary>Starts the subscription's job and returns without waiting for it to finish.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Start() => _task.Start(this, _observer);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => _task.DisposeAsync();

    /// <inheritdoc/>
    async ValueTask ITaskSignalJob<T>.ExecuteAsync(IObserverAsync<T> observer, CancellationToken cancellationToken)
    {
        var result = await Factory(cancellationToken).ConfigureAwait(false);
        await observer.OnNextAsync(result, cancellationToken).ConfigureAwait(false);
        await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
    }
}
