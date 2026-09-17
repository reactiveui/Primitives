// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A subscription that emits the contents of an enumerable.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("EnumerableSubscription: Source = {Source}")]
public sealed class EnumerableSubscription<T> : IAsyncDisposable, ITaskSignalJob<T>
{
    /// <summary>The observer receiving the job's notifications.</summary>
    private readonly IObserverAsync<T> _observer;

    /// <summary>Runs the job and joins it on disposal.</summary>
    private readonly TaskSignalState _task = new();

    /// <summary>Initializes a new instance of the <see cref="EnumerableSubscription{T}"/> class.</summary>
    /// <param name="observer">The observer receiving the values.</param>
    /// <param name="source">The source sequence.</param>
    public EnumerableSubscription(IObserverAsync<T> observer, IEnumerable<T> source)
    {
        _observer = observer;
        Source = source;
    }

    /// <summary>Gets the source sequence.</summary>
    private IEnumerable<T> Source { get; }

    /// <summary>Starts the subscription's job and returns without waiting for it to finish.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Start() => _task.Start(this, _observer);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask DisposeAsync() => _task.DisposeAsync();

    /// <inheritdoc/>
    async ValueTask ITaskSignalJob<T>.ExecuteAsync(IObserverAsync<T> observer, CancellationToken cancellationToken)
    {
        foreach (var value in Source)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            await observer.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
        }

        await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
    }
}
