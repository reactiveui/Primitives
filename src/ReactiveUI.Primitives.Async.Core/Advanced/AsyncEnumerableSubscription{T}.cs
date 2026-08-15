// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A subscription that emits the contents of an asynchronous enumerable.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Source = {Source}")]
public sealed class AsyncEnumerableSubscription<T> : TaskSignalSubscription<T>
{
    /// <summary>Initializes a new instance of the <see cref="AsyncEnumerableSubscription{T}"/> class.</summary>
    /// <param name="observer">The observer receiving the values.</param>
    /// <param name="source">The source sequence.</param>
    public AsyncEnumerableSubscription(IObserverAsync<T> observer, IAsyncEnumerable<T> source)
        : base(observer) =>
        Source = source;

    /// <summary>Gets the source sequence.</summary>
    private IAsyncEnumerable<T> Source { get; }

    /// <inheritdoc/>
    protected override async ValueTask ExecuteAsyncCore(IObserverAsync<T> observer, CancellationToken cancellationToken)
    {
        await foreach (var value in Source.WithCancellation(cancellationToken))
        {
            await observer.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
        }

        await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
    }
}
