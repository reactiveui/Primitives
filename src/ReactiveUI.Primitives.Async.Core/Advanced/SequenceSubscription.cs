// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>A subscription that emits a contiguous integer sequence.</summary>
[System.Diagnostics.DebuggerDisplay("StartValue = {StartValue}, Count = {Count}")]
public sealed class SequenceSubscription : TaskSignalSubscription<int>
{
    /// <summary>Initializes a new instance of the <see cref="SequenceSubscription"/> class.</summary>
    /// <param name="observer">The observer receiving the values.</param>
    /// <param name="start">The first emitted value.</param>
    /// <param name="count">The number of values to emit.</param>
    public SequenceSubscription(IObserverAsync<int> observer, int start, int count)
        : base(observer)
    {
        StartValue = start;
        Count = count;
    }

    /// <summary>Gets the first emitted value.</summary>
    private int StartValue { get; }

    /// <summary>Gets the number of values to emit.</summary>
    private int Count { get; }

    /// <inheritdoc/>
    protected override async ValueTask ExecuteAsyncCore(
        IObserverAsync<int> observer,
        CancellationToken cancellationToken)
    {
        for (var i = 0; i < Count; i++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // Keep the observer notification token-free on the hot path; the loop checks the subscription token.
            await observer.OnNextAsync(StartValue + i, CancellationToken.None).ConfigureAwait(false);
        }

        await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
    }
}
