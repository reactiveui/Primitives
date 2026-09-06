// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that emits a contiguous integer sequence for each subscription.</summary>
[System.Diagnostics.DebuggerDisplay("SequenceSignal: StartValue = {StartValue}, Count = {Count}")]
public sealed class SequenceSignal : IObservableAsync<int>
{
    /// <summary>Initializes a new instance of the <see cref="SequenceSignal"/> class.</summary>
    /// <param name="start">The first emitted value.</param>
    /// <param name="count">The number of values to emit.</param>
    public SequenceSignal(int start, int count)
    {
        StartValue = start;
        Count = count;
    }

    /// <summary>Gets the first emitted value.</summary>
    private int StartValue { get; }

    /// <summary>Gets the number of values to emit.</summary>
    private int Count { get; }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<int>.SubscribeAsync(
        IObserverAsync<int> observer,
        CancellationToken cancellationToken)
    {
        _ = cancellationToken;

        SequenceSubscription subscription = new(observer, StartValue, Count);
        subscription.Start();
        return new(subscription);
    }
}
