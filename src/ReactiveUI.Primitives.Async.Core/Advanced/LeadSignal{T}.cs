// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>An observable that emits leading values before subscribing to the source sequence.</summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("LeadSignal: Source = {Source}, Values = {Values}")]
public sealed class LeadSignal<T> : IObservableAsync<T>
{
    /// <summary>Initializes a new instance of the <see cref="LeadSignal{T}"/> class.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="values">The leading values.</param>
    public LeadSignal(IObservableAsync<T> source, IEnumerable<T> values)
    {
        Source = source;
        Values = values;
    }

    /// <summary>Gets the source sequence.</summary>
    private IObservableAsync<T> Source { get; }

    /// <summary>Gets the leading values.</summary>
    private IEnumerable<T> Values { get; }

    /// <inheritdoc/>
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        LeadSubscription<T> subscription = new(Source, Values, observer, cancellationToken);
        subscription.Start();
        return new(subscription);
    }
}
