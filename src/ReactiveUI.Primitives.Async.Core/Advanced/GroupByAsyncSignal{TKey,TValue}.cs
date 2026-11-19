// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;
using ReactiveUI.Primitives.Async.Signals;
using AsyncSignalFactory = ReactiveUI.Primitives.Async.Signals.Signal;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Async observable that groups source elements by key, emitting one observable per unique key.</summary>
/// <typeparam name="TKey">The type of the grouping key.</typeparam>
/// <typeparam name="TValue">The type of elements in the source sequence.</typeparam>
[System.Diagnostics.DebuggerDisplay("GroupByAsyncSignal: Source = {Source}")]
public sealed class GroupByAsyncSignal<TKey, TValue> : IObservableAsync<GroupedAsyncSignal<TKey, TValue>>
    where TKey : notnull
{
    /// <summary>Initializes a new instance of the <see cref="GroupByAsyncSignal{TKey, TValue}"/> class with default group signals.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="keySelector">A function to extract the key for each element.</param>
    public GroupByAsyncSignal(
        IObservableAsync<TValue> source,
        Func<TValue, TKey> keySelector)
        : this(source, keySelector, static _ => AsyncSignalFactory.Create<TValue>())
    {
    }

    /// <summary>Initializes a new instance of the <see cref="GroupByAsyncSignal{TKey, TValue}"/> class.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="keySelector">A function to extract the key for each element.</param>
    /// <param name="groupSignalSelector">A function that provides a signal for each group, given its key.</param>
    public GroupByAsyncSignal(
        IObservableAsync<TValue> source,
        Func<TValue, TKey> keySelector,
        Func<TKey, ISignalAsync<TValue>> groupSignalSelector)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(keySelector);
        Source = source;
        KeySelector = keySelector;
        GroupSignalSelector = groupSignalSelector;
    }

    /// <summary>Gets the source observable sequence whose elements are grouped by key.</summary>
    internal IObservableAsync<TValue> Source { get; }

    /// <summary>Gets the function used to extract the grouping key from each source element.</summary>
    internal Func<TValue, TKey> KeySelector { get; }

    /// <summary>Gets the factory function that creates a signal for each new group key.</summary>
    internal Func<TKey, ISignalAsync<TValue>> GroupSignalSelector { get; }

    /// <inheritdoc/>
    async ValueTask<IAsyncDisposable> IObservableAsync<GroupedAsyncSignal<TKey, TValue>>.SubscribeAsync(
        IObserverAsync<GroupedAsyncSignal<TKey, TValue>> observer,
        CancellationToken cancellationToken)
    {
        GroupingCoordinator<TKey, TValue> subscription = new(this, observer);
        ExceptionDispatchInfo failure;
        try
        {
            return await subscription.SubscribeSourcesAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            failure = ExceptionDispatchInfo.Capture(e);
        }

        await subscription.DisposeAsync().ConfigureAwait(false);
        return CapturedFailure.Rethrow<IAsyncDisposable>(failure);
    }
}
