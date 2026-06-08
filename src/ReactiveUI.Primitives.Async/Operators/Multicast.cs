// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;
using ReactiveUI.Primitives.Async.Signals;
using AsyncSignalFactory = ReactiveUI.Primitives.Async.Signals.Signal;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides a set of extension methods for creating and managing connectable asynchronous observables using various
/// Signal types.
/// </summary>
/// <remarks>The methods in this class enable advanced multicasting scenarios for asynchronous observables,
/// allowing multiple subscribers to share a single subscription to the underlying data source. These methods support
/// different Signal types and configuration options, including stateless and replay behaviors, to accommodate a wide
/// range of reactive programming patterns.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Cached creation options for stateless Signal publishing.</summary>
    private static readonly SignalCreationOptions _statelessPublishOptions = SignalCreationOptions.Default with
    {
        IsStateless = true
    };

    /// <summary>Cached creation options for stateless behavior Signal publishing.</summary>
    private static readonly BehaviorSignalCreationOptions _statelessBehaviorPublishOptions =
        BehaviorSignalCreationOptions.Default with { IsStateless = true };

    /// <summary>Cached creation options for stateless replay-latest Signal publishing.</summary>
    private static readonly ReplayLatestSignalCreationOptions _statelessReplayLatestPublishOptions =
        ReplayLatestSignalCreationOptions.Default with { IsStateless = true };

    /// <summary>Multicasting and publishing operators for an observable source sequence.</summary>
    /// <param name="source">The source sequence.</param>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>
        /// Creates a connectable observable sequence that shares a single subscription to the underlying sequence using
        /// the specified Signal.
        /// </summary>
        /// <remarks>The returned connectable observable will not begin emitting items until its Connect
        /// method is called. This allows multiple observers to subscribe before the sequence starts.</remarks>
        /// <param name="signal">The signal used to multicast the elements of the source sequence to multiple observers. Cannot be null.</param>
        /// <returns>A connectable observable sequence that multicasts the source sequence through the specified signal.</returns>
        public ConnectableSignalAsync<T> Multicast(ISignalAsync<T> signal) =>
            new MulticastSignalAsync<T>(source, signal);

        /// <summary>
        /// Returns a connectable observable sequence that shares a single subscription to the underlying asynchronous
        /// observable. Observers will receive all notifications published after they subscribe.
        /// </summary>
        /// <remarks>Use this method to create a hot observable that allows multiple observers to share a
        /// single subscription to the source. This is useful for scenarios where you want to avoid multiple
        /// subscriptions to the source sequence or coordinate the timing of subscriptions. The returned connectable
        /// observable is asynchronous and supports concurrent observers.</remarks>
        /// <returns>A connectable observable sequence that multicasts notifications to all subscribed observers. The sequence
        /// does not begin emitting items until its Connect method is called.</returns>
        public ConnectableSignalAsync<T> Publish() => source.Multicast(AsyncSignalFactory.Create<T>());

        /// <summary>
        /// Creates a connectable observable sequence that shares a single subscription to the underlying sequence,
        /// using a Signal created with the specified options.
        /// </summary>
        /// <remarks>The returned connectable observable does not begin emitting items until its Connect
        /// method is called. Use this method to control when the subscription to the source sequence starts and to
        /// share the subscription among multiple observers.</remarks>
        /// <param name="options">The options used to configure the Signal that will multicast the source sequence. Cannot be null.</param>
        /// <returns>A connectable observable sequence that multicasts the source sequence using a Signal configured with the
        /// specified options.</returns>
        public ConnectableSignalAsync<T> Publish(SignalCreationOptions options) =>
            source.Multicast(AsyncSignalFactory.Create<T>(options));

        /// <summary>
        /// Returns a connectable observable sequence that shares a single subscription to the underlying sequence and
        /// replays the most recent value to new subscribers, starting with the specified initial value.
        /// </summary>
        /// <remarks>Subscribers will immediately receive the initial value upon subscription, followed by
        /// subsequent values from the source sequence. The returned connectable observable does not begin emitting
        /// values until its Connect method is called.</remarks>
        /// <param name="initialValue">The initial value to be emitted to subscribers before any values are emitted by the source sequence.</param>
        /// <returns>A connectable observable sequence that multicasts the source sequence and replays the latest value, starting
        /// with the specified initial value.</returns>
        public ConnectableSignalAsync<T> Publish(T initialValue) =>
            source.Multicast(AsyncSignalFactory.CreateBehavior(initialValue));

        /// <summary>
        /// Creates a connectable observable sequence that shares a single subscription to the underlying sequence and
        /// starts with the specified initial value.
        /// </summary>
        /// <remarks>The returned connectable observable will not begin emitting items from the source
        /// sequence until its Connect method is called. Subscribers will immediately receive the most recent value,
        /// starting with the specified initial value, upon subscription.</remarks>
        /// <param name="initialValue">The initial value to be emitted to subscribers before any items are emitted by the source sequence.</param>
        /// <param name="options">The options used to configure the behavior of the underlying behavior Signal.</param>
        /// <returns>A connectable observable sequence that multicasts the source sequence and emits the specified initial value
        /// to new subscribers.</returns>
        public ConnectableSignalAsync<T> Publish(T initialValue, BehaviorSignalCreationOptions options) =>
            source.Multicast(AsyncSignalFactory.CreateBehavior(initialValue, options));

        /// <summary>
        /// Creates a connectable observable sequence that shares a single subscription to the underlying source and
        /// does not retain any state between subscriptions.
        /// </summary>
        /// <remarks>Use this method when you want to share a single subscription to the source among
        /// multiple observers, but do not require the observable to cache or replay any items for new subscribers. Each
        /// connection to the returned observable is independent and does not affect subsequent connections.</remarks>
        /// <returns>A connectable observable sequence that multicasts notifications from the source without retaining state
        /// between subscribers.</returns>
        public ConnectableSignalAsync<T> StatelessPublish() =>
            source.Multicast(AsyncSignalFactory.Create<T>(_statelessPublishOptions));

        /// <summary>
        /// Creates a connectable observable sequence that shares a single subscription to the underlying source and
        /// replays the most recent value to new subscribers, starting with the specified initial value.
        /// </summary>
        /// <remarks>The returned observable does not maintain any state between connections. Each
        /// connection starts with the provided initial value and only replays the most recent value published during
        /// that connection. This is useful for scenarios where late subscribers should always receive the latest value,
        /// even if they subscribe after the source has started emitting.</remarks>
        /// <param name="initialValue">The initial value to be emitted to subscribers before any values are published by the source sequence.</param>
        /// <returns>A connectable observable sequence that multicasts the source sequence and replays the latest value, starting
        /// with the specified initial value.</returns>
        public ConnectableSignalAsync<T> StatelessPublish(T initialValue) =>
            source.Multicast(AsyncSignalFactory.CreateBehavior(initialValue, _statelessBehaviorPublishOptions));

        /// <summary>Creates a connectable observable sequence that replays only the most recent item to new subscribers.</summary>
        /// <remarks>This method enables late subscribers to immediately receive the most recently
        /// published value, followed by subsequent values. The returned sequence does not replay earlier items beyond
        /// the latest one. Use this method when you want all subscribers to observe the most recent value, regardless
        /// of when they subscribe.</remarks>
        /// <returns>A connectable observable sequence that publishes the latest item to current and future subscribers until a
        /// new item is emitted.</returns>
        public ConnectableSignalAsync<T> ReplayLatestPublish() =>
            source.Multicast(AsyncSignalFactory.CreateReplayLatest<T>());

        /// <summary>
        /// Creates a connectable observable sequence that replays only the latest published value to new subscribers,
        /// using the specified replay Signal creation options.
        /// </summary>
        /// <remarks>Use this method when you want late subscribers to receive only the most recently
        /// published value, rather than the entire sequence or a fixed buffer. The returned connectable observable does
        /// not begin emitting items until its Connect method is called.</remarks>
        /// <param name="options">The options used to configure the replay Signal, such as buffer size, scheduler, or other replay behavior
        /// settings.</param>
        /// <returns>A connectable observable sequence that replays the most recent value to each new subscriber after
        /// connection.</returns>
        public ConnectableSignalAsync<T> ReplayLatestPublish(ReplayLatestSignalCreationOptions options) =>
            source.Multicast(AsyncSignalFactory.CreateReplayLatest<T>(options));

        /// <summary>
        /// Creates a connectable observable sequence that replays only the latest item to new subscribers and publishes
        /// items to all current subscribers.
        /// </summary>
        /// <remarks>This method is stateless; each call returns a new connectable observable. Subscribers
        /// that connect after an item has been published will immediately receive the latest item. This is useful for
        /// scenarios where late subscribers should catch up with the most recent value without receiving the full
        /// history.</remarks>
        /// <returns>A connectable observable sequence that replays the most recent item to new subscribers and multicasts
        /// notifications to all current subscribers.</returns>
        public ConnectableSignalAsync<T> StatelessReplayLatestPublish() =>
            source.Multicast(AsyncSignalFactory.CreateReplayLatest<T>(_statelessReplayLatestPublishOptions));
    }
}
