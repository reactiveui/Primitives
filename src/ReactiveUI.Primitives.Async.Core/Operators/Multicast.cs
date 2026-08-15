// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

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
    /// <summary>Multicasting and publishing operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>
        /// Creates a connectable observable sequence that shares a single subscription to the underlying sequence using
        /// the specified Signal.
        /// </summary>
        /// <param name="signal">The signal used to multicast the elements of the source sequence to multiple observers. Cannot be null.</param>
        /// <returns>A connectable observable sequence that multicasts the source sequence through the specified signal.</returns>
        /// <remarks>The returned connectable observable will not begin emitting items until its Connect
        /// method is called. This allows multiple observers to subscribe before the sequence starts.</remarks>
        public ConnectableSignalAsync<T> Multicast(ISignalAsync<T> signal) =>
            new(source, signal);

        /// <summary>
        /// Returns a connectable observable sequence that shares a single subscription to the underlying asynchronous
        /// observable. Observers will receive all notifications published after they subscribe.
        /// </summary>
        /// <returns>A connectable observable sequence that multicasts notifications to all subscribed observers. The sequence
        /// does not begin emitting items until its Connect method is called.</returns>
        /// <remarks>Use this method to create a hot observable that allows multiple observers to share a
        /// single subscription to the source. This is useful for scenarios where you want to avoid multiple
        /// subscriptions to the source sequence or coordinate the timing of subscriptions. The returned connectable
        /// observable is asynchronous and supports concurrent observers.</remarks>
        public ConnectableSignalAsync<T> Publish() =>
            new(source, new SerialSignalAsync<T>());

        /// <summary>
        /// Creates a connectable observable sequence that shares a single subscription to the underlying sequence,
        /// using a Signal created with the specified options.
        /// </summary>
        /// <param name="options">The options used to configure the Signal that will multicast the source sequence. Cannot be null.</param>
        /// <returns>A connectable observable sequence that multicasts the source sequence using a Signal configured with the
        /// specified options.</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="options"/> names an unsupported publishing option.</exception>
        /// <remarks>The returned connectable observable does not begin emitting items until its Connect
        /// method is called. Use this method to control when the subscription to the source sequence starts and to
        /// share the subscription among multiple observers.</remarks>
        public ConnectableSignalAsync<T> Publish(SignalCreationOptions options) =>
            new(source, options switch
            {
                { PublishingOption: PublishingOption.Serial, IsStateless: false } => new SerialSignalAsync<T>(),
                { PublishingOption: PublishingOption.Concurrent, IsStateless: false } => new ConcurrentSignalAsync<T>(),
                { PublishingOption: PublishingOption.Serial, IsStateless: true } => new SerialStatelessSignalAsync<T>(),
                { PublishingOption: PublishingOption.Concurrent, IsStateless: true } =>
                    new ConcurrentStatelessSignalAsync<T>(),
                _ => throw new ArgumentOutOfRangeException(
                    nameof(options),
                    options,
                    "Unsupported signal creation options.")
            });

        /// <summary>
        /// Returns a connectable observable sequence that shares a single subscription to the underlying sequence and
        /// replays the most recent value to new subscribers, starting with the specified initial value.
        /// </summary>
        /// <param name="initialValue">The initial value to be emitted to subscribers before any values are emitted by the source sequence.</param>
        /// <returns>A connectable observable sequence that multicasts the source sequence and replays the latest value, starting
        /// with the specified initial value.</returns>
        /// <remarks>Subscribers will immediately receive the initial value upon subscription, followed by
        /// subsequent values from the source sequence. The returned connectable observable does not begin emitting
        /// values until its Connect method is called.</remarks>
        public ConnectableSignalAsync<T> Publish(T initialValue) =>
            new(source, new SerialReplayLatestSignalAsync<T>(new(initialValue)));

        /// <summary>
        /// Creates a connectable observable sequence that shares a single subscription to the underlying sequence and
        /// starts with the specified initial value.
        /// </summary>
        /// <param name="initialValue">The initial value to be emitted to subscribers before any items are emitted by the source sequence.</param>
        /// <param name="options">The options used to configure the behavior of the underlying behavior Signal.</param>
        /// <returns>A connectable observable sequence that multicasts the source sequence and emits the specified initial value
        /// to new subscribers.</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="options"/> names an unsupported publishing option.</exception>
        /// <remarks>The returned connectable observable will not begin emitting items from the source
        /// sequence until its Connect method is called. Subscribers will immediately receive the most recent value,
        /// starting with the specified initial value, upon subscription.</remarks>
        public ConnectableSignalAsync<T> Publish(T initialValue, BehaviorSignalCreationOptions options) =>
            new(source, options switch
            {
                { PublishingOption: PublishingOption.Serial, IsStateless: false } =>
                    new SerialReplayLatestSignalAsync<T>(new(initialValue)),
                { PublishingOption: PublishingOption.Concurrent, IsStateless: false } =>
                    new ConcurrentReplayLatestSignalAsync<T>(new(initialValue)),
                { PublishingOption: PublishingOption.Serial, IsStateless: true } =>
                    new SerialStatelessReplayLatestSignalAsync<T>(new(initialValue)),
                { PublishingOption: PublishingOption.Concurrent, IsStateless: true } =>
                    new ConcurrentStatelessReplayLatestSignalAsync<T>(new(initialValue)),
                _ => throw new ArgumentOutOfRangeException(
                    nameof(options),
                    options,
                    "Unsupported behavior signal creation options.")
            });

        /// <summary>
        /// Creates a connectable observable sequence that shares a single subscription to the underlying source and
        /// does not retain any state between subscriptions.
        /// </summary>
        /// <returns>A connectable observable sequence that multicasts notifications from the source without retaining state
        /// between subscribers.</returns>
        /// <remarks>Use this method when you want to share a single subscription to the source among
        /// multiple observers, but do not require the observable to cache or replay any items for new subscribers. Each
        /// connection to the returned observable is independent and does not affect subsequent connections.</remarks>
        public ConnectableSignalAsync<T> StatelessPublish() =>
            new(source, new SerialStatelessSignalAsync<T>());

        /// <summary>
        /// Creates a connectable observable sequence that shares a single subscription to the underlying source and
        /// replays the most recent value to new subscribers, starting with the specified initial value.
        /// </summary>
        /// <param name="initialValue">The initial value to be emitted to subscribers before any values are published by the source sequence.</param>
        /// <returns>A connectable observable sequence that multicasts the source sequence and replays the latest value, starting
        /// with the specified initial value.</returns>
        /// <remarks>The returned observable does not maintain any state between connections. Each
        /// connection starts with the provided initial value and only replays the most recent value published during
        /// that connection. This is useful for scenarios where late subscribers should always receive the latest value,
        /// even if they subscribe after the source has started emitting.</remarks>
        public ConnectableSignalAsync<T> StatelessPublish(T initialValue) =>
            new(source, new SerialStatelessReplayLatestSignalAsync<T>(new(initialValue)));

        /// <summary>Creates a connectable observable sequence that replays only the most recent item to new subscribers.</summary>
        /// <returns>A connectable observable sequence that publishes the latest item to current and future subscribers until a
        /// new item is emitted.</returns>
        /// <remarks>This method enables late subscribers to immediately receive the most recently
        /// published value, followed by subsequent values. The returned sequence does not replay earlier items beyond
        /// the latest one. Use this method when you want all subscribers to observe the most recent value, regardless
        /// of when they subscribe.</remarks>
        public ConnectableSignalAsync<T> ReplayLatestPublish() =>
            new(source, new SerialReplayLatestSignalAsync<T>(Optional<T>.Empty));

        /// <summary>
        /// Creates a connectable observable sequence that replays only the latest published value to new subscribers,
        /// using the specified replay Signal creation options.
        /// </summary>
        /// <param name="options">The options used to configure the replay Signal, such as buffer size, scheduler, or other replay behavior
        /// settings.</param>
        /// <returns>A connectable observable sequence that replays the most recent value to each new subscriber after
        /// connection.</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="options"/> names an unsupported publishing option.</exception>
        /// <remarks>Use this method when you want late subscribers to receive only the most recently
        /// published value, rather than the entire sequence or a fixed buffer. The returned connectable observable does
        /// not begin emitting items until its Connect method is called.</remarks>
        public ConnectableSignalAsync<T> ReplayLatestPublish(ReplayLatestSignalCreationOptions options) =>
            new(source, options switch
            {
                { PublishingOption: PublishingOption.Serial, IsStateless: false } =>
                    new SerialReplayLatestSignalAsync<T>(Optional<T>.Empty),
                { PublishingOption: PublishingOption.Concurrent, IsStateless: false } =>
                    new ConcurrentReplayLatestSignalAsync<T>(Optional<T>.Empty),
                { PublishingOption: PublishingOption.Serial, IsStateless: true } =>
                    new SerialStatelessReplayLatestSignalAsync<T>(Optional<T>.Empty),
                { PublishingOption: PublishingOption.Concurrent, IsStateless: true } =>
                    new ConcurrentStatelessReplayLatestSignalAsync<T>(Optional<T>.Empty),
                _ => throw new ArgumentOutOfRangeException(
                    nameof(options),
                    options,
                    "Unsupported replay-latest signal creation options.")
            });

        /// <summary>
        /// Creates a connectable observable sequence that replays only the latest item to new subscribers and publishes
        /// items to all current subscribers.
        /// </summary>
        /// <returns>A connectable observable sequence that replays the most recent item to new subscribers and multicasts
        /// notifications to all current subscribers.</returns>
        /// <remarks>This method is stateless; each call returns a new connectable observable. Subscribers
        /// that connect after an item has been published will immediately receive the latest item. This is useful for
        /// scenarios where late subscribers should catch up with the most recent value without receiving the full
        /// history.</remarks>
        public ConnectableSignalAsync<T> StatelessReplayLatestPublish() =>
            new(source, new SerialStatelessReplayLatestSignalAsync<T>(Optional<T>.Empty));
    }
}
