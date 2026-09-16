// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides a set of extension methods for creating and managing connectable asynchronous observables using various Signal types.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Multicasting and publishing operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Creates a connectable observable sequence that shares a single subscription to the underlying sequence using the specified Signal.</summary>
        /// <param name="signal">The signal that multicasts the source elements to multiple observers. Cannot be null.</param>
        /// <returns>A connectable observable sequence that multicasts the source sequence through the specified signal.</returns>
        /// <remarks>The source is not subscribed until Connect is called, so observers can subscribe first.</remarks>
        public ConnectableSignalAsync<T> Multicast(ISignalAsync<T> signal) =>
            new(source, signal);

        /// <summary>Shares one source subscription, forwarding live notifications to connected observers.</summary>
        /// <returns>A connectable observable sequence that multicasts notifications to all subscribed observers. The sequence
        /// does not begin emitting items until its Connect method is called.</returns>
        /// <remarks>Notifications are delivered to observers serially.</remarks>
        public ConnectableSignalAsync<T> Publish() =>
            new(source, new SerialSignalAsync<T>());

        /// <summary>Creates a connectable observable sequence that shares a single subscription to the underlying sequence, using a Signal created with the specified options.</summary>
        /// <param name="options">The options that configure the multicasting signal. Cannot be null.</param>
        /// <returns>A connectable observable sequence that multicasts the source sequence using a Signal configured with the
        /// specified options.</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="options"/> names an unsupported publishing option.</exception>
        /// <remarks>The source is not subscribed until Connect is called.</remarks>
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
        /// <remarks>A subscriber receives the latest value - the initial value until the source publishes one - and the
        /// source is not subscribed until Connect is called.</remarks>
        public ConnectableSignalAsync<T> Publish(T initialValue) =>
            new(source, new SerialReplayLatestSignalAsync<T>(new(initialValue)));

        /// <summary>Creates a connectable observable sequence that shares a single subscription to the underlying sequence and starts with the specified initial value.</summary>
        /// <param name="initialValue">The initial value to be emitted to subscribers before any items are emitted by the source sequence.</param>
        /// <param name="options">The options that configure the underlying behavior signal.</param>
        /// <returns>A connectable observable sequence that multicasts the source sequence and emits the specified initial value
        /// to new subscribers.</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="options"/> names an unsupported publishing option.</exception>
        /// <remarks>A subscriber receives the latest value - the initial value until the source publishes one - and the
        /// source is not subscribed until Connect is called.</remarks>
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

        /// <summary>Creates a connectable observable sequence that shares a single subscription to the underlying source and does not retain any state between subscriptions.</summary>
        /// <returns>A connectable observable sequence that multicasts notifications from the source without retaining state
        /// between subscribers.</returns>
        /// <remarks>Each connection is independent of the ones around it, and no value is cached for late subscribers.</remarks>
        public ConnectableSignalAsync<T> StatelessPublish() =>
            new(source, new SerialStatelessSignalAsync<T>());

        /// <summary>
        /// Creates a connectable observable sequence that shares a single subscription to the underlying source and
        /// replays the most recent value to new subscribers, starting with the specified initial value.
        /// </summary>
        /// <param name="initialValue">The initial value to be emitted to subscribers before any values are published by the source sequence.</param>
        /// <returns>A connectable observable sequence that multicasts the source sequence and replays the latest value, starting
        /// with the specified initial value.</returns>
        /// <remarks>Each connection starts from the initial value and replays only the most recent value published
        /// during that connection; nothing is retained between connections.</remarks>
        public ConnectableSignalAsync<T> StatelessPublish(T initialValue) =>
            new(source, new SerialStatelessReplayLatestSignalAsync<T>(new(initialValue)));

        /// <summary>Creates a connectable observable sequence that replays only the most recent item to new subscribers.</summary>
        /// <returns>A connectable observable sequence that publishes the latest item to current and future subscribers until a
        /// new item is emitted.</returns>
        /// <remarks>A late subscriber receives the most recent value on subscription; earlier values are not replayed.</remarks>
        public ConnectableSignalAsync<T> ReplayLatestPublish() =>
            new(source, new SerialReplayLatestSignalAsync<T>(Optional<T>.Empty));

        /// <summary>Creates a connectable observable sequence that replays only the latest published value to new subscribers, using the specified replay Signal creation options.</summary>
        /// <param name="options">The options that configure the replay signal.</param>
        /// <returns>A connectable observable sequence that replays the most recent value to each new subscriber after
        /// connection.</returns>
        /// <exception cref="ArgumentOutOfRangeException"><paramref name="options"/> names an unsupported publishing option.</exception>
        /// <remarks>A late subscriber receives the most recent value only, and the source is not subscribed until
        /// Connect is called.</remarks>
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

        /// <summary>Creates a connectable observable sequence that replays only the latest item to new subscribers and publishes items to all current subscribers.</summary>
        /// <returns>A connectable observable sequence that replays the most recent item to new subscribers and multicasts
        /// notifications to all current subscribers.</returns>
        /// <remarks>Each connection replays only the most recent value published during that connection; nothing is
        /// retained between connections.</remarks>
        public ConnectableSignalAsync<T> StatelessReplayLatestPublish() =>
            new(source, new SerialStatelessReplayLatestSignalAsync<T>(Optional<T>.Empty));
    }
}
