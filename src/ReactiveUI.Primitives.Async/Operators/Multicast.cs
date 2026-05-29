// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internals;
using ReactiveUI.Primitives.Async.Subjects;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides a set of extension methods for creating and managing connectable asynchronous observables using various
/// subject types.
/// </summary>
/// <remarks>The methods in this class enable advanced multicasting scenarios for asynchronous observables,
/// allowing multiple subscribers to share a single subscription to the underlying data source. These methods support
/// different subject types and configuration options, including stateless and replay behaviors, to accommodate a wide
/// range of reactive programming patterns.</remarks>
public static partial class ObservableAsync
{
    /// <summary>
    /// Cached creation options for stateless subject publishing.
    /// </summary>
    private static readonly SubjectCreationOptions _statelessPublishOptions = SubjectCreationOptions.Default with
    {
        IsStateless = true
    };

    /// <summary>
    /// Cached creation options for stateless behavior subject publishing.
    /// </summary>
    private static readonly BehaviorSubjectCreationOptions _statelessBehaviorPublishOptions =
        BehaviorSubjectCreationOptions.Default with { IsStateless = true };

    /// <summary>
    /// Cached creation options for stateless replay-latest subject publishing.
    /// </summary>
    private static readonly ReplayLatestSubjectCreationOptions _statelessReplayLatestPublishOptions =
        ReplayLatestSubjectCreationOptions.Default with { IsStateless = true };

    /// <summary>
    /// Creates a connectable observable sequence that shares a single subscription to the underlying sequence using
    /// the specified subject.
    /// </summary>
    /// <remarks>The returned connectable observable will not begin emitting items until its Connect
    /// method is called. This allows multiple observers to subscribe before the sequence starts.</remarks>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="subject">The subject used to multicast the elements of the source sequence to multiple observers. Cannot be null.</param>
    /// <returns>A connectable observable sequence that multicasts the source sequence through the specified subject.</returns>
    public static ConnectableObservableAsync<T> Multicast<T>(this IObservableAsync<T> source, ISubjectAsync<T> subject) =>
        new MulticastObservableAsync<T>(source, subject);

    /// <summary>
    /// Returns a connectable observable sequence that shares a single subscription to the underlying asynchronous
    /// observable. Observers will receive all notifications published after they subscribe.
    /// </summary>
    /// <remarks>Use this method to create a hot observable that allows multiple observers to share a
    /// single subscription to the source. This is useful for scenarios where you want to avoid multiple
    /// subscriptions to the source sequence or coordinate the timing of subscriptions. The returned connectable
    /// observable is asynchronous and supports concurrent observers.</remarks>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A connectable observable sequence that multicasts notifications to all subscribed observers. The sequence
    /// does not begin emitting items until its Connect method is called.</returns>
    public static ConnectableObservableAsync<T> Publish<T>(this IObservableAsync<T> source) => source.Multicast(SubjectAsync.Create<T>());

    /// <summary>
    /// Creates a connectable observable sequence that shares a single subscription to the underlying sequence,
    /// using a subject created with the specified options.
    /// </summary>
    /// <remarks>The returned connectable observable does not begin emitting items until its Connect
    /// method is called. Use this method to control when the subscription to the source sequence starts and to
    /// share the subscription among multiple observers.</remarks>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="options">The options used to configure the subject that will multicast the source sequence. Cannot be null.</param>
    /// <returns>A connectable observable sequence that multicasts the source sequence using a subject configured with the
    /// specified options.</returns>
    public static ConnectableObservableAsync<T> Publish<T>(this IObservableAsync<T> source, SubjectCreationOptions options) =>
        source.Multicast(SubjectAsync.Create<T>(options));

    /// <summary>
    /// Returns a connectable observable sequence that shares a single subscription to the underlying sequence and
    /// replays the most recent value to new subscribers, starting with the specified initial value.
    /// </summary>
    /// <remarks>Subscribers will immediately receive the initial value upon subscription, followed by
    /// subsequent values from the source sequence. The returned connectable observable does not begin emitting
    /// values until its Connect method is called.</remarks>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="initialValue">The initial value to be emitted to subscribers before any values are emitted by the source sequence.</param>
    /// <returns>A connectable observable sequence that multicasts the source sequence and replays the latest value, starting
    /// with the specified initial value.</returns>
    public static ConnectableObservableAsync<T> Publish<T>(this IObservableAsync<T> source, T initialValue) =>
        source.Multicast(SubjectAsync.CreateBehavior(initialValue));

    /// <summary>
    /// Creates a connectable observable sequence that shares a single subscription to the underlying sequence and
    /// starts with the specified initial value.
    /// </summary>
    /// <remarks>The returned connectable observable will not begin emitting items from the source
    /// sequence until its Connect method is called. Subscribers will immediately receive the most recent value,
    /// starting with the specified initial value, upon subscription.</remarks>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="initialValue">The initial value to be emitted to subscribers before any items are emitted by the source sequence.</param>
    /// <param name="options">The options used to configure the behavior of the underlying behavior subject.</param>
    /// <returns>A connectable observable sequence that multicasts the source sequence and emits the specified initial value
    /// to new subscribers.</returns>
    public static ConnectableObservableAsync<T> Publish<T>(this IObservableAsync<T> source, T initialValue, BehaviorSubjectCreationOptions options) =>
        source.Multicast(SubjectAsync.CreateBehavior(initialValue, options));

    /// <summary>
    /// Creates a connectable observable sequence that shares a single subscription to the underlying source and
    /// does not retain any state between subscriptions.
    /// </summary>
    /// <remarks>Use this method when you want to share a single subscription to the source among
    /// multiple observers, but do not require the observable to cache or replay any items for new subscribers. Each
    /// connection to the returned observable is independent and does not affect subsequent connections.</remarks>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A connectable observable sequence that multicasts notifications from the source without retaining state
    /// between subscribers.</returns>
    public static ConnectableObservableAsync<T> StatelessPublish<T>(this IObservableAsync<T> source) =>
        source.Multicast(SubjectAsync.Create<T>(_statelessPublishOptions));

    /// <summary>
    /// Creates a connectable observable sequence that shares a single subscription to the underlying source and
    /// replays the most recent value to new subscribers, starting with the specified initial value.
    /// </summary>
    /// <remarks>The returned observable does not maintain any state between connections. Each
    /// connection starts with the provided initial value and only replays the most recent value published during
    /// that connection. This is useful for scenarios where late subscribers should always receive the latest value,
    /// even if they subscribe after the source has started emitting.</remarks>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="initialValue">The initial value to be emitted to subscribers before any values are published by the source sequence.</param>
    /// <returns>A connectable observable sequence that multicasts the source sequence and replays the latest value, starting
    /// with the specified initial value.</returns>
    public static ConnectableObservableAsync<T> StatelessPublish<T>(this IObservableAsync<T> source, T initialValue) =>
        source.Multicast(SubjectAsync.CreateBehavior(initialValue, _statelessBehaviorPublishOptions));

    /// <summary>
    /// Creates a connectable observable sequence that replays only the most recent item to new subscribers.
    /// </summary>
    /// <remarks>This method enables late subscribers to immediately receive the most recently
    /// published value, followed by subsequent values. The returned sequence does not replay earlier items beyond
    /// the latest one. Use this method when you want all subscribers to observe the most recent value, regardless
    /// of when they subscribe.</remarks>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A connectable observable sequence that publishes the latest item to current and future subscribers until a
    /// new item is emitted.</returns>
    public static ConnectableObservableAsync<T> ReplayLatestPublish<T>(this IObservableAsync<T> source) =>
        source.Multicast(SubjectAsync.CreateReplayLatest<T>());

    /// <summary>
    /// Creates a connectable observable sequence that replays only the latest published value to new subscribers,
    /// using the specified replay subject creation options.
    /// </summary>
    /// <remarks>Use this method when you want late subscribers to receive only the most recently
    /// published value, rather than the entire sequence or a fixed buffer. The returned connectable observable does
    /// not begin emitting items until its Connect method is called.</remarks>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="options">The options used to configure the replay subject, such as buffer size, scheduler, or other replay behavior
    /// settings.</param>
    /// <returns>A connectable observable sequence that replays the most recent value to each new subscriber after
    /// connection.</returns>
    public static ConnectableObservableAsync<T> ReplayLatestPublish<T>(this IObservableAsync<T> source, ReplayLatestSubjectCreationOptions options) =>
        source.Multicast(SubjectAsync.CreateReplayLatest<T>(options));

    /// <summary>
    /// Creates a connectable observable sequence that replays only the latest item to new subscribers and publishes
    /// items to all current subscribers.
    /// </summary>
    /// <remarks>This method is stateless; each call returns a new connectable observable. Subscribers
    /// that connect after an item has been published will immediately receive the latest item. This is useful for
    /// scenarios where late subscribers should catch up with the most recent value without receiving the full
    /// history.</remarks>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <returns>A connectable observable sequence that replays the most recent item to new subscribers and multicasts
    /// notifications to all current subscribers.</returns>
    public static ConnectableObservableAsync<T> StatelessReplayLatestPublish<T>(this IObservableAsync<T> source) =>
        source.Multicast(SubjectAsync.CreateReplayLatest<T>(_statelessReplayLatestPublishOptions));
}
