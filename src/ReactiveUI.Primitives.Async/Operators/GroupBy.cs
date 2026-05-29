// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Internals;
using ReactiveUI.Primitives.Async.Subjects;
using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides extension methods for creating and manipulating asynchronous observable sequences.
/// </summary>
/// <remarks>The methods in this class enable advanced operations on asynchronous observables, such as grouping
/// elements by key. These extensions are intended for use with types implementing asynchronous observation patterns,
/// allowing developers to compose and transform streams of data in a reactive manner.</remarks>
public static partial class ObservableAsync
{
    /// <summary>
    /// Groups the elements of an asynchronous observable sequence according to a specified key selector function.
    /// </summary>
    /// <remarks>Each group in the resulting sequence corresponds to a unique key produced by the key
    /// selector. The groups are emitted as soon as their first element is encountered in the source sequence. The
    /// returned grouped observables can be subscribed to independently.</remarks>
    /// <typeparam name="TValue">The type of elements in the source sequence.</typeparam>
    /// <typeparam name="TKey">The type of the key returned by the key selector function. Must be non-nullable.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="keySelector">A function to extract the key for each element in the source sequence.</param>
    /// <returns>An asynchronous observable sequence of grouped observables, each containing elements that share a common key.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="keySelector"/> is null.</exception>
    public static IObservableAsync<GroupedAsyncObservable<TKey, TValue>> GroupBy<TValue, TKey>(this IObservableAsync<TValue> source, Func<TValue, TKey> keySelector)
        where TKey : notnull
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(keySelector);

        return new GroupByAsyncObservable<TKey, TValue>(
            source,
            keySelector,
            static _ => SubjectAsync.Create<TValue>());
    }

    /// <summary>
    /// Groups the elements of an asynchronous observable sequence according to a specified key selector function and
    /// returns an observable sequence of grouped observables.
    /// </summary>
    /// <remarks>Each group in the resulting sequence is represented by a <see
    /// cref="GroupedAsyncObservable{TKey, TValue}"/>, which exposes the group's key and an observable sequence of its
    /// elements. The <paramref name="groupSubjectSelector"/> parameter allows customization of the subject used for
    /// each group, which can affect how elements are buffered or multicast within the group.</remarks>
    /// <typeparam name="TValue">The type of elements in the source sequence.</typeparam>
    /// <typeparam name="TKey">The type of the key returned by the key selector function. Must be non-null.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="keySelector">A function to extract the key for each element in the source sequence.</param>
    /// <param name="groupSubjectSelector">A function that provides a subject for each group, given its key. Used to control how elements are published
    /// within each group.</param>
    /// <returns>An asynchronous observable sequence containing grouped observables, each representing a collection of elements
    /// that share a common key.</returns>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="keySelector"/> is null.</exception>
    public static IObservableAsync<GroupedAsyncObservable<TKey, TValue>> GroupBy<TValue, TKey>(
        this IObservableAsync<TValue> source,
        Func<TValue, TKey> keySelector,
        Func<TKey, ISubjectAsync<TValue>> groupSubjectSelector)
        where TKey : notnull
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(keySelector);

        return new GroupByAsyncObservable<TKey, TValue>(source, keySelector, groupSubjectSelector);
    }

    /// <summary>
    /// Async observable that groups source elements by key, emitting a <see cref="GroupedAsyncObservable{TKey, TValue}"/>
    /// for each unique key encountered.
    /// </summary>
    /// <typeparam name="TKey">The type of the grouping key.</typeparam>
    /// <typeparam name="TValue">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="keySelector">A function to extract the key for each element.</param>
    /// <param name="groupSubjectSelector">A function that provides a subject for each group, given its key.</param>
    internal sealed class GroupByAsyncObservable<TKey, TValue>(
        IObservableAsync<TValue> source,
        Func<TValue, TKey> keySelector,
        Func<TKey, ISubjectAsync<TValue>> groupSubjectSelector) : ObservableAsync<GroupedAsyncObservable<TKey, TValue>>
        where TKey : notnull
    {
        /// <summary>
        /// The source observable sequence whose elements are grouped by key.
        /// </summary>
        private readonly IObservableAsync<TValue> _source = source;

        /// <summary>
        /// The function used to extract the grouping key from each source element.
        /// </summary>
        private readonly Func<TValue, TKey> _keySelector = keySelector;

        /// <summary>
        /// The factory function that creates a subject for each new group key.
        /// </summary>
        private readonly Func<TKey, ISubjectAsync<TValue>> _groupSubjectSelector = groupSubjectSelector;

        /// <summary>
        /// Subscribes the specified observer by creating a <see cref="Subscription"/> that tracks groups by key.
        /// </summary>
        /// <param name="observer">The observer to receive grouped observable sequences.</param>
        /// <param name="cancellationToken">A token to cancel the subscription.</param>
        /// <returns>An async disposable that tears down the subscription when disposed.</returns>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<GroupedAsyncObservable<TKey, TValue>> observer,
            CancellationToken cancellationToken)
        {
            var subscription = new Subscription(this, observer);
            try
            {
                return await subscription.SubscribeSourcesAsync(cancellationToken).ConfigureAwait(false);
            }
            catch
            {
                await subscription.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }

        /// <summary>
        /// Observer subscription that tracks groups by key, creating new grouped observables as new keys are encountered.
        /// </summary>
        /// <param name="parent">The parent GroupBy observable that provides the key selector and subject factory.</param>
        /// <param name="observer">The downstream observer to receive grouped observables.</param>
        internal sealed class Subscription(
            GroupByAsyncObservable<TKey, TValue> parent,
            IObserverAsync<GroupedAsyncObservable<TKey, TValue>> observer) : ObserverAsync<TValue>
        {
            /// <summary>
            /// The composite disposable that tracks all group subscription disposables.
            /// </summary>
            private readonly CompositeDisposableAsync _disposables = new();

            /// <summary>
            /// A dictionary mapping each encountered key to its corresponding group subject.
            /// </summary>
            private Dictionary<TKey, ISubjectAsync<TValue>> _subjectsByKey = [];

            /// <summary>
            /// Subscribes this observer to the parent's source sequence.
            /// </summary>
            /// <param name="cancellationToken">A token to cancel the subscription.</param>
            /// <returns>An async disposable representing the source subscription.</returns>
            public ValueTask<IAsyncDisposable> SubscribeSourcesAsync(CancellationToken cancellationToken) =>
                parent._source.SubscribeAsync(this, cancellationToken);

            /// <summary>
            /// Routes the element to the appropriate group subject, creating a new group if the key is new.
            /// </summary>
            /// <param name="value">The element to route.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override async ValueTask OnNextAsyncCore(TValue value, CancellationToken cancellationToken)
            {
                var key = parent._keySelector(value);
                if (!_subjectsByKey.TryGetValue(key, out var subject))
                {
                    subject = parent._groupSubjectSelector(key);
                    _subjectsByKey.Add(key, subject);

                    // We use the cancellationToken passed from the source subscription.
                    await observer.OnNextAsync(new Observable(this, key, subject.Values), cancellationToken).ConfigureAwait(false);
                }

                await subject.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
            }

            /// <summary>
            /// Forwards a non-fatal error to the downstream observer.
            /// </summary>
            /// <param name="error">The error to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                observer.OnErrorResumeAsync(error, cancellationToken);

            /// <summary>
            /// Completes all group subjects and then completes the downstream observer.
            /// </summary>
            /// <param name="result">The completion result.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override async ValueTask OnCompletedAsyncCore(Result result)
            {
                var subjects = _subjectsByKey.Values;
                _subjectsByKey = null!;
                foreach (var subject in subjects)
                {
                    await subject.OnCompletedAsync(result).ConfigureAwait(false);
                }

                await observer.OnCompletedAsync(result).ConfigureAwait(false);
            }

            /// <summary>
            /// Disposes all tracked group subscriptions.
            /// </summary>
            /// <returns>A task representing the asynchronous disposal operation.</returns>
            protected override async ValueTask DisposeAsyncCore()
            {
                await base.DisposeAsyncCore().ConfigureAwait(false);
                await _disposables.DisposeAsync().ConfigureAwait(false);
            }

            /// <summary>
            /// Represents a single grouped async observable identified by its key.
            /// </summary>
            /// <param name="parent">The parent subscription that manages group disposables.</param>
            /// <param name="key">The key that identifies this group.</param>
            /// <param name="subjectValues">The observable sequence of values for this group.</param>
            internal sealed class Observable(Subscription parent, TKey key, IObservableAsync<TValue> subjectValues)
                : GroupedAsyncObservable<TKey, TValue>
            {
                /// <summary>
                /// Gets the key associated with this element.
                /// </summary>
                public override TKey Key => key;

                /// <summary>
                /// Subscribes the specified observer to this group's value stream.
                /// </summary>
                /// <param name="observer">The observer to receive elements for this group.</param>
                /// <param name="cancellationToken">A token to cancel the subscription.</param>
                /// <returns>An async disposable that removes the subscription from the parent on disposal.</returns>
                protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
                    IObserverAsync<TValue> observer,
                    CancellationToken cancellationToken)
                {
                    // Inline-create the wrap so we can wire chain-aware cancellation. The wrap
                    // observes the upstream Subscription's dispose token (broadcasts arrive with
                    // that token); the downstream observer (if an ObserverAsync) observes the
                    // wrap's dispose token. Together they collapse the per-emission
                    // CancellationTokenSource.CreateLinkedTokenSource allocations to zero.
                    var wrap = new WrappedObserverAsync<TValue>(observer);
                    wrap.LinkUpstreamCancellation(parent.InternalDisposedToken);
                    if (observer is ObserverAsync<TValue> downstream)
                    {
                        downstream.LinkUpstreamCancellation(wrap.InternalDisposedToken);
                    }

                    var subscription = await subjectValues.SubscribeAsync(wrap, cancellationToken).ConfigureAwait(false);
                    await parent._disposables.AddAsync(subscription).ConfigureAwait(false);
                    return DisposableAsync.Create(async () =>
                    {
                        await parent._disposables.Remove(subscription).ConfigureAwait(false);
                        await subscription.DisposeAsync().ConfigureAwait(false);
                    });
                }
            }
        }
    }
}
