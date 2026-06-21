// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;
using AsyncSignalFactory = ReactiveUI.Primitives.Async.Signals.Signal;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for creating and manipulating asynchronous observable sequences.</summary>
/// <remarks>The methods in this class enable advanced operations on asynchronous observables, such as grouping
/// elements by key. These extensions are intended for use with types implementing asynchronous observation patterns,
/// allowing developers to compose and transform streams of data in a reactive manner.</remarks>
public static partial class SignalAsyncExtensions
{
    /// <summary>Grouping operators for an observable source sequence.</summary>
    /// <param name="source">The source observable sequence.</param>
    /// <typeparam name="TValue">The type of elements in the source sequence.</typeparam>
    extension<TValue>(IObservableAsync<TValue> source)
    {
        /// <summary>Groups the elements of an asynchronous observable sequence according to a specified key selector function.</summary>
        /// <remarks>Each group in the resulting sequence corresponds to a unique key produced by the key
        /// selector. The groups are emitted as soon as their first element is encountered in the source sequence. The
        /// returned grouped observables can be subscribed to independently.</remarks>
        /// <typeparam name="TKey">The type of the key returned by the key selector function. Must be non-nullable.</typeparam>
        /// <param name="keySelector">A function to extract the key for each element in the source sequence.</param>
        /// <returns>An asynchronous observable sequence of grouped observables, each containing elements that share a common key.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="keySelector"/> is null.</exception>
        public IObservableAsync<GroupedAsyncSignal<TKey, TValue>> GroupBy<TKey>(Func<TValue, TKey> keySelector)
            where TKey : notnull
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(keySelector);

            return new GroupByAsyncSignal<TKey, TValue>(source, keySelector, static _ => AsyncSignalFactory.Create<TValue>());
        }

        /// <summary>
        /// Groups the elements of an asynchronous observable sequence according to a specified key selector function and
        /// returns an observable sequence of grouped observables.
        /// </summary>
        /// <remarks>Each group in the resulting sequence is represented by a <see
        /// cref="GroupedAsyncSignal{TKey, TValue}"/>, which exposes the group's key and an observable sequence of its
        /// elements. The <paramref name="groupSignalSelector"/> parameter allows customization of the signal used for
        /// each group, which can affect how elements are buffered or multicast within the group.</remarks>
        /// <typeparam name="TKey">The type of the key returned by the key selector function. Must be non-null.</typeparam>
        /// <param name="keySelector">A function to extract the key for each element in the source sequence.</param>
        /// <param name="groupSignalSelector">A function that provides a signal for each group, given its key. Used to control how elements are published
        /// within each group.</param>
        /// <returns>An asynchronous observable sequence containing grouped observables, each representing a collection of elements
        /// that share a common key.</returns>
        /// <exception cref="ArgumentExceptionHelper">Thrown if <paramref name="keySelector"/> is null.</exception>
        public IObservableAsync<GroupedAsyncSignal<TKey, TValue>> GroupBy<TKey>(
            Func<TValue, TKey> keySelector,
            Func<TKey, ISignalAsync<TValue>> groupSignalSelector)
            where TKey : notnull
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(keySelector);

            return new GroupByAsyncSignal<TKey, TValue>(source, keySelector, groupSignalSelector);
        }
    }

    /// <summary>Async observable that groups source elements by key, emitting one observable per unique key.</summary>
    /// <typeparam name="TKey">The type of the grouping key.</typeparam>
    /// <typeparam name="TValue">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="keySelector">A function to extract the key for each element.</param>
    /// <param name="groupSignalSelector">A function that provides a signal for each group, given its key.</param>
    internal sealed class GroupByAsyncSignal<TKey, TValue>(
        IObservableAsync<TValue> source,
        Func<TValue, TKey> keySelector,
        Func<TKey, ISignalAsync<TValue>> groupSignalSelector)
        : IObservableAsync<GroupedAsyncSignal<TKey, TValue>>
        where TKey : notnull
    {
        /// <summary>The source observable sequence whose elements are grouped by key.</summary>
        private readonly IObservableAsync<TValue> _source = source;

        /// <summary>The function used to extract the grouping key from each source element.</summary>
        private readonly Func<TValue, TKey> _keySelector = keySelector;

        /// <summary>The factory function that creates a signal for each new group key.</summary>
        private readonly Func<TKey, ISignalAsync<TValue>> _groupSignalSelector = groupSignalSelector;

        async ValueTask<IAsyncDisposable> IObservableAsync<GroupedAsyncSignal<TKey, TValue>>.SubscribeAsync(
            IObserverAsync<GroupedAsyncSignal<TKey, TValue>> observer,
            CancellationToken cancellationToken)
        {
            GroupingCoordinator subscription = new(this, observer);
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

        /// <summary>Observer subscription that tracks groups by key, creating new grouped observables as new keys are encountered.</summary>
        /// <param name="parent">The parent GroupBy observable that provides the key selector and signal factory.</param>
        /// <param name="observer">The downstream observer to receive grouped observables.</param>
        internal sealed class GroupingCoordinator(
            GroupByAsyncSignal<TKey, TValue> parent,
            IObserverAsync<GroupedAsyncSignal<TKey, TValue>> observer) : WitnessAsync<TValue>
        {
            /// <summary>The composite disposable that tracks all group subscription disposables.</summary>
            private readonly MultipleDisposableAsync _disposables = new();

            /// <summary>A dictionary mapping each encountered key to its corresponding group signal.</summary>
            private Dictionary<TKey, ISignalAsync<TValue>> _signalsByKey = [];

            /// <summary>Subscribes this observer to the parent's source sequence.</summary>
            /// <param name="cancellationToken">A token to cancel the subscription.</param>
            /// <returns>An async disposable representing the source subscription.</returns>
            public ValueTask<IAsyncDisposable> SubscribeSourcesAsync(CancellationToken cancellationToken) =>
                parent._source.SubscribeAsync(this, cancellationToken);

            /// <summary>Routes the element to the appropriate group signal, creating a new group if the key is new.</summary>
            /// <param name="value">The element to route.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override async ValueTask OnNextAsyncCore(TValue value, CancellationToken cancellationToken)
            {
                var key = parent._keySelector(value);
                if (!_signalsByKey.TryGetValue(key, out var signal))
                {
                    signal = parent._groupSignalSelector(key);
                    _signalsByKey.Add(key, signal);

                    // We use the cancellationToken passed from the source subscription.
                    await observer.OnNextAsync(
                        new GroupedAsyncSignal<TKey, TValue>(
                            key,
                            signal.Values,
                            _disposables,
                            InternalDisposedToken),
                        cancellationToken).ConfigureAwait(false);
                }

                await signal.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
            }

            /// <summary>Forwards a non-fatal error to the downstream observer.</summary>
            /// <param name="error">The error to forward.</param>
            /// <param name="cancellationToken">A token to cancel the operation.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                observer.OnErrorResumeAsync(error, cancellationToken);

            /// <summary>Completes all group signals and then completes the downstream observer.</summary>
            /// <param name="result">The completion result.</param>
            /// <returns>A task representing the asynchronous operation.</returns>
            protected override async ValueTask OnCompletedAsyncCore(Result result)
            {
                var signals = _signalsByKey.Values;
                _signalsByKey = null!;
                foreach (var signal in signals)
                {
                    await signal.OnCompletedAsync(result).ConfigureAwait(false);
                }

                await observer.OnCompletedAsync(result).ConfigureAwait(false);
            }

            /// <summary>Disposes all tracked group subscriptions.</summary>
            /// <returns>A task representing the asynchronous disposal operation.</returns>
            protected override async ValueTask DisposeAsyncCore()
            {
                await base.DisposeAsyncCore().ConfigureAwait(false);
                await _disposables.DisposeAsync().ConfigureAwait(false);
            }
        }
    }
}
