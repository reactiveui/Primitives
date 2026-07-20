// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Predicate-driven stop signals backing the <c>TakeUntil(predicate)</c> overloads declared in
/// <c>TakeUntil.cs</c>. Unlike the other take-until signals these have no second sequence, task or
/// token to race against: the stop condition is evaluated inline on each source element.
/// </summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Async observable that emits items from the source until the specified predicate returns true.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">The predicate that signals when to stop emitting items.</param>
    internal sealed class PredicateStopSignal<T>(IObservableAsync<T> source, Func<T, bool> predicate)
        : IObservableAsync<T>
    {
        /// <summary>The predicate that signals when to stop emitting items.</summary>
        private readonly Func<T, bool> _predicate = predicate;

        /// <summary>The source observable sequence.</summary>
        private readonly IObservableAsync<T> _source = source;

        /// <inheritdoc/>
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            PredicateStopCoordinator subscription = new(this, observer);
            return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
                subscription,
                () => subscription.SubscribeSourcesAsync(cancellationToken));
        }

        /// <summary>Observer that forwards items from the source until the predicate returns true.</summary>
        /// <param name="parent">The parent observable that owns this subscription.</param>
        /// <param name="observer">The downstream observer to forward items to.</param>
        internal sealed class PredicateStopCoordinator(PredicateStopSignal<T> parent, IObserverAsync<T> observer)
            : WitnessAsync<T>
        {
            /// <summary>The inner subscription handle.</summary>
            private IAsyncDisposable? _subscription;

            /// <summary>Subscribes to the source observable.</summary>
            /// <param name="cancellationToken">A token to cancel the subscription.</param>
            /// <returns>A task representing the asynchronous subscribe operation.</returns>
            internal async ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken) =>
                _subscription = await parent._source.SubscribeAsync(this, cancellationToken).ConfigureAwait(false);

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
                parent._predicate(value)
                    ? OnCompletedAsyncCore(Result.Success)
                    : observer.OnNextAsync(value, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                observer.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) => observer.OnCompletedAsync(result);

            /// <inheritdoc/>
            protected override async ValueTask DisposeAsyncCore()
            {
                if (_subscription is not null)
                {
                    await _subscription.DisposeAsync().ConfigureAwait(false);
                }

                await base.DisposeAsyncCore().ConfigureAwait(false);
            }
        }
    }

    /// <summary>Emits source items until an async predicate returns true.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="asyncPredicate">Predicate that signals when to stop.</param>
    internal sealed class AsyncPredicateStopSignal<T>(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask<bool>> asyncPredicate) : IObservableAsync<T>
    {
        /// <summary>The async predicate that signals when to stop emitting items.</summary>
        private readonly Func<T, CancellationToken, ValueTask<bool>> _asyncPredicate = asyncPredicate;

        /// <summary>The source observable sequence.</summary>
        private readonly IObservableAsync<T> _source = source;

        /// <inheritdoc/>
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            AsyncPredicateStopCoordinator subscription = new(this, observer);
            return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
                subscription,
                () => subscription.SubscribeSourcesAsync(cancellationToken));
        }

        /// <summary>Forwards source items until the async predicate returns true.</summary>
        /// <param name="parent">The owning signal.</param>
        /// <param name="observer">The downstream observer.</param>
        internal sealed class AsyncPredicateStopCoordinator(
            AsyncPredicateStopSignal<T> parent,
            IObserverAsync<T> observer) : WitnessAsync<T>
        {
            /// <summary>The inner subscription handle.</summary>
            private IAsyncDisposable? _subscription;

            /// <summary>Subscribes to the source observable.</summary>
            /// <param name="cancellationToken">A token to cancel the subscription.</param>
            /// <returns>A task representing the asynchronous subscribe operation.</returns>
            internal async ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken) =>
                _subscription = await parent._source.SubscribeAsync(this, cancellationToken).ConfigureAwait(false);

            /// <inheritdoc/>
            protected override async ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (await parent._asyncPredicate(value, cancellationToken).ConfigureAwait(false))
                {
                    await OnCompletedAsyncCore(Result.Success).ConfigureAwait(false);
                    return;
                }

                await observer.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
            }

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                observer.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) => observer.OnCompletedAsync(result);

            /// <inheritdoc/>
            protected override async ValueTask DisposeAsyncCore()
            {
                if (_subscription is not null)
                {
                    await _subscription.DisposeAsync().ConfigureAwait(false);
                }

                await base.DisposeAsyncCore().ConfigureAwait(false);
            }
        }
    }
}
