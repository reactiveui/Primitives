// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides the predicate-driven stop signals that evaluate their stop condition inline on each source element.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Async observable that emits items from the source until the specified predicate returns true.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    /// <param name="predicate">The predicate that signals when to stop emitting items.</param>
    internal sealed class PredicateStopSignal<T>(IObservableAsync<T> source, Func<T, bool> predicate) : IObservableAsync<T>
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
        [DebuggerDisplay("PredicateStopCoordinator: {_witness}")]
        internal sealed class PredicateStopCoordinator(PredicateStopSignal<T> parent, IObserverAsync<T> observer) : IWitnessAsync<T>
        {
            /// <summary>The inner subscription handle.</summary>
            private IAsyncDisposable? _subscription;

            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness;

            /// <inheritdoc/>
            ref WitnessAsyncState IWitnessState.Witness => ref _witness;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
                WitnessAsync.OnNextAsync(this, value, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
                WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

            /// <inheritdoc/>
            public async ValueTask DisposeAsync()
            {
                if (_subscription is not null)
                {
                    await _subscription.DisposeAsync().ConfigureAwait(false);
                }

                await WitnessAsync.DisposeStateAsync(this).ConfigureAwait(false);
            }

            /// <summary>Subscribes to the source observable.</summary>
            /// <param name="cancellationToken">A token to cancel the subscription.</param>
            /// <returns>A task representing the asynchronous subscribe operation.</returns>
            internal async ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken) =>
                _subscription = await parent._source.SubscribeAsync(this, cancellationToken).ConfigureAwait(false);

            /// <inheritdoc/>
            ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
                parent._predicate(value)
                    ? SendThenCompleteAsync(value, cancellationToken)
                    : observer.OnNextAsync(value, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                observer.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) => observer.OnCompletedAsync(result);

            /// <summary>Forwards the element that matched, then completes.</summary>
            /// <param name="value">The matching element.</param>
            /// <param name="cancellationToken">A token to cancel the forwarding.</param>
            /// <returns>The forwarding operation.</returns>
            private async ValueTask SendThenCompleteAsync(T value, CancellationToken cancellationToken)
            {
                await observer.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
                await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
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
        [DebuggerDisplay("AsyncPredicateStopCoordinator: {_witness}")]
        internal sealed class AsyncPredicateStopCoordinator(
            AsyncPredicateStopSignal<T> parent,
            IObserverAsync<T> observer) : IWitnessAsync<T>
        {
            /// <summary>The inner subscription handle.</summary>
            private IAsyncDisposable? _subscription;

            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness;

            /// <inheritdoc/>
            ref WitnessAsyncState IWitnessState.Witness => ref _witness;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
                WitnessAsync.OnNextAsync(this, value, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
                WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

            /// <inheritdoc/>
            public async ValueTask DisposeAsync()
            {
                if (_subscription is not null)
                {
                    await _subscription.DisposeAsync().ConfigureAwait(false);
                }

                await WitnessAsync.DisposeStateAsync(this).ConfigureAwait(false);
            }

            /// <summary>Subscribes to the source observable.</summary>
            /// <param name="cancellationToken">A token to cancel the subscription.</param>
            /// <returns>A task representing the asynchronous subscribe operation.</returns>
            internal async ValueTask SubscribeSourcesAsync(CancellationToken cancellationToken) =>
                _subscription = await parent._source.SubscribeAsync(this, cancellationToken).ConfigureAwait(false);

            /// <inheritdoc/>
            async ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                var matched = await parent._asyncPredicate(value, cancellationToken).ConfigureAwait(false);
                await observer.OnNextAsync(value, cancellationToken).ConfigureAwait(false);

                if (matched)
                {
                    await observer.OnCompletedAsync(Result.Success).ConfigureAwait(false);
                }
            }

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                observer.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) => observer.OnCompletedAsync(result);
        }
    }
}
