// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides extension methods for creating and manipulating asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Filtering (Keep/Where) operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Creates a new observable sequence that contains only the elements from the source sequence that satisfy the specified asynchronous predicate.</summary>
        /// <param name="predicate">A function that evaluates each element and its associated cancellation token, returning a ValueTask that
        /// resolves to <see langword="true"/> to include the element in the resulting sequence; otherwise, <see
        /// langword="false"/>.</param>
        /// <returns>An observable sequence that emits only those elements for which the predicate returns <see
        /// langword="true"/>.</returns>
        /// <remarks>An exception thrown by <paramref name="predicate"/>, or a faulted task from it, propagates to the
        /// observers of the resulting sequence.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Keep(Func<T, CancellationToken, ValueTask<bool>> predicate) =>
            new KeepAsyncSignal<T>(source, predicate);

        /// <summary>Creates a new observable sequence that contains only the elements from the current sequence that satisfy the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition. The element is included in the resulting sequence if the
        /// function returns <see langword="true"/>.</param>
        /// <returns>An observable sequence that contains elements from the current sequence that satisfy the specified
        /// predicate.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Keep(Func<T, bool> predicate) =>
            new KeepSyncSignal<T>(source, predicate);

        /// <summary>Keeps values that satisfy a stateful predicate.</summary>
        /// <typeparam name="TState">The caller-supplied state type.</typeparam>
        /// <param name="state">The caller-supplied state passed to the predicate.</param>
        /// <param name="predicate">The predicate that values and the state must satisfy.</param>
        /// <returns>An observable sequence of values that satisfy the predicate.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
        public IObservableAsync<T> KeepWith<TState>(
            TState state,
            Func<TState, T, bool> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(predicate);

            return new KeepSyncSignal<T>(source, value => predicate(state, value));
        }

        /// <summary>Creates a new observable sequence that contains only the elements from the source sequence that satisfy the specified asynchronous predicate.</summary>
        /// <param name="predicate">A function that evaluates each element and cancellation token.</param>
        /// <returns>An observable sequence that emits only those elements for which the predicate returns <see langword="true"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Where(Func<T, CancellationToken, ValueTask<bool>> predicate) =>
            new KeepAsyncSignal<T>(source, predicate);

        /// <summary>Creates a new observable sequence that contains only the elements from the current sequence that satisfy the specified predicate.</summary>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <returns>An observable sequence that contains elements from the current sequence that satisfy the predicate.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Where(Func<T, bool> predicate) =>
            new KeepSyncSignal<T>(source, predicate);
    }

    /// <summary>
    /// Async-predicate variant of <see cref="Keep{T}(IObservableAsync{T}, Func{T,CancellationToken,ValueTask{bool}})"/>,
    /// allocating one observer per subscription and nothing per emission.
    /// </summary>
    /// <typeparam name="T">The element type of the source sequence.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="predicate">The asynchronous predicate.</param>
    internal sealed class KeepAsyncSignal<T>(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask<bool>> predicate) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            KeepAsyncWitness sink = new(observer, predicate, cancellationToken);

            if (observer is WitnessAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer that applies the async predicate and forwards passing elements.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="predicate">The async predicate.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain.</param>
        internal sealed class KeepAsyncWitness(
            IObserverAsync<T> downstream,
            Func<T, CancellationToken, ValueTask<bool>> predicate,
            CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
        {
            /// <inheritdoc/>
            protected override async ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (await predicate(value, cancellationToken).ConfigureAwait(false))
                {
                    await downstream.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
                }
            }

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }

    /// <summary>
    /// Synchronous-predicate variant of <see cref="Keep{T}(IObservableAsync{T}, Func{T,bool})"/>, whose per-emission
    /// path completes synchronously when the predicate rejects a value.
    /// </summary>
    /// <typeparam name="T">The element type of the source sequence.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="predicate">The synchronous predicate.</param>
    internal sealed class KeepSyncSignal<T>(
        IObservableAsync<T> source,
        Func<T, bool> predicate) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            KeepSyncWitness sink = new(observer, predicate, cancellationToken);

            if (observer is WitnessAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer that applies the sync predicate and forwards passing elements.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="predicate">The sync predicate.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain.</param>
        internal sealed class KeepSyncWitness(
            IObserverAsync<T> downstream,
            Func<T, bool> predicate,
            CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
        {
            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
                !predicate(value) ? default : downstream.OnNextAsync(value, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }
}
