// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async;

/// <summary>Fused filter / projection observables that back the parity-helper extension methods in <see cref="SignalAsyncExtensions"/>.</summary>
[SuppressMessage(
    "Major Code Smell",
    "S3604:Member initializer values should not be redundant",
    Justification = "Primary-constructor parameters are captured into observer state.")]
public static partial class SignalAsyncExtensions
{
    /// <summary>
    /// Fuses the previous <c>Create&lt;(T, T)&gt;</c> + closure-based <c>Pairwise</c> implementation
    /// into a single <see cref="WitnessAsync{T}"/> layer; per-subscription state is held in fields
    /// instead of a captured closure, eliminating the per-emission async-lambda state-machine box.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    internal sealed class PairwiseSignal<T>(IObservableAsync<T> source) : SignalAsync<(T Previous, T Current)>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<(T Previous, T Current)> observer,
            CancellationToken cancellationToken)
        {
            var sink = new PairwiseWitness(observer, cancellationToken);

            if (observer is WitnessAsync<(T Previous, T Current)> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription witness that emits <c>(previous, current)</c> tuples once primed.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class PairwiseWitness(
            IObserverAsync<(T Previous, T Current)> downstream,
            CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
        {
            /// <summary>The previously-seen value; valid only when <see cref="_hasPrevious"/> is set.</summary>
            private T? _previous;

            /// <summary>Latches to <see langword="true"/> after the first upstream emission.</summary>
            private bool _hasPrevious;

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (!_hasPrevious)
                {
                    _previous = value;
                    _hasPrevious = true;
                    return default;
                }

                var pair = (_previous!, value);
                _previous = value;
                return downstream.OnNextAsync(pair, cancellationToken);
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
    /// Combined skip-then-cast observable that fuses the previous
    /// <c>SkipWhile(value is null).Select(value!)</c> composition into a single
    /// <see cref="WitnessAsync{T}"/> layer. Once a non-null value has been seen the gate latches
    /// off and the operator becomes a transparent null-stripping forwarder.
    /// </summary>
    /// <typeparam name="T">The non-nullable element type seen downstream.</typeparam>
    /// <param name="source">The nullable source observable.</param>
    internal sealed class SkipWhileNullSignal<T>(IObservableAsync<T?> source) : SignalAsync<T>
        where T : class
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            var sink = new SkipWhileNullWitness(observer, cancellationToken);

            if (observer is WitnessAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer that skips leading nulls then forwards every subsequent value.</summary>
        /// <param name="downstream">The downstream observer expecting non-nullable values.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain.</param>
        internal sealed class SkipWhileNullWitness(
            IObserverAsync<T> downstream,
            CancellationToken subscribeToken) : WitnessAsync<T?>(subscribeToken)
        {
            /// <summary>Latches to <see langword="true"/> once a non-null value has been forwarded.</summary>
            private bool _gateOpen;

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T? value, CancellationToken cancellationToken)
            {
                if (!_gateOpen)
                {
                    if (value is null)
                    {
                        return default;
                    }

                    _gateOpen = true;
                }

                return downstream.OnNextAsync(value!, cancellationToken);
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
    /// Combined filter-and-cast observable that fuses the previous <c>Where(value is not null).Select(value!)</c>
    /// composition into a single <see cref="WitnessAsync{T}"/> layer. Halves the per-emission observer-chain
    /// cost (one TryEnter / Exit, one set of chain-aware-cancellation wiring) for what is fundamentally a
    /// null-stripping projection over a single source.
    /// </summary>
    /// <typeparam name="T">The non-nullable element type seen downstream.</typeparam>
    /// <param name="source">The nullable source observable.</param>
    internal sealed class WhereIsNotNullSignal<T>(IObservableAsync<T?> source) : SignalAsync<T>
        where T : class
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            var sink = new WhereIsNotNullWitness(observer, cancellationToken);

            // Wire sink's dispose token into the downstream's link chain so its hot path recognises
            // this token without allocating a per-emission linked CTS.
            if (observer is WitnessAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer that strips nulls and forwards the rest as non-nullable.</summary>
        /// <param name="downstream">The downstream observer expecting non-nullable values.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain.</param>
        internal sealed class WhereIsNotNullWitness(
            IObserverAsync<T> downstream,
            CancellationToken subscribeToken) : WitnessAsync<T?>(subscribeToken)
        {
            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T? value, CancellationToken cancellationToken)
            {
                if (value is null)
                {
                    return default;
                }

                return downstream.OnNextAsync(value, cancellationToken);
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
    /// Combined seed-and-distinct observable that fuses the previous
    /// <c>StartWith(seed).DistinctUntilChanged()</c> composition into a single
    /// <see cref="WitnessAsync{T}"/> layer. The seed is emitted on subscribe and tracked as the
    /// initial "last value"; source emissions that compare equal under
    /// <see cref="EqualityComparer{T}"/> are swallowed.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="defaultValue">The seed value emitted on subscribe.</param>
    internal sealed class LatestOrDefaultSignal<T>(IObservableAsync<T> source, T defaultValue) : SignalAsync<T>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            var sink = new LatestOrDefaultWitness(observer, defaultValue, cancellationToken);

            if (observer is WitnessAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            await observer.OnNextAsync(defaultValue, cancellationToken).ConfigureAwait(false);

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer that swallows values equal to the most-recently-forwarded one.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="seed">The seed value already emitted from <see cref="SubscribeAsyncCore"/>; treated as the initial "last forwarded value".</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain.</param>
        internal sealed class LatestOrDefaultWitness(
            IObserverAsync<T> downstream,
            T seed,
            CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
        {
            /// <summary>Equality comparer used for the distinct check; matches DistinctUntilChanged's default.</summary>
            private static readonly EqualityComparer<T> Comparer = EqualityComparer<T>.Default;

            /// <summary>The most-recently-forwarded value; seeded by the constructor.</summary>
            private T _last = seed;

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (Comparer.Equals(value, _last))
                {
                    return default;
                }

                _last = value;
                return downstream.OnNextAsync(value, cancellationToken);
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
    /// Combined filter-and-take-one observable that fuses the previous
    /// <c>Where(predicate).Take(1)</c> composition into a single <see cref="WitnessAsync{T}"/>
    /// layer. The first emission matching the predicate is forwarded, completion is signalled
    /// downstream, and the source subscription is disposed via the base observer's
    /// dispose-cascade.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="predicate">The predicate matched against each value.</param>
    internal sealed class WaitUntilSignal<T>(IObservableAsync<T> source, Func<T, bool> predicate) : SignalAsync<T>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            var sink = new WaitUntilWitness(observer, predicate, cancellationToken);

            if (observer is WitnessAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription witness that matches the predicate, forwards the first hit, and completes.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="predicate">The predicate matched against each value.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain.</param>
        internal sealed class WaitUntilWitness(
            IObserverAsync<T> downstream,
            Func<T, bool> predicate,
            CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
        {
            /// <summary>Latches to <see langword="true"/> after the first matching emission has been forwarded.</summary>
            private bool _matched;

            /// <inheritdoc/>
            protected override async ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (_matched || !predicate(value))
                {
                    return;
                }

                _matched = true;
                await downstream.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
                await downstream.OnCompletedAsync(Result.Success).ConfigureAwait(false);
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
    /// Fuses <c>source.Select(static _ =&gt; RxVoid.Default)</c> into a single observer layer; every
    /// upstream emission is forwarded as <see cref="RxVoid.Default"/> with no closure capture.
    /// </summary>
    /// <typeparam name="T">The upstream element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    internal sealed class AsRxVoidSignal<T>(IObservableAsync<T> source) : SignalAsync<RxVoid>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<RxVoid> observer,
            CancellationToken cancellationToken)
        {
            var sink = new AsSignalWitness(observer, cancellationToken);

            if (observer is WitnessAsync<RxVoid> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Forwards <see cref="RxVoid.Default"/> for every upstream emission.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class AsSignalWitness(
            IObserverAsync<RxVoid> downstream,
            CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
        {
            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
                downstream.OnNextAsync(RxVoid.Default, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }

    /// <summary>Fuses <c>source.Select(static value =&gt; !value)</c> into a single observer layer.</summary>
    /// <param name="source">The boolean source observable.</param>
    internal sealed class NotSignal(IObservableAsync<bool> source) : SignalAsync<bool>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<bool> observer,
            CancellationToken cancellationToken)
        {
            var sink = new NotWitness(observer, cancellationToken);

            if (observer is WitnessAsync<bool> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Negates every upstream emission.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class NotWitness(
            IObserverAsync<bool> downstream,
            CancellationToken subscribeToken) : WitnessAsync<bool>(subscribeToken)
        {
            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(bool value, CancellationToken cancellationToken) =>
                downstream.OnNextAsync(!value, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }

    /// <summary>Fuses <c>source.Where(static value =&gt; value)</c> into a single observer layer.</summary>
    /// <param name="source">The boolean source observable.</param>
    internal sealed class WhereTrueSignal(IObservableAsync<bool> source) : SignalAsync<bool>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<bool> observer,
            CancellationToken cancellationToken)
        {
            var sink = new WhereTrueWitness(observer, cancellationToken);

            if (observer is WitnessAsync<bool> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Forwards only <see langword="true"/> values.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class WhereTrueWitness(
            IObserverAsync<bool> downstream,
            CancellationToken subscribeToken) : WitnessAsync<bool>(subscribeToken)
        {
            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(bool value, CancellationToken cancellationToken) =>
                value ? downstream.OnNextAsync(true, cancellationToken) : default;

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }

    /// <summary>Fuses <c>source.Where(static value =&gt; !value)</c> into a single observer layer.</summary>
    /// <param name="source">The boolean source observable.</param>
    internal sealed class WhereFalseSignal(IObservableAsync<bool> source) : SignalAsync<bool>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<bool> observer,
            CancellationToken cancellationToken)
        {
            var sink = new WhereFalseWitness(observer, cancellationToken);

            if (observer is WitnessAsync<bool> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Forwards only <see langword="false"/> values.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class WhereFalseWitness(
            IObserverAsync<bool> downstream,
            CancellationToken subscribeToken) : WitnessAsync<bool>(subscribeToken)
        {
            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(bool value, CancellationToken cancellationToken) =>
                value ? default : downstream.OnNextAsync(false, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }
}
