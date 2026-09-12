// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Fused filter / projection observables that back the parity-helper extension methods in <see cref="SignalAsyncExtensions"/>.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Emits each adjacent <c>(previous, current)</c> pair of source values.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    internal sealed class PairwiseSignal<T>(IObservableAsync<T> source) : IObservableAsync<(T Previous, T Current)>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<(T Previous, T Current)>.SubscribeAsync(
            IObserverAsync<(T Previous, T Current)> observer,
            CancellationToken cancellationToken)
        {
            PairwiseWitness sink = new(observer, cancellationToken);

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
            /// <summary>The last value seen; valid only when <see cref="_hasPrevious"/> is set.</summary>
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

                var pair = (Previous: _previous!, Current: value);
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

    /// <summary>Skips values until the first non-null one arrives, then forwards every later value as non-nullable.</summary>
    /// <typeparam name="T">The non-nullable element type seen downstream.</typeparam>
    /// <param name="source">The nullable source observable.</param>
    internal sealed class SkipWhileNullSignal<T>(IObservableAsync<T?> source) : IObservableAsync<T>
        where T : class
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            SkipWhileNullWitness sink = new(observer, cancellationToken);

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

    /// <summary>Forwards the non-null source values as non-nullable through a single observer layer.</summary>
    /// <typeparam name="T">The non-nullable element type seen downstream.</typeparam>
    /// <param name="source">The nullable source observable.</param>
    internal sealed class WhereIsNotNullSignal<T>(IObservableAsync<T?> source) : IObservableAsync<T>
        where T : class
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            WhereIsNotNullWitness sink = new(observer, cancellationToken);

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
            protected override ValueTask OnNextAsyncCore(T? value, CancellationToken cancellationToken) =>
                value is null
                    ? default
                    : downstream.OnNextAsync(value, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }

    /// <summary>Emits the seed on subscribe, then forwards only the source values that differ from the last emission.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="defaultValue">The seed value emitted on subscribe.</param>
    internal sealed class LatestOrDefaultSignal<T>(IObservableAsync<T> source, T defaultValue) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            LatestOrDefaultWitness sink = new(observer, defaultValue, cancellationToken);

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
        /// <param name="seed">The seed value emitted during subscription, which becomes the initial last-forwarded value.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain.</param>
        internal sealed class LatestOrDefaultWitness(
            IObserverAsync<T> downstream,
            T seed,
            CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
        {
            /// <summary>The equality comparer for the distinct check.</summary>
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

    /// <summary>Forwards the first value matching the predicate, then completes and tears down the source subscription.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The source observable.</param>
    /// <param name="predicate">The predicate matched against each value.</param>
    internal sealed class WaitUntilSignal<T>(IObservableAsync<T> source, Func<T, bool> predicate) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            WaitUntilWitness sink = new(observer, predicate, cancellationToken);

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

    /// <summary>Fuses <c>source.Select(static value =&gt; !value)</c> into a single observer layer.</summary>
    /// <param name="source">The boolean source observable.</param>
    internal sealed class NotSignal(IObservableAsync<bool> source) : IObservableAsync<bool>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<bool>.SubscribeAsync(
            IObserverAsync<bool> observer,
            CancellationToken cancellationToken)
        {
            NotWitness sink = new(observer, cancellationToken);

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
    internal sealed class WhereTrueSignal(IObservableAsync<bool> source) : IObservableAsync<bool>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<bool>.SubscribeAsync(
            IObserverAsync<bool> observer,
            CancellationToken cancellationToken)
        {
            WhereTrueWitness sink = new(observer, cancellationToken);

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
    internal sealed class WhereFalseSignal(IObservableAsync<bool> source) : IObservableAsync<bool>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<bool>.SubscribeAsync(
            IObserverAsync<bool> observer,
            CancellationToken cancellationToken)
        {
            WhereFalseWitness sink = new(observer, cancellationToken);

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
