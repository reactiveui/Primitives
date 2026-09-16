// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

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

            if (observer is IWitnessAsync<(T Previous, T Current)> downstreamBase)
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
        [DebuggerDisplay("PairwiseWitness: {_witness}")]
        internal sealed class PairwiseWitness(
            IObserverAsync<(T Previous, T Current)> downstream,
            CancellationToken subscribeToken) : IWitnessAsync<T>
        {
            /// <summary>The last value seen; valid only when <see cref="_hasPrevious"/> is set.</summary>
            private T? _previous;

            /// <summary>Latches to <see langword="true"/> after the first upstream emission.</summary>
            private bool _hasPrevious;

            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness = new(subscribeToken);

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
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

            /// <inheritdoc/>
            ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
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
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
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

            if (observer is IWitnessAsync<T> downstreamBase)
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
        [DebuggerDisplay("SkipWhileNullWitness: {_witness}")]
        internal sealed class SkipWhileNullWitness(
            IObserverAsync<T> downstream,
            CancellationToken subscribeToken) : IWitnessAsync<T?>
        {
            /// <summary>Latches to <see langword="true"/> once a non-null value has been forwarded.</summary>
            private bool _gateOpen;

            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness = new(subscribeToken);

            /// <inheritdoc/>
            ref WitnessAsyncState IWitnessState.Witness => ref _witness;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnNextAsync(T? value, CancellationToken cancellationToken) =>
                WitnessAsync.OnNextAsync(this, value, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
                WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

            /// <inheritdoc/>
            ValueTask IWitnessAsync<T?>.OnNextAsyncCore(T? value, CancellationToken cancellationToken)
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
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T?>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T?>.OnCompletedAsyncCore(Result result) =>
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

            if (observer is IWitnessAsync<T> downstreamBase)
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
        [DebuggerDisplay("WhereIsNotNullWitness: {_witness}")]
        internal sealed class WhereIsNotNullWitness(
            IObserverAsync<T> downstream,
            CancellationToken subscribeToken) : IWitnessAsync<T?>
        {
            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness = new(subscribeToken);

            /// <inheritdoc/>
            ref WitnessAsyncState IWitnessState.Witness => ref _witness;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnNextAsync(T? value, CancellationToken cancellationToken) =>
                WitnessAsync.OnNextAsync(this, value, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
                WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

            /// <inheritdoc/>
            ValueTask IWitnessAsync<T?>.OnNextAsyncCore(T? value, CancellationToken cancellationToken) =>
                value is null
                    ? default
                    : downstream.OnNextAsync(value, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T?>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T?>.OnCompletedAsyncCore(Result result) =>
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

            if (observer is IWitnessAsync<T> downstreamBase)
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
        [DebuggerDisplay("LatestOrDefaultWitness: {_witness}")]
        internal sealed class LatestOrDefaultWitness(
            IObserverAsync<T> downstream,
            T seed,
            CancellationToken subscribeToken) : IWitnessAsync<T>
        {
            /// <summary>The equality comparer for the distinct check.</summary>
            private static readonly EqualityComparer<T> Comparer = EqualityComparer<T>.Default;

            /// <summary>The most-recently-forwarded value; seeded by the constructor.</summary>
            private T _last = seed;

            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness = new(subscribeToken);

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
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

            /// <inheritdoc/>
            ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (Comparer.Equals(value, _last))
                {
                    return default;
                }

                _last = value;
                return downstream.OnNextAsync(value, cancellationToken);
            }

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
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

            if (observer is IWitnessAsync<T> downstreamBase)
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
        [DebuggerDisplay("WaitUntilWitness: {_witness}")]
        internal sealed class WaitUntilWitness(
            IObserverAsync<T> downstream,
            Func<T, bool> predicate,
            CancellationToken subscribeToken) : IWitnessAsync<T>
        {
            /// <summary>Latches to <see langword="true"/> after the first matching emission has been forwarded.</summary>
            private bool _matched;

            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness = new(subscribeToken);

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
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

            /// <inheritdoc/>
            async ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
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
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
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

            if (observer is IWitnessAsync<bool> downstreamBase)
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
        [DebuggerDisplay("NotWitness: {_witness}")]
        internal sealed class NotWitness(
            IObserverAsync<bool> downstream,
            CancellationToken subscribeToken) : IWitnessAsync<bool>
        {
            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness = new(subscribeToken);

            /// <inheritdoc/>
            ref WitnessAsyncState IWitnessState.Witness => ref _witness;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnNextAsync(bool value, CancellationToken cancellationToken) =>
                WitnessAsync.OnNextAsync(this, value, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
                WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<bool>.OnNextAsyncCore(bool value, CancellationToken cancellationToken) =>
                downstream.OnNextAsync(!value, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<bool>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<bool>.OnCompletedAsyncCore(Result result) =>
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

            if (observer is IWitnessAsync<bool> downstreamBase)
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
        [DebuggerDisplay("WhereTrueWitness: {_witness}")]
        internal sealed class WhereTrueWitness(
            IObserverAsync<bool> downstream,
            CancellationToken subscribeToken) : IWitnessAsync<bool>
        {
            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness = new(subscribeToken);

            /// <inheritdoc/>
            ref WitnessAsyncState IWitnessState.Witness => ref _witness;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnNextAsync(bool value, CancellationToken cancellationToken) =>
                WitnessAsync.OnNextAsync(this, value, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
                WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

            /// <inheritdoc/>
            ValueTask IWitnessAsync<bool>.OnNextAsyncCore(bool value, CancellationToken cancellationToken) =>
                value ? downstream.OnNextAsync(true, cancellationToken) : default;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<bool>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<bool>.OnCompletedAsyncCore(Result result) =>
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

            if (observer is IWitnessAsync<bool> downstreamBase)
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
        [DebuggerDisplay("WhereFalseWitness: {_witness}")]
        internal sealed class WhereFalseWitness(
            IObserverAsync<bool> downstream,
            CancellationToken subscribeToken) : IWitnessAsync<bool>
        {
            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness = new(subscribeToken);

            /// <inheritdoc/>
            ref WitnessAsyncState IWitnessState.Witness => ref _witness;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnNextAsync(bool value, CancellationToken cancellationToken) =>
                WitnessAsync.OnNextAsync(this, value, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) =>
                WitnessAsync.OnErrorResumeAsync(this, error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnCompletedAsync(Result result) => WitnessAsync.OnCompletedAsync(this, result);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask DisposeAsync() => WitnessAsync.DisposeStateAsync(this);

            /// <inheritdoc/>
            ValueTask IWitnessAsync<bool>.OnNextAsyncCore(bool value, CancellationToken cancellationToken) =>
                value ? default : downstream.OnNextAsync(false, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<bool>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<bool>.OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }
}
