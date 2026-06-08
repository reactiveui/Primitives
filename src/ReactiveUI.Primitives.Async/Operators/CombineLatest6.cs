// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides the arity-6 (<c>six</c>-source) <c>CombineLatest</c> extension method
/// and its supporting internal observable + subscription types.
/// </summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Combines the latest values from multiple asynchronous observable sources.</summary>
    /// <param name="src1">Source observable 1 whose latest value is combined.</param>
    /// <typeparam name="T1">The element type of source 1.</typeparam>
    extension<T1>(IObservableAsync<T1> src1)
    {
        /// <summary>
        /// Combines the latest values from six asynchronous observable sources into a single
        /// sequence, projecting them through <paramref name="selector"/> whenever any source emits.
        /// </summary>
        /// <remarks>
        /// The returned sequence does not produce a value until every source has emitted at least
        /// once. After that, each new value from any source produces a fresh projection using the
        /// most recent value from each. Completion / failure of any source propagates downstream.
        /// </remarks>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="T4">The element type of source 4.</typeparam>
        /// <typeparam name="T5">The element type of source 5.</typeparam>
        /// <typeparam name="T6">The element type of source 6.</typeparam>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="src2">Source observable 2 whose latest value is combined.</param>
        /// <param name="src3">Source observable 3 whose latest value is combined.</param>
        /// <param name="src4">Source observable 4 whose latest value is combined.</param>
        /// <param name="src5">Source observable 5 whose latest value is combined.</param>
        /// <param name="src6">Source observable 6 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - just expected for arity-N CombineLatest operator surface.")]
        public IObservableAsync<TResult> CombineLatest<T2, T3, T4, T5, T6, TResult>(
            IObservableAsync<T2> src2,
            IObservableAsync<T3> src3,
            IObservableAsync<T4> src4,
            IObservableAsync<T5> src5,
            IObservableAsync<T6> src6,
            Func<T1, T2, T3, T4, T5, T6, TResult> selector) =>
            new CombineLatest6SignalAsync<T1, T2, T3, T4, T5, T6, TResult>(
                new(src1, src2, src3, src4, src5, src6),
                selector);
    }

    /// <summary>Async observable that combines the latest values from six source sequences using a selector.</summary>
    /// <typeparam name="T1">Element type of source 1.</typeparam>
    /// <typeparam name="T2">Element type of source 2.</typeparam>
    /// <typeparam name="T3">Element type of source 3.</typeparam>
    /// <typeparam name="T4">Element type of source 4.</typeparam>
    /// <typeparam name="T5">Element type of source 5.</typeparam>
    /// <typeparam name="T6">Element type of source 6.</typeparam>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    /// <param name="sources">The bundled source observables.</param>
    /// <param name="selector">The selector that projects the latest values.</param>
    internal sealed class CombineLatest6SignalAsync<T1, T2, T3, T4, T5, T6, TResult>(
        CombineLatest6SignalAsync<T1, T2, T3, T4, T5, T6, TResult>.Sources sources,
        Func<T1, T2, T3, T4, T5, T6, TResult> selector) : SignalAsync<TResult>
    {
        /// <inheritdoc/>
        protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<TResult> observer,
            CancellationToken cancellationToken)
        {
            var subscription = new CombineLatestCoordinator(observer, sources, selector);
            subscription.Lifecycle.LinkExternalCancellation(cancellationToken);
            return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
                subscription,
                () => subscription.SubscribeSourcesAsync(cancellationToken));
        }

        /// <summary>
        /// Bundles the six source observables so the subscription constructor stays at three
        /// parameters (observer, sources, selector) regardless of arity. Sonar S107 caps method /
        /// constructor parameter count; the bundle keeps the internal types compliant.
        /// </summary>
        /// <param name="Src1">Source observable 1.</param>
        /// <param name="Src2">Source observable 2.</param>
        /// <param name="Src3">Source observable 3.</param>
        /// <param name="Src4">Source observable 4.</param>
        /// <param name="Src5">Source observable 5.</param>
        /// <param name="Src6">Source observable 6.</param>
        internal readonly record struct Sources(
            IObservableAsync<T1> Src1,
            IObservableAsync<T2> Src2,
            IObservableAsync<T3> Src3,
            IObservableAsync<T4> Src4,
            IObservableAsync<T5> Src5,
            IObservableAsync<T6> Src6);

        /// <summary>
        /// Per-arity subscription holding the typed Optional slots, the pre-built indexed
        /// observers, the SubscribeAtAsync switch, and the selector invocation. Shared scaffolding
        /// (gate, lifecycle, ValuesLock, OnErrorResume, SubscribeSourcesAsync, DisposeAsync) lives
        /// in <see cref="CombineLatestCoordinatorBase{TResult}"/>; the per-source OnNext / OnError /
        /// OnCompleted forwarding lives in <see cref="CombineLatestIndexedWitness{TSource, TResult}"/>.
        /// </summary>
        internal sealed class CombineLatestCoordinator : CombineLatestCoordinatorBase<TResult>
        {
            /// <summary>Bit owned by source 1 inside the lifecycle's completion bitmask.</summary>
            private const int Source1Bit = 1 << 0;

            /// <summary>Bit owned by source 2 inside the lifecycle's completion bitmask.</summary>
            private const int Source2Bit = 1 << 1;

            /// <summary>Bit owned by source 3 inside the lifecycle's completion bitmask.</summary>
            private const int Source3Bit = 1 << 2;

            /// <summary>Bit owned by source 4 inside the lifecycle's completion bitmask.</summary>
            private const int Source4Bit = 1 << 3;

            /// <summary>Bit owned by source 5 inside the lifecycle's completion bitmask.</summary>
            private const int Source5Bit = 1 << 4;

            /// <summary>Bit owned by source 6 inside the lifecycle's completion bitmask.</summary>
            private const int Source6Bit = 1 << 5;

            /// <summary>Bundled source observables.</summary>
            private readonly Sources _sources;

            /// <summary>The result selector function.</summary>
            private readonly Func<T1, T2, T3, T4, T5, T6, TResult> _selector;

            /// <summary>Indexed observer for source 1.</summary>
            private readonly CombineLatestIndexedWitness<T1, TResult> _obs1;

            /// <summary>Indexed observer for source 2.</summary>
            private readonly CombineLatestIndexedWitness<T2, TResult> _obs2;

            /// <summary>Indexed observer for source 3.</summary>
            private readonly CombineLatestIndexedWitness<T3, TResult> _obs3;

            /// <summary>Indexed observer for source 4.</summary>
            private readonly CombineLatestIndexedWitness<T4, TResult> _obs4;

            /// <summary>Indexed observer for source 5.</summary>
            private readonly CombineLatestIndexedWitness<T5, TResult> _obs5;

            /// <summary>Indexed observer for source 6.</summary>
            private readonly CombineLatestIndexedWitness<T6, TResult> _obs6;

            /// <summary>Latest value from source 1.</summary>
            private Optional<T1> _val1 = Optional<T1>.Empty;

            /// <summary>Latest value from source 2.</summary>
            private Optional<T2> _val2 = Optional<T2>.Empty;

            /// <summary>Latest value from source 3.</summary>
            private Optional<T3> _val3 = Optional<T3>.Empty;

            /// <summary>Latest value from source 4.</summary>
            private Optional<T4> _val4 = Optional<T4>.Empty;

            /// <summary>Latest value from source 5.</summary>
            private Optional<T5> _val5 = Optional<T5>.Empty;

            /// <summary>Latest value from source 6.</summary>
            private Optional<T6> _val6 = Optional<T6>.Empty;

            /// <summary>Initializes a new instance of the <see cref="CombineLatestCoordinator"/> class.</summary>
            /// <param name="observer">The downstream observer.</param>
            /// <param name="sources">The bundled source observables.</param>
            /// <param name="selector">The selector that projects the latest values.</param>
            public CombineLatestCoordinator(
                IObserverAsync<TResult> observer,
                Sources sources,
                Func<T1, T2, T3, T4, T5, T6, TResult> selector)
                : base(observer, sourceCount: 6)
            {
                _sources = sources;
                _selector = selector;
                _obs1 = new(this, Source1Bit, v => _val1 = new(v));
                _obs2 = new(this, Source2Bit, v => _val2 = new(v));
                _obs3 = new(this, Source3Bit, v => _val3 = new(v));
                _obs4 = new(this, Source4Bit, v => _val4 = new(v));
                _obs5 = new(this, Source5Bit, v => _val5 = new(v));
                _obs6 = new(this, Source6Bit, v => _val6 = new(v));
            }

            /// <inheritdoc/>
            internal override ValueTask EmitLatestAsync() =>
                TryReadValues(out var values)
                    ? Lifecycle.EmitDownstreamAsync(_selector(values.V1, values.V2, values.V3, values.V4, values.V5, values.V6))
                    : default;

            /// <inheritdoc/>
            [SuppressMessage(
                "Minor Code Smell",
                "S109:Magic numbers should not be used",
                Justification = "Switch dispatches on the 0..N-1 source index; naming each numeric arm would just rename the obvious.")]
            [SuppressMessage(
                "Major Code Smell",
                "S1541:Methods and properties should not be too complex",
                Justification = "Switch arm per source — the high arms-count IS the dispatch surface; splitting hurts readability more than it helps.")]
            protected override ValueTask<IAsyncDisposable> SubscribeAtAsync(int index, CancellationToken cancellationToken) =>
                index switch
                {
                    0 => _sources.Src1.SubscribeAsync(_obs1, cancellationToken),
                    1 => _sources.Src2.SubscribeAsync(_obs2, cancellationToken),
                    2 => _sources.Src3.SubscribeAsync(_obs3, cancellationToken),
                    3 => _sources.Src4.SubscribeAsync(_obs4, cancellationToken),
                    4 => _sources.Src5.SubscribeAsync(_obs5, cancellationToken),
                    _ => _sources.Src6.SubscribeAsync(_obs6, cancellationToken),
                };

            /// <summary>
            /// Reads every source's latest value into a single snapshot. Returns <see langword="false"/>
            /// (with <paramref name="values"/> set to <see langword="default"/>) until every source has
            /// produced at least one value.
            /// </summary>
            /// <param name="values">When the method returns <see langword="true"/>, the snapshot.</param>
            /// <returns><see langword="true"/> when every source has produced a value; otherwise <see langword="false"/>.</returns>
            [SuppressMessage(
                "Major Code Smell",
                "S1541:Methods and properties should not be too complex",
                Justification = "Short-circuited && chain over every source's Optional; the high condition count IS the snapshot semantic.")]
            private bool TryReadValues(out Values values)
            {
                if (_val1.TryGetValue(out var v1)
                    && _val2.TryGetValue(out var v2)
                    && _val3.TryGetValue(out var v3)
                    && _val4.TryGetValue(out var v4)
                    && _val5.TryGetValue(out var v5)
                    && _val6.TryGetValue(out var v6))
                {
                    values = new(v1, v2, v3, v4, v5, v6);
                    return true;
                }

                values = default;
                return false;
            }

            /// <summary>Latest-value snapshot taken when every source has produced at least one value.</summary>
            /// <param name="V1">Latest value from source 1.</param>
            /// <param name="V2">Latest value from source 2.</param>
            /// <param name="V3">Latest value from source 3.</param>
            /// <param name="V4">Latest value from source 4.</param>
            /// <param name="V5">Latest value from source 5.</param>
            /// <param name="V6">Latest value from source 6.</param>
            internal readonly record struct Values(
                T1 V1,
                T2 V2,
                T3 V3,
                T4 V4,
                T5 V5,
                T6 V6);
        }
    }
}
