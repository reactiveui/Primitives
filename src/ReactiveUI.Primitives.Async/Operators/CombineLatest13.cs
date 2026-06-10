// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides the arity-13 (<c>thirteen</c>-source) <c>CombineLatest</c> extension method
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
        /// Combines the latest values from 13 asynchronous observable sources into a single
        /// sequence, projecting them through <paramref name="selector"/> whenever any source emits.
        /// </summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="T4">The element type of source 4.</typeparam>
        /// <typeparam name="T5">The element type of source 5.</typeparam>
        /// <typeparam name="T6">The element type of source 6.</typeparam>
        /// <typeparam name="T7">The element type of source 7.</typeparam>
        /// <typeparam name="T8">The element type of source 8.</typeparam>
        /// <typeparam name="T9">The element type of source 9.</typeparam>
        /// <typeparam name="T10">The element type of source 10.</typeparam>
        /// <typeparam name="T11">The element type of source 11.</typeparam>
        /// <typeparam name="T12">The element type of source 12.</typeparam>
        /// <typeparam name="T13">The element type of source 13.</typeparam>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="src2">Source observable 2 whose latest value is combined.</param>
        /// <param name="src3">Source observable 3 whose latest value is combined.</param>
        /// <param name="src4">Source observable 4 whose latest value is combined.</param>
        /// <param name="src5">Source observable 5 whose latest value is combined.</param>
        /// <param name="src6">Source observable 6 whose latest value is combined.</param>
        /// <param name="src7">Source observable 7 whose latest value is combined.</param>
        /// <param name="src8">Source observable 8 whose latest value is combined.</param>
        /// <param name="src9">Source observable 9 whose latest value is combined.</param>
        /// <param name="src10">Source observable 10 whose latest value is combined.</param>
        /// <param name="src11">Source observable 11 whose latest value is combined.</param>
        /// <param name="src12">Source observable 12 whose latest value is combined.</param>
        /// <param name="src13">Source observable 13 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - just expected for arity-N CombineLatest operator surface.")]
        public IObservableAsync<TResult> SyncLatest<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult>(
            IObservableAsync<T2> src2,
            IObservableAsync<T3> src3,
            IObservableAsync<T4> src4,
            IObservableAsync<T5> src5,
            IObservableAsync<T6> src6,
            IObservableAsync<T7> src7,
            IObservableAsync<T8> src8,
            IObservableAsync<T9> src9,
            IObservableAsync<T10> src10,
            IObservableAsync<T11> src11,
            IObservableAsync<T12> src12,
            IObservableAsync<T13> src13,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult> selector) =>
            new CombineLatest13SignalAsync<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult>(
                new(src1, src2, src3, src4, src5, src6, src7, src8, src9, src10, src11, src12, src13),
                selector);

        /// <summary>
        /// Combines the latest values from 13 asynchronous observable sources into a single
        /// sequence, projecting them through <paramref name="selector"/> whenever any source emits.
        /// </summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="T3">The element type of source 3.</typeparam>
        /// <typeparam name="T4">The element type of source 4.</typeparam>
        /// <typeparam name="T5">The element type of source 5.</typeparam>
        /// <typeparam name="T6">The element type of source 6.</typeparam>
        /// <typeparam name="T7">The element type of source 7.</typeparam>
        /// <typeparam name="T8">The element type of source 8.</typeparam>
        /// <typeparam name="T9">The element type of source 9.</typeparam>
        /// <typeparam name="T10">The element type of source 10.</typeparam>
        /// <typeparam name="T11">The element type of source 11.</typeparam>
        /// <typeparam name="T12">The element type of source 12.</typeparam>
        /// <typeparam name="T13">The element type of source 13.</typeparam>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="src2">Source observable 2 whose latest value is combined.</param>
        /// <param name="src3">Source observable 3 whose latest value is combined.</param>
        /// <param name="src4">Source observable 4 whose latest value is combined.</param>
        /// <param name="src5">Source observable 5 whose latest value is combined.</param>
        /// <param name="src6">Source observable 6 whose latest value is combined.</param>
        /// <param name="src7">Source observable 7 whose latest value is combined.</param>
        /// <param name="src8">Source observable 8 whose latest value is combined.</param>
        /// <param name="src9">Source observable 9 whose latest value is combined.</param>
        /// <param name="src10">Source observable 10 whose latest value is combined.</param>
        /// <param name="src11">Source observable 11 whose latest value is combined.</param>
        /// <param name="src12">Source observable 12 whose latest value is combined.</param>
        /// <param name="src13">Source observable 13 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - just expected for arity-N CombineLatest operator surface.")]
        public IObservableAsync<TResult> CombineLatest<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult>(
            IObservableAsync<T2> src2,
            IObservableAsync<T3> src3,
            IObservableAsync<T4> src4,
            IObservableAsync<T5> src5,
            IObservableAsync<T6> src6,
            IObservableAsync<T7> src7,
            IObservableAsync<T8> src8,
            IObservableAsync<T9> src9,
            IObservableAsync<T10> src10,
            IObservableAsync<T11> src11,
            IObservableAsync<T12> src12,
            IObservableAsync<T13> src13,
            Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult> selector) =>
            src1.SyncLatest(src2, src3, src4, src5, src6, src7, src8, src9, src10, src11, src12, src13, selector);
    }

    /// <summary>Async observable that combines the latest values from thirteen source sequences using a selector.</summary>
    /// <typeparam name="T1">Element type of source 1.</typeparam>
    /// <typeparam name="T2">Element type of source 2.</typeparam>
    /// <typeparam name="T3">Element type of source 3.</typeparam>
    /// <typeparam name="T4">Element type of source 4.</typeparam>
    /// <typeparam name="T5">Element type of source 5.</typeparam>
    /// <typeparam name="T6">Element type of source 6.</typeparam>
    /// <typeparam name="T7">Element type of source 7.</typeparam>
    /// <typeparam name="T8">Element type of source 8.</typeparam>
    /// <typeparam name="T9">Element type of source 9.</typeparam>
    /// <typeparam name="T10">Element type of source 10.</typeparam>
    /// <typeparam name="T11">Element type of source 11.</typeparam>
    /// <typeparam name="T12">Element type of source 12.</typeparam>
    /// <typeparam name="T13">Element type of source 13.</typeparam>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    /// <param name="sources">The bundled source observables.</param>
    /// <param name="selector">The selector that projects the latest values.</param>
    internal sealed class CombineLatest13SignalAsync<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult>(
        CombineLatest13SignalAsync<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult>.Sources sources,
        Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult> selector) : SignalAsync<TResult>
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
        /// Bundles the thirteen source observables so the subscription constructor stays at three
        /// parameters (observer, sources, selector) regardless of arity. Sonar S107 caps method /
        /// constructor parameter count; the bundle keeps the internal types compliant.
        /// </summary>
        /// <param name="Src1">Source observable 1.</param>
        /// <param name="Src2">Source observable 2.</param>
        /// <param name="Src3">Source observable 3.</param>
        /// <param name="Src4">Source observable 4.</param>
        /// <param name="Src5">Source observable 5.</param>
        /// <param name="Src6">Source observable 6.</param>
        /// <param name="Src7">Source observable 7.</param>
        /// <param name="Src8">Source observable 8.</param>
        /// <param name="Src9">Source observable 9.</param>
        /// <param name="Src10">Source observable 10.</param>
        /// <param name="Src11">Source observable 11.</param>
        /// <param name="Src12">Source observable 12.</param>
        /// <param name="Src13">Source observable 13.</param>
        internal readonly record struct Sources(
            IObservableAsync<T1> Src1,
            IObservableAsync<T2> Src2,
            IObservableAsync<T3> Src3,
            IObservableAsync<T4> Src4,
            IObservableAsync<T5> Src5,
            IObservableAsync<T6> Src6,
            IObservableAsync<T7> Src7,
            IObservableAsync<T8> Src8,
            IObservableAsync<T9> Src9,
            IObservableAsync<T10> Src10,
            IObservableAsync<T11> Src11,
            IObservableAsync<T12> Src12,
            IObservableAsync<T13> Src13);

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

            /// <summary>Bit owned by source 7 inside the lifecycle's completion bitmask.</summary>
            private const int Source7Bit = 1 << 6;

            /// <summary>Bit owned by source 8 inside the lifecycle's completion bitmask.</summary>
            private const int Source8Bit = 1 << 7;

            /// <summary>Bit owned by source 9 inside the lifecycle's completion bitmask.</summary>
            private const int Source9Bit = 1 << 8;

            /// <summary>Bit owned by source 10 inside the lifecycle's completion bitmask.</summary>
            private const int Source10Bit = 1 << 9;

            /// <summary>Bit owned by source 11 inside the lifecycle's completion bitmask.</summary>
            private const int Source11Bit = 1 << 10;

            /// <summary>Bit owned by source 12 inside the lifecycle's completion bitmask.</summary>
            private const int Source12Bit = 1 << 11;

            /// <summary>Bit owned by source 13 inside the lifecycle's completion bitmask.</summary>
            private const int Source13Bit = 1 << 12;

            /// <summary>Bundled source observables.</summary>
            private readonly Sources _sources;

            /// <summary>The result selector function.</summary>
            private readonly Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult> _selector;

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

            /// <summary>Indexed observer for source 7.</summary>
            private readonly CombineLatestIndexedWitness<T7, TResult> _obs7;

            /// <summary>Indexed observer for source 8.</summary>
            private readonly CombineLatestIndexedWitness<T8, TResult> _obs8;

            /// <summary>Indexed observer for source 9.</summary>
            private readonly CombineLatestIndexedWitness<T9, TResult> _obs9;

            /// <summary>Indexed observer for source 10.</summary>
            private readonly CombineLatestIndexedWitness<T10, TResult> _obs10;

            /// <summary>Indexed observer for source 11.</summary>
            private readonly CombineLatestIndexedWitness<T11, TResult> _obs11;

            /// <summary>Indexed observer for source 12.</summary>
            private readonly CombineLatestIndexedWitness<T12, TResult> _obs12;

            /// <summary>Indexed observer for source 13.</summary>
            private readonly CombineLatestIndexedWitness<T13, TResult> _obs13;

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

            /// <summary>Latest value from source 7.</summary>
            private Optional<T7> _val7 = Optional<T7>.Empty;

            /// <summary>Latest value from source 8.</summary>
            private Optional<T8> _val8 = Optional<T8>.Empty;

            /// <summary>Latest value from source 9.</summary>
            private Optional<T9> _val9 = Optional<T9>.Empty;

            /// <summary>Latest value from source 10.</summary>
            private Optional<T10> _val10 = Optional<T10>.Empty;

            /// <summary>Latest value from source 11.</summary>
            private Optional<T11> _val11 = Optional<T11>.Empty;

            /// <summary>Latest value from source 12.</summary>
            private Optional<T12> _val12 = Optional<T12>.Empty;

            /// <summary>Latest value from source 13.</summary>
            private Optional<T13> _val13 = Optional<T13>.Empty;

            /// <summary>Initializes a new instance of the <see cref="CombineLatestCoordinator"/> class.</summary>
            /// <param name="observer">The downstream observer.</param>
            /// <param name="sources">The bundled source observables.</param>
            /// <param name="selector">The selector that projects the latest values.</param>
            public CombineLatestCoordinator(
                IObserverAsync<TResult> observer,
                Sources sources,
                Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, TResult> selector)
                : base(observer, sourceCount: 13)
            {
                _sources = sources;
                _selector = selector;
                _obs1 = new(this, Source1Bit, v => _val1 = new(v));
                _obs2 = new(this, Source2Bit, v => _val2 = new(v));
                _obs3 = new(this, Source3Bit, v => _val3 = new(v));
                _obs4 = new(this, Source4Bit, v => _val4 = new(v));
                _obs5 = new(this, Source5Bit, v => _val5 = new(v));
                _obs6 = new(this, Source6Bit, v => _val6 = new(v));
                _obs7 = new(this, Source7Bit, v => _val7 = new(v));
                _obs8 = new(this, Source8Bit, v => _val8 = new(v));
                _obs9 = new(this, Source9Bit, v => _val9 = new(v));
                _obs10 = new(this, Source10Bit, v => _val10 = new(v));
                _obs11 = new(this, Source11Bit, v => _val11 = new(v));
                _obs12 = new(this, Source12Bit, v => _val12 = new(v));
                _obs13 = new(this, Source13Bit, v => _val13 = new(v));
            }

            /// <inheritdoc/>
            internal override ValueTask EmitLatestAsync()
            {
                if (!TryReadValues(out var values))
                {
                    return default;
                }

                var projected = _selector(
                            values.V1,
                            values.V2,
                            values.V3,
                            values.V4,
                            values.V5,
                            values.V6,
                            values.V7,
                            values.V8,
                            values.V9,
                            values.V10,
                            values.V11,
                            values.V12,
                            values.V13);
                return Lifecycle.EmitDownstreamAsync(projected);
            }

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
                    5 => _sources.Src6.SubscribeAsync(_obs6, cancellationToken),
                    6 => _sources.Src7.SubscribeAsync(_obs7, cancellationToken),
                    7 => _sources.Src8.SubscribeAsync(_obs8, cancellationToken),
                    8 => _sources.Src9.SubscribeAsync(_obs9, cancellationToken),
                    9 => _sources.Src10.SubscribeAsync(_obs10, cancellationToken),
                    10 => _sources.Src11.SubscribeAsync(_obs11, cancellationToken),
                    11 => _sources.Src12.SubscribeAsync(_obs12, cancellationToken),
                    _ => _sources.Src13.SubscribeAsync(_obs13, cancellationToken),
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
                    && _val6.TryGetValue(out var v6)
                    && _val7.TryGetValue(out var v7)
                    && _val8.TryGetValue(out var v8)
                    && _val9.TryGetValue(out var v9)
                    && _val10.TryGetValue(out var v10)
                    && _val11.TryGetValue(out var v11)
                    && _val12.TryGetValue(out var v12)
                    && _val13.TryGetValue(out var v13))
                {
                    values = new(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13);
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
            /// <param name="V7">Latest value from source 7.</param>
            /// <param name="V8">Latest value from source 8.</param>
            /// <param name="V9">Latest value from source 9.</param>
            /// <param name="V10">Latest value from source 10.</param>
            /// <param name="V11">Latest value from source 11.</param>
            /// <param name="V12">Latest value from source 12.</param>
            /// <param name="V13">Latest value from source 13.</param>
            internal readonly record struct Values(
                T1 V1,
                T2 V2,
                T3 V3,
                T4 V4,
                T5 V5,
                T6 V6,
                T7 V7,
                T8 V8,
                T9 V9,
                T10 V10,
                T11 V11,
                T12 V12,
                T13 V13);
        }
    }
}
