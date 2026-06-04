// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides the arity-16 (<c>sixteen</c>-source) <c>CombineLatest</c> extension method
/// and its supporting internal observable + subscription types.
/// </summary>
[SuppressMessage(
    "Major Code Smell",
    "S107:Methods should not have too many parameters",
    Justification = "Has more than 7 parameters - just expected for arity-N CombineLatest operator surface.")]
public static partial class SignalAsync
{
    /// <summary>
    /// Combines the latest values from sixteen asynchronous observable sources into a single
    /// sequence, projecting them through <paramref name="selector"/> whenever any source emits.
    /// </summary>
    /// <remarks>
    /// The returned sequence does not produce a value until every source has emitted at least
    /// once. After that, each new value from any source produces a fresh projection using the
    /// most recent value from each. Completion / failure of any source propagates downstream.
    /// </remarks>
    /// <typeparam name="T1">The element type of source 1.</typeparam>
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
    /// <typeparam name="T14">The element type of source 14.</typeparam>
    /// <typeparam name="T15">The element type of source 15.</typeparam>
    /// <typeparam name="T16">The element type of source 16.</typeparam>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    /// <param name="src1">Source observable 1 whose latest value is combined.</param>
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
    /// <param name="src14">Source observable 14 whose latest value is combined.</param>
    /// <param name="src15">Source observable 15 whose latest value is combined.</param>
    /// <param name="src16">Source observable 16 whose latest value is combined.</param>
    /// <param name="selector">Projects the latest value of every source into a result.</param>
    /// <returns>An observable sequence of projected results.</returns>
    public static IObservableAsync<TResult> CombineLatest<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, TResult>(
        this IObservableAsync<T1> src1,
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
        IObservableAsync<T14> src14,
        IObservableAsync<T15> src15,
        IObservableAsync<T16> src16,
        Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, TResult> selector) =>
        new CombineLatest16SignalAsync<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, TResult>(
            new(src1, src2, src3, src4, src5, src6, src7, src8, src9, src10, src11, src12, src13, src14, src15, src16),
            selector);

    /// <summary>
    /// Async observable that combines the latest values from sixteen source sequences using a selector.
    /// </summary>
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
    /// <typeparam name="T14">Element type of source 14.</typeparam>
    /// <typeparam name="T15">Element type of source 15.</typeparam>
    /// <typeparam name="T16">Element type of source 16.</typeparam>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    internal sealed class CombineLatest16SignalAsync<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, TResult>(
        CombineLatest16SignalAsync<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, TResult>.Sources sources,
        Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, TResult> selector) : SignalAsync<TResult>
    {
        /// <summary>
        /// Bundles the sixteen source observables so the subscription constructor stays at three
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
        /// <param name="Src14">Source observable 14.</param>
        /// <param name="Src15">Source observable 15.</param>
        /// <param name="Src16">Source observable 16.</param>
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
            IObservableAsync<T13> Src13,
            IObservableAsync<T14> Src14,
            IObservableAsync<T15> Src15,
            IObservableAsync<T16> Src16);

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
        /// Per-arity subscription holding the typed Optional slots, the pre-built indexed
        /// observers, the SubscribeAtAsync switch, and the selector invocation. Shared scaffolding
        /// (gate, lifecycle, ValuesLock, OnErrorResume, SubscribeSourcesAsync, DisposeAsync) lives
        /// in <see cref="CombineLatestCoordinatorBase{TResult}"/>; the per-source OnNext / OnError /
        /// OnCompleted forwarding lives in <see cref="CombineLatestIndexedObserver{TSource, TResult}"/>.
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

            /// <summary>Bit owned by source 14 inside the lifecycle's completion bitmask.</summary>
            private const int Source14Bit = 1 << 13;

            /// <summary>Bit owned by source 15 inside the lifecycle's completion bitmask.</summary>
            private const int Source15Bit = 1 << 14;

            /// <summary>Bit owned by source 16 inside the lifecycle's completion bitmask.</summary>
            private const int Source16Bit = 1 << 15;

            /// <summary>Bundled source observables.</summary>
            private readonly Sources _sources;

            /// <summary>The result selector function.</summary>
            private readonly Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, TResult> _selector;

            /// <summary>Indexed observer for source 1.</summary>
            private readonly CombineLatestIndexedObserver<T1, TResult> _obs1;

            /// <summary>Indexed observer for source 2.</summary>
            private readonly CombineLatestIndexedObserver<T2, TResult> _obs2;

            /// <summary>Indexed observer for source 3.</summary>
            private readonly CombineLatestIndexedObserver<T3, TResult> _obs3;

            /// <summary>Indexed observer for source 4.</summary>
            private readonly CombineLatestIndexedObserver<T4, TResult> _obs4;

            /// <summary>Indexed observer for source 5.</summary>
            private readonly CombineLatestIndexedObserver<T5, TResult> _obs5;

            /// <summary>Indexed observer for source 6.</summary>
            private readonly CombineLatestIndexedObserver<T6, TResult> _obs6;

            /// <summary>Indexed observer for source 7.</summary>
            private readonly CombineLatestIndexedObserver<T7, TResult> _obs7;

            /// <summary>Indexed observer for source 8.</summary>
            private readonly CombineLatestIndexedObserver<T8, TResult> _obs8;

            /// <summary>Indexed observer for source 9.</summary>
            private readonly CombineLatestIndexedObserver<T9, TResult> _obs9;

            /// <summary>Indexed observer for source 10.</summary>
            private readonly CombineLatestIndexedObserver<T10, TResult> _obs10;

            /// <summary>Indexed observer for source 11.</summary>
            private readonly CombineLatestIndexedObserver<T11, TResult> _obs11;

            /// <summary>Indexed observer for source 12.</summary>
            private readonly CombineLatestIndexedObserver<T12, TResult> _obs12;

            /// <summary>Indexed observer for source 13.</summary>
            private readonly CombineLatestIndexedObserver<T13, TResult> _obs13;

            /// <summary>Indexed observer for source 14.</summary>
            private readonly CombineLatestIndexedObserver<T14, TResult> _obs14;

            /// <summary>Indexed observer for source 15.</summary>
            private readonly CombineLatestIndexedObserver<T15, TResult> _obs15;

            /// <summary>Indexed observer for source 16.</summary>
            private readonly CombineLatestIndexedObserver<T16, TResult> _obs16;

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

            /// <summary>Latest value from source 14.</summary>
            private Optional<T14> _val14 = Optional<T14>.Empty;

            /// <summary>Latest value from source 15.</summary>
            private Optional<T15> _val15 = Optional<T15>.Empty;

            /// <summary>Latest value from source 16.</summary>
            private Optional<T16> _val16 = Optional<T16>.Empty;

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
            /// <param name="V14">Latest value from source 14.</param>
            /// <param name="V15">Latest value from source 15.</param>
            /// <param name="V16">Latest value from source 16.</param>
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
                T13 V13,
                T14 V14,
                T15 V15,
                T16 V16);

            /// <summary>
            /// Initializes a new instance of the <see cref="CombineLatestCoordinator"/> class.
            /// </summary>
            /// <param name="observer">The downstream observer.</param>
            /// <param name="sources">The bundled source observables.</param>
            /// <param name="selector">The selector that projects the latest values.</param>
            public CombineLatestCoordinator(
                IObserverAsync<TResult> observer,
                Sources sources,
                Func<T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, TResult> selector)
                : base(observer, sourceCount: 16)
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
                _obs14 = new(this, Source14Bit, v => _val14 = new(v));
                _obs15 = new(this, Source15Bit, v => _val15 = new(v));
                _obs16 = new(this, Source16Bit, v => _val16 = new(v));
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
                            values.V13,
                            values.V14,
                            values.V15,
                            values.V16);
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
                    12 => _sources.Src13.SubscribeAsync(_obs13, cancellationToken),
                    13 => _sources.Src14.SubscribeAsync(_obs14, cancellationToken),
                    14 => _sources.Src15.SubscribeAsync(_obs15, cancellationToken),
                    _ => _sources.Src16.SubscribeAsync(_obs16, cancellationToken),
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
                    && _val13.TryGetValue(out var v13)
                    && _val14.TryGetValue(out var v14)
                    && _val15.TryGetValue(out var v15)
                    && _val16.TryGetValue(out var v16))
                {
                    values = new(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16);
                    return true;
                }

                values = default;
                return false;
            }
        }
    }
}
