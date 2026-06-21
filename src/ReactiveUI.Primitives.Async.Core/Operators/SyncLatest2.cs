// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Provides the arity-2 (<c>two</c>-source) <c>CombineLatest</c> extension method
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
        /// Combines the latest values from two asynchronous observable sources into a single
        /// sequence, projecting them through <paramref name="selector"/> whenever any source emits.
        /// </summary>
        /// <remarks>
        /// The returned sequence does not produce a value until every source has emitted at least
        /// once. After that, each new value from any source produces a fresh projection using the
        /// most recent value from each. Completion / failure of any source propagates downstream.
        /// </remarks>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="src2">Source observable 2 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - just expected for arity-N CombineLatest operator surface.")]
        public IObservableAsync<TResult> SyncLatest<T2, TResult>(
            IObservableAsync<T2> src2,
            Func<T1, T2, TResult> selector) =>
            new SyncLatest2SignalAsync<T1, T2, TResult>(
                new(src1, src2),
                selector);

        /// <summary>
        /// Combines the latest values from two asynchronous observable sources into a single
        /// sequence, projecting them through <paramref name="selector"/> whenever any source emits.
        /// </summary>
        /// <typeparam name="T2">The element type of source 2.</typeparam>
        /// <typeparam name="TResult">The projected element type.</typeparam>
        /// <param name="src2">Source observable 2 whose latest value is combined.</param>
        /// <param name="selector">Projects the latest value of every source into a result.</param>
        /// <returns>An observable sequence of projected results.</returns>
        [SuppressMessage(
            "Major Code Smell",
            "S107:Methods should not have too many parameters",
            Justification = "Has more than 7 parameters - just expected for arity-N CombineLatest operator surface.")]
        public IObservableAsync<TResult> CombineLatest<T2, TResult>(
            IObservableAsync<T2> src2,
            Func<T1, T2, TResult> selector) =>
            src1.SyncLatest(src2, selector);

        /// <summary>Combines latest values from two sources.</summary>
        /// <typeparam name="T2">The second element type.</typeparam>
        /// <typeparam name="TResult">The result element type.</typeparam>
        /// <param name="src2">The second source sequence.</param>
        /// <param name="selector">The function combining the latest values.</param>
        /// <returns>An observable sequence of combined values.</returns>
        public IObservableAsync<TResult> PairLatest<T2, TResult>(
            IObservableAsync<T2> src2,
            Func<T1, T2, TResult> selector) =>
            src1.SyncLatest(src2, selector);
    }

    /// <summary>Async observable that combines the latest values from two source sequences using a selector.</summary>
    /// <typeparam name="T1">Element type of source 1.</typeparam>
    /// <typeparam name="T2">Element type of source 2.</typeparam>
    /// <typeparam name="TResult">The projected element type.</typeparam>
    /// <param name="sources">The bundled source observables.</param>
    /// <param name="selector">The selector that projects the latest values.</param>
    internal sealed class SyncLatest2SignalAsync<T1, T2, TResult>(
        SyncLatest2SignalAsync<T1, T2, TResult>.Sources sources,
        Func<T1, T2, TResult> selector) : IObservableAsync<TResult>
    {
        /// <inheritdoc/>
        ValueTask<IAsyncDisposable> IObservableAsync<TResult>.SubscribeAsync(
            IObserverAsync<TResult> observer,
            CancellationToken cancellationToken)
        {
            SyncLatestCoordinator subscription = new(observer, sources, selector);
            subscription.Lifecycle.LinkExternalCancellation(cancellationToken);
            return SubscriptionHelper.SubscribeAndDisposeOnFailureAsync(
                subscription,
                () => subscription.SubscribeSourcesAsync(cancellationToken));
        }

        /// <summary>
        /// Bundles the two source observables so the subscription constructor stays at three
        /// parameters (observer, sources, selector) regardless of arity. Sonar S107 caps method /
        /// constructor parameter count; the bundle keeps the internal types compliant.
        /// </summary>
        /// <param name="Src1">Source observable 1.</param>
        /// <param name="Src2">Source observable 2.</param>
        internal readonly record struct Sources(
            IObservableAsync<T1> Src1,
            IObservableAsync<T2> Src2);

        /// <summary>
        /// Per-arity subscription holding the typed Optional slots, the pre-built indexed
        /// observers, the SubscribeAtAsync switch, and the selector invocation. Shared scaffolding
        /// (gate, lifecycle, ValuesLock, OnErrorResume, SubscribeSourcesAsync, DisposeAsync) lives
        /// in <see cref="SyncLatestCoordinatorBase{TResult}"/>; the per-source OnNext / OnError /
        /// OnCompleted forwarding lives in <see cref="SyncLatestIndexedWitness{TSource, TResult}"/>.
        /// </summary>
        internal sealed class SyncLatestCoordinator : SyncLatestCoordinatorBase<TResult>
        {
            /// <summary>Bit owned by source 1 inside the lifecycle's completion bitmask.</summary>
            private const int Source1Bit = 1 << 0;

            /// <summary>Bit owned by source 2 inside the lifecycle's completion bitmask.</summary>
            private const int Source2Bit = 1 << 1;

            /// <summary>Bundled source observables.</summary>
            private readonly Sources _sources;

            /// <summary>The result selector function.</summary>
            private readonly Func<T1, T2, TResult> _selector;

            /// <summary>Indexed observer for source 1.</summary>
            private readonly SyncLatestIndexedWitness<T1, TResult> _obs1;

            /// <summary>Indexed observer for source 2.</summary>
            private readonly SyncLatestIndexedWitness<T2, TResult> _obs2;

            /// <summary>Latest value from source 1.</summary>
            private Optional<T1> _val1 = Optional<T1>.Empty;

            /// <summary>Latest value from source 2.</summary>
            private Optional<T2> _val2 = Optional<T2>.Empty;

            /// <summary>Initializes a new instance of the <see cref="SyncLatestCoordinator"/> class.</summary>
            /// <param name="observer">The downstream observer.</param>
            /// <param name="sources">The bundled source observables.</param>
            /// <param name="selector">The selector that projects the latest values.</param>
            public SyncLatestCoordinator(
                IObserverAsync<TResult> observer,
                Sources sources,
                Func<T1, T2, TResult> selector)
                : base(observer, sourceCount: 2)
            {
                _sources = sources;
                _selector = selector;
                _obs1 = new(this, Source1Bit, v => _val1 = new(v));
                _obs2 = new(this, Source2Bit, v => _val2 = new(v));
            }

            /// <inheritdoc/>
            internal override ValueTask EmitLatestAsync() =>
                TryReadValues(out var values)
                    ? Lifecycle.EmitDownstreamAsync(_selector(values.V1, values.V2))
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
                    _ => _sources.Src2.SubscribeAsync(_obs2, cancellationToken),
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
                    && _val2.TryGetValue(out var v2))
                {
                    values = new(v1, v2);
                    return true;
                }

                values = default;
                return false;
            }

            /// <summary>Latest-value snapshot taken when every source has produced at least one value.</summary>
            /// <param name="V1">Latest value from source 1.</param>
            /// <param name="V2">Latest value from source 2.</param>
            internal readonly record struct Values(
                T1 V1,
                T2 V2);
        }
    }
}
