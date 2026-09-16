// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive;
#else
namespace ReactiveUI.Primitives.Async;
#endif

/// <summary>Fused shim-typed projection observable that backs the parity-helper extension methods.</summary>
public static partial class SignalAsyncReactiveExtensions
{
    /// <summary>
    /// Fuses <c>source.Select(static _ =&gt; RxVoid.Default)</c> into a single observer layer; every
    /// upstream emission is forwarded as <see cref = "RxVoid.Default"/> with no closure capture.
    /// </summary>
    /// <typeparam name = "T">The upstream element type.</typeparam>
    /// <param name = "source">The upstream observable.</param>
    internal sealed class AsRxVoidSignal<T>(IObservableAsync<T> source) : IObservableAsync<RxVoid>
    {
        async ValueTask<IAsyncDisposable> IObservableAsync<RxVoid>.SubscribeAsync(
            IObserverAsync<RxVoid> observer,
            CancellationToken cancellationToken)
        {
            AsSignalWitness sink = new(observer, cancellationToken);
            if (observer is IWitnessAsync<RxVoid> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Forwards <see cref = "RxVoid.Default"/> for every upstream emission.</summary>
        /// <param name = "downstream">The downstream observer.</param>
        /// <param name = "subscribeToken">The subscribe-time cancellation token.</param>
        [DebuggerDisplay("AsSignalWitness: {_witness}")]
        internal sealed class AsSignalWitness(IObserverAsync<RxVoid> downstream, CancellationToken subscribeToken) : IWitnessAsync<T>
        {
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
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
                downstream.OnNextAsync(RxVoid.Default, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) => downstream.OnCompletedAsync(result);
        }
    }
}
