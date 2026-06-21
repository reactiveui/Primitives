// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Diagnostics.CodeAnalysis;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive;
#else
namespace ReactiveUI.Primitives.Async;
#endif

/// <summary>Fused shim-typed projection observable that backs the parity-helper extension methods.</summary>
[SuppressMessage("Major Code Smell", "S3604:Member initializer values should not be redundant", Justification = "Primary-constructor parameters are captured into observer state.")]
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
        async ValueTask<IAsyncDisposable> IObservableAsync<RxVoid>.SubscribeAsync(IObserverAsync<RxVoid> observer, CancellationToken cancellationToken)
        {
            AsSignalWitness sink = new(observer, cancellationToken);
            if (observer is WitnessAsync<RxVoid> downstreamBase)
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
        internal sealed class AsSignalWitness(IObserverAsync<RxVoid> downstream, CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
        {
            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken) => downstream.OnNextAsync(RxVoid.Default, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) => downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) => downstream.OnCompletedAsync(result);
        }
    }
}
