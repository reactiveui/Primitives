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
/// <summary>An observable that switches the notification context of a source observable to a specified async context.</summary>
/// <typeparam name = "T">The type of elements in the observable sequence.</typeparam>
/// <param name = "source">The source observable whose notifications will be context-switched.</param>
/// <param name = "asyncContext">The async context to switch notifications onto.</param>
/// <param name = "forceYielding">Whether to yield even when the calling thread is on the target context.</param>
public sealed class ContextSwitchSignalAsync<T>(
    IObservableAsync<T> source,
    AsyncContext asyncContext,
    bool forceYielding) : IObservableAsync<T>
{
    ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        ContextSwitchWitness contextSwitchObserver = new(observer, asyncContext, forceYielding);
        return source.SubscribeAsync(contextSwitchObserver, cancellationToken);
    }

    /// <summary>An observer that switches each notification onto the specified async context before forwarding.</summary>
    /// <param name = "observer">The downstream observer to forward notifications to.</param>
    /// <param name = "asyncContext">The async context to switch onto.</param>
    /// <param name = "forceYielding">Whether to yield even when the calling thread is on the target context.</param>
    [DebuggerDisplay("ContextSwitchWitness: {_witness}")]
    internal sealed class ContextSwitchWitness(
        IObserverAsync<T> observer,
        AsyncContext asyncContext,
        bool forceYielding) : IWitnessAsync<T>
    {
        /// <summary>The notification gate, cancellation link and disposal state.</summary>
        private WitnessAsyncState _witness;

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

        /// <summary>Switches to the target context before forwarding the value.</summary>
        /// <param name = "value">The value to forward.</param>
        /// <param name = "cancellationToken">The cancellation token.</param>
        /// <returns>A task that completes after the context switch and downstream forward.</returns>
        internal async ValueTask ForwardAfterContextSwitchAsync(T value, CancellationToken cancellationToken)
        {
            await asyncContext.SwitchContextAsync(forceYielding, cancellationToken);
            await observer.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Switches to the target context before forwarding the error.</summary>
        /// <param name = "error">The error to forward.</param>
        /// <param name = "cancellationToken">The cancellation token.</param>
        /// <returns>A task that completes after the context switch and downstream forward.</returns>
        internal async ValueTask ForwardErrorAfterContextSwitchAsync(
            Exception error,
            CancellationToken cancellationToken)
        {
            await asyncContext.SwitchContextAsync(forceYielding, cancellationToken);
            await observer.OnErrorResumeAsync(error, cancellationToken).ConfigureAwait(false);
        }

        /// <summary>Switches to the target context before forwarding completion.</summary>
        /// <param name = "result">The completion result.</param>
        /// <returns>A task that completes after the context switch and downstream forward.</returns>
        internal async ValueTask ForwardCompletionAfterContextSwitchAsync(Result result)
        {
            await asyncContext.SwitchContextAsync(forceYielding, CancellationToken.None);
            await observer.OnCompletedAsync(result).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken) =>
            // A matching context needs no switch unless yielding is forced.
            !forceYielding && asyncContext.IsSameAsCurrentAsyncContext()
                ? observer.OnNextAsync(value, cancellationToken)
                : ForwardAfterContextSwitchAsync(value, cancellationToken);

        /// <inheritdoc/>
        ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
            !forceYielding && asyncContext.IsSameAsCurrentAsyncContext()
                ? observer.OnErrorResumeAsync(error, cancellationToken)
                : ForwardErrorAfterContextSwitchAsync(error, cancellationToken);

        /// <inheritdoc/>
        ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
            !forceYielding && asyncContext.IsSameAsCurrentAsyncContext()
                ? observer.OnCompletedAsync(result)
                : ForwardCompletionAfterContextSwitchAsync(result);
    }
}
