// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides SkipWhile extension methods for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>SkipWhile operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Bypasses elements in the observable sequence as long as the specified asynchronous condition is true, then emits all remaining elements.</summary>
        /// <param name="predicate">An asynchronous function to test each element for a condition. Receives the element
        /// and a cancellation token.</param>
        /// <returns>An observable sequence that skips elements while the predicate returns true and emits
        /// all subsequent elements.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
        public IObservableAsync<T> SkipWhile(Func<T, CancellationToken, ValueTask<bool>> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(predicate);

            return new SkipWhileAsyncSignal<T>(source, predicate);
        }

        /// <summary>Bypasses elements in the observable sequence as long as the specified condition is true, then emits all remaining elements.</summary>
        /// <param name="predicate">A function to test each element for a condition.</param>
        /// <returns>An observable sequence that skips elements while the predicate returns true and emits
        /// all subsequent elements.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="predicate"/> is <see langword="null"/>.</exception>
        public IObservableAsync<T> SkipWhile(Func<T, bool> predicate)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);
            ArgumentExceptionHelper.ThrowIfNull(predicate);

            return new SkipWhileSyncSignal<T>(source, predicate);
        }
    }

    /// <summary>Skips values while the predicate holds; after it first fails, every later value forwards untested.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="predicate">The skip-while predicate.</param>
    internal sealed class SkipWhileSyncSignal<T>(IObservableAsync<T> source, Func<T, bool> predicate) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken) =>
            WitnessSubscription.SubscribeAsync(
                source,
                new SkipWhileSyncWitness(observer, predicate, cancellationToken),
                observer,
                cancellationToken);

        /// <summary>Per-subscription observer maintaining the latched-gate state.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="predicate">The skip-while predicate.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        [DebuggerDisplay("SkipWhileSyncWitness: {_witness}")]
        internal sealed class SkipWhileSyncWitness(
            IObserverAsync<T> downstream,
            Func<T, bool> predicate,
            CancellationToken subscribeToken) : IWitnessAsync<T>
        {
            /// <summary>Latches to <see langword="false"/> once the predicate fails - every subsequent value forwards.</summary>
            private bool _skipping = true;

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
                if (_skipping)
                {
                    if (predicate(value))
                    {
                        return default;
                    }

                    _skipping = false;
                }

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

    /// <summary>Skips values while the asynchronous predicate holds, then forwards every later value without awaiting it.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="predicate">The async skip-while predicate.</param>
    internal sealed class SkipWhileAsyncSignal<T>(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask<bool>> predicate) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken) =>
            WitnessSubscription.SubscribeAsync(
                source,
                new SkipWhileAsyncWitness(observer, predicate, cancellationToken),
                observer,
                cancellationToken);

        /// <summary>Per-subscription observer with a latched gate; once the gate opens, the predicate is not invoked again.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="predicate">The async skip-while predicate.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        [DebuggerDisplay("SkipWhileAsyncWitness: {_witness}")]
        internal sealed class SkipWhileAsyncWitness(
            IObserverAsync<T> downstream,
            Func<T, CancellationToken, ValueTask<bool>> predicate,
            CancellationToken subscribeToken) : IWitnessAsync<T>
        {
            /// <summary>Latches to <see langword="false"/> after the predicate first returns <see langword="false"/>.</summary>
            private bool _skipping = true;

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
                if (!_skipping)
                {
                    return downstream.OnNextAsync(value, cancellationToken);
                }

                var pending = predicate(value, cancellationToken);
                if (pending.IsCompletedSuccessfully)
                {
                    if (pending.Result)
                    {
                        return default;
                    }

                    _skipping = false;
                    return downstream.OnNextAsync(value, cancellationToken);
                }

                return EvaluateAndForwardAsync(pending, value, cancellationToken);
            }

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);

            /// <summary>Slow path when the async predicate does not complete synchronously.</summary>
            /// <param name="pending">The pending predicate evaluation.</param>
            /// <param name="value">The candidate value.</param>
            /// <param name="cancellationToken">The cancellation token.</param>
            /// <returns>A task that completes after the predicate resolves and (if not skipped) the downstream forward completes.</returns>
            private async ValueTask EvaluateAndForwardAsync(
                ValueTask<bool> pending,
                T value,
                CancellationToken cancellationToken)
            {
                if (await pending.ConfigureAwait(false))
                {
                    return;
                }

                _skipping = false;
                await downstream.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
