// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Provides Probe extension methods for asynchronous observable sequences.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Sampling operators for an observable source sequence.</summary>
    /// <typeparam name="T">The type of elements in the source sequence.</typeparam>
    /// <param name="source">The source observable sequence.</param>
    extension<T>(IObservableAsync<T> source)
    {
        /// <summary>Emits the latest element once the period has passed since the element that started the timer.</summary>
        /// <param name="period">The sampling period. Must be non-negative.</param>
        /// <returns>A sequence carrying the latest element once each period elapses; a quiet source sends nothing.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="period"/> is negative.</exception>
        /// <remarks>An element still waiting when the source completes is sent before the completion.</remarks>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Probe(TimeSpan period) => source.Probe(period, null);

        /// <summary>Emits the latest element once the period has passed since the element that started the timer.</summary>
        /// <param name="period">The sampling period. Must be non-negative.</param>
        /// <param name="timeProvider">An optional time provider. If null, <see cref="TimeProvider.System"/> is used.</param>
        /// <returns>A sequence carrying the latest element once each period elapses; a quiet source sends nothing.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="period"/> is negative.</exception>
        /// <remarks>An element still waiting when the source completes is sent before the completion.</remarks>
        public IObservableAsync<T> Probe(TimeSpan period, TimeProvider? timeProvider)
        {
            ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(period, TimeSpan.Zero);

            return new ProbeSignal<T>(source, period, timeProvider ?? TimeProvider.System);
        }

        /// <summary>Another name for <c>Probe</c>.</summary>
        /// <param name="interval">The sampling period. Must be non-negative.</param>
        /// <returns>A sequence carrying the latest element once each period elapses.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="interval"/> is negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Sample(TimeSpan interval) => source.Probe(interval, null);

        /// <summary>Another name for <c>Probe</c>, taking a time provider.</summary>
        /// <param name="interval">The sampling period. Must be non-negative.</param>
        /// <param name="timeProvider">An optional time provider. If null, <see cref="TimeProvider.System"/> is used.</param>
        /// <returns>A sequence carrying the latest element once each period elapses.</returns>
        /// <exception cref="ArgumentOutOfRangeException">Thrown if <paramref name="interval"/> is negative.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservableAsync<T> Sample(TimeSpan interval, TimeProvider? timeProvider) =>
            source.Probe(interval, timeProvider);
    }

    /// <summary>Async observable that forwards the latest source element once each sampling period elapses.</summary>
    /// <typeparam name="T">The type of elements in the sequence.</typeparam>
    /// <param name="source">The source observable sequence to sample.</param>
    /// <param name="period">The sampling period.</param>
    /// <param name="timeProvider">The time provider used for the sampling timer.</param>
    internal sealed class ProbeSignal<T>(IObservableAsync<T> source, TimeSpan period, TimeProvider timeProvider) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken) =>
            source.SubscribeAsync(new ProbeWitness(observer, period, timeProvider), cancellationToken);

        /// <summary>Holds the latest element and forwards it when the sampling timer elapses.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="period">The sampling period.</param>
        /// <param name="timeProvider">The time provider used for the sampling timer.</param>
        [DebuggerDisplay("ProbeWitness: {_witness}")]
        internal sealed class ProbeWitness(IObserverAsync<T> observer, TimeSpan period, TimeProvider timeProvider) : IWitnessAsync<T>
        {
            /// <summary>Guards the held element and the timer flag.</summary>
            private readonly Lock _gate = new();

            /// <summary>The most recent element; valid only while <see cref="_hasLatest"/> is set.</summary>
            private T? _latest;

            /// <summary>Whether an element has arrived since the last tick.</summary>
            private bool _hasLatest;

            /// <summary>Whether a sampling timer is already running.</summary>
            private bool _timerActive;

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
            public ValueTask DisposeAsync()
            {
                lock (_gate)
                {
                    _hasLatest = false;
                    _timerActive = false;
                }

                return WitnessAsync.DisposeStateAsync(this);
            }

            /// <inheritdoc/>
            ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                _ = HoldAsync(value, cancellationToken);
                return default;
            }

            /// <summary>Holds the element, starting a sampling timer when none is running.</summary>
            /// <param name="value">The element to hold until the next tick.</param>
            /// <param name="cancellationToken">Cancellation for the sampling timer.</param>
            /// <returns>The timer operation, or a completed task when a timer is already running.</returns>
            internal Task HoldAsync(T value, CancellationToken cancellationToken)
            {
                bool startTimer;
                lock (_gate)
                {
                    _latest = value;
                    _hasLatest = true;
                    startTimer = !_timerActive;
                    _timerActive = true;
                }

                return startTimer ? TickAfterPeriodAsync(cancellationToken) : Task.CompletedTask;
            }

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                observer.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result)
            {
                T? pending;
                bool hasPending;
                lock (_gate)
                {
                    hasPending = _hasLatest;
                    pending = _latest;
                    _hasLatest = false;
                    _timerActive = false;
                }

                // An element that arrived since the last tick still goes out, ahead of the completion.
                return hasPending
                    ? SendThenCompleteAsync(pending!, result)
                    : observer.OnCompletedAsync(result);
            }

            /// <summary>Waits one period and forwards whichever element is held when it elapses.</summary>
            /// <param name="cancellationToken">A token that cancels the wait.</param>
            /// <returns>The wait and forwarding operation.</returns>
            internal async Task TickAfterPeriodAsync(CancellationToken cancellationToken)
            {
                try
                {
                    await DelayAsync(period, timeProvider, cancellationToken).ConfigureAwait(false);

                    T? pending;
                    bool hasPending;
                    lock (_gate)
                    {
                        _timerActive = false;
                        hasPending = _hasLatest;
                        pending = _latest;
                        _hasLatest = false;
                    }

                    if (hasPending)
                    {
                        await observer.OnNextAsync(pending!, cancellationToken).ConfigureAwait(false);
                    }
                }
                catch (Exception e)
                {
                    UnhandledExceptionHandler.ReportUnhandledException(e);
                }
            }

            /// <summary>Forwards a held element and then the terminal result.</summary>
            /// <param name="value">The element to forward.</param>
            /// <param name="result">The terminal result.</param>
            /// <returns>The forwarding operation.</returns>
            private async ValueTask SendThenCompleteAsync(T value, Result result)
            {
                await observer.OnNextAsync(value, CancellationToken.None).ConfigureAwait(false);
                await observer.OnCompletedAsync(result).ConfigureAwait(false);
            }
        }
    }
}
