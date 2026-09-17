// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async;

/// <summary>Fused operator observables backing the parity-helper extension methods in <see cref="SignalAsyncExtensions"/>.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>Emits the initial accumulator on subscribe, then folds each source value into it and forwards the result.</summary>
    /// <typeparam name="TSource">The upstream element type.</typeparam>
    /// <typeparam name="TAccumulate">The accumulator type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="initial">The initial accumulator value, emitted on subscribe.</param>
    /// <param name="accumulator">The synchronous accumulator.</param>
    internal sealed class ScanWithInitialSignal<TSource, TAccumulate>(
        IObservableAsync<TSource> source,
        TAccumulate initial,
        Func<TAccumulate, TSource, TAccumulate> accumulator) : IObservableAsync<TAccumulate>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<TAccumulate>.SubscribeAsync(
            IObserverAsync<TAccumulate> observer,
            CancellationToken cancellationToken)
        {
            ScanWithInitialWitness sink = new(observer, initial, accumulator, cancellationToken);

            if (observer is IWitnessAsync<TAccumulate> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            await observer.OnNextAsync(initial, cancellationToken).ConfigureAwait(false);

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription accumulator observer.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="seed">The seed accumulator value emitted during subscription.</param>
        /// <param name="accumulator">The synchronous accumulator.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        [DebuggerDisplay("ScanWithInitialWitness: {_witness}")]
        internal sealed class ScanWithInitialWitness(
            IObserverAsync<TAccumulate> downstream,
            TAccumulate seed,
            Func<TAccumulate, TSource, TAccumulate> accumulator,
            CancellationToken subscribeToken) : IWitnessAsync<TSource>
        {
            /// <summary>Running accumulator state; seeded with the initial value.</summary>
            private TAccumulate _accumulator = seed;

            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness = new(subscribeToken);

            /// <inheritdoc/>
            ref WitnessAsyncState IWitnessState.Witness => ref _witness;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnNextAsync(TSource value, CancellationToken cancellationToken) =>
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
            ValueTask IWitnessAsync<TSource>.OnNextAsyncCore(TSource value, CancellationToken cancellationToken)
            {
                _accumulator = accumulator(_accumulator, value);
                return downstream.OnNextAsync(_accumulator, cancellationToken);
            }

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<TSource>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<TSource>.OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }

    /// <summary>Async-accumulator variant of <see cref="ScanWithInitialSignal{TSource, TAccumulate}"/>.</summary>
    /// <typeparam name="TSource">The upstream element type.</typeparam>
    /// <typeparam name="TAccumulate">The accumulator type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="initial">The initial accumulator value, emitted on subscribe.</param>
    /// <param name="accumulator">The asynchronous accumulator.</param>
    internal sealed class ScanWithInitialAsyncSignal<TSource, TAccumulate>(
        IObservableAsync<TSource> source,
        TAccumulate initial,
        Func<TAccumulate, TSource, CancellationToken, ValueTask<TAccumulate>> accumulator) : IObservableAsync<TAccumulate>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<TAccumulate>.SubscribeAsync(
            IObserverAsync<TAccumulate> observer,
            CancellationToken cancellationToken)
        {
            ScanWithInitialAsyncWitness sink = new(observer, initial, accumulator, cancellationToken);

            if (observer is IWitnessAsync<TAccumulate> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            await observer.OnNextAsync(initial, cancellationToken).ConfigureAwait(false);

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription async accumulator observer.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="seed">The seed accumulator value emitted during subscription.</param>
        /// <param name="accumulator">The asynchronous accumulator.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        [DebuggerDisplay("ScanWithInitialAsyncWitness: {_witness}")]
        internal sealed class ScanWithInitialAsyncWitness(
            IObserverAsync<TAccumulate> downstream,
            TAccumulate seed,
            Func<TAccumulate, TSource, CancellationToken, ValueTask<TAccumulate>> accumulator,
            CancellationToken subscribeToken) : IWitnessAsync<TSource>
        {
            /// <summary>Running accumulator state; seeded with the initial value.</summary>
            private TAccumulate _accumulator = seed;

            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness = new(subscribeToken);

            /// <inheritdoc/>
            ref WitnessAsyncState IWitnessState.Witness => ref _witness;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnNextAsync(TSource value, CancellationToken cancellationToken) =>
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
            ValueTask IWitnessAsync<TSource>.OnNextAsyncCore(TSource value, CancellationToken cancellationToken)
            {
                var pending = accumulator(_accumulator, value, cancellationToken);
                if (pending.IsCompletedSuccessfully)
                {
                    _accumulator = pending.Result;
                    return downstream.OnNextAsync(_accumulator, cancellationToken);
                }

                return AwaitAndForwardAsync(pending, cancellationToken);
            }

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<TSource>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<TSource>.OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);

            /// <summary>Slow path for asynchronously-completing accumulators.</summary>
            /// <param name="pending">The pending accumulator <see cref="ValueTask{TResult}"/>.</param>
            /// <param name="cancellationToken">The cancellation token to pass downstream.</param>
            /// <returns>A task that completes after the accumulator resolves and the downstream emission completes.</returns>
            private async ValueTask AwaitAndForwardAsync(
                ValueTask<TAccumulate> pending,
                CancellationToken cancellationToken)
            {
                _accumulator = await pending.ConfigureAwait(false);
                await downstream.OnNextAsync(_accumulator, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    /// <summary>Debounces the source, dropping repeated values both before the delay and before each forward, so only the newest pending value is emitted.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="dueTime">The debounce window.</param>
    /// <param name="timeProvider">The time provider for the debounce timer.</param>
    internal sealed class ThrottleDistinctSignal<T>(
        IObservableAsync<T> source,
        TimeSpan dueTime,
        TimeProvider timeProvider) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken) =>
            WitnessSubscription.SubscribeAsync(
                source,
                new ThrottleDistinctWitness(observer, dueTime, timeProvider, cancellationToken),
                observer,
                cancellationToken);

        /// <summary>Per-subscription witness fusing upstream-distinct + debounce + downstream-distinct.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="dueTime">The debounce window.</param>
        /// <param name="timeProvider">The time provider used for the debounce timer.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        [DebuggerDisplay("ThrottleDistinctWitness: {_witness}")]
        internal sealed class ThrottleDistinctWitness(
            IObserverAsync<T> downstream,
            TimeSpan dueTime,
            TimeProvider timeProvider,
            CancellationToken subscribeToken) : IWitnessAsync<T>
        {
            /// <summary>Equality comparer used for both distinct layers.</summary>
            private static readonly EqualityComparer<T> Comparer = EqualityComparer<T>.Default;

            /// <summary>Synchronization gate protecting throttle/distinct state.</summary>
            private readonly Lock _gate = new();

            /// <summary>Most-recent upstream value (for upstream DistinctUntilChanged).</summary>
            private T _lastUpstream = default!;

            /// <summary>Most-recently-forwarded value (for downstream DistinctUntilChanged).</summary>
            private T _lastEmitted = default!;

            /// <summary>Set to <see langword="true"/> after the first upstream emission has been seen.</summary>
            private bool _hasUpstream;

            /// <summary>Set to <see langword="true"/> after the first value has been forwarded downstream.</summary>
            private bool _hasEmitted;

            /// <summary>Monotonically increasing identifier stamped on each pending delay; a mismatch marks it superseded.</summary>
            private long _id;

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
            public ValueTask DisposeAsync()
            {
                lock (_gate)
                {
                    _id++;
                }

                return WitnessAsync.DisposeStateAsync(this);
            }

            /// <summary>Claims the emission when the stamped id is current and the value differs from the last forwarded one.</summary>
            /// <param name="value">The candidate value.</param>
            /// <param name="id">The id stamped when this delay was started.</param>
            /// <returns><see langword="true"/> when the caller should forward the value; <see langword="false"/> when it
            /// was superseded or duplicates the last forwarded value.</returns>
            internal bool TryClaimEmission(T value, long id)
            {
                lock (_gate)
                {
                    if (_id != id)
                    {
                        return false;
                    }

                    if (_hasEmitted && Comparer.Equals(value, _lastEmitted))
                    {
                        return false;
                    }

                    _lastEmitted = value;
                    _hasEmitted = true;
                    return true;
                }
            }

            /// <summary>Starts a delay for a distinct upstream value and supersedes the previous pending value.</summary>
            /// <param name="value">The upstream value.</param>
            /// <param name="cancellationToken">Cancellation for the delay.</param>
            /// <returns>The pending emission, or a completed task for a duplicate value.</returns>
            internal Task StartDelayAsync(T value, CancellationToken cancellationToken)
            {
                long currentId;
                lock (_gate)
                {
                    if (_hasUpstream && Comparer.Equals(value, _lastUpstream))
                    {
                        return Task.CompletedTask;
                    }

                    _lastUpstream = value;
                    _hasUpstream = true;
                    currentId = ++_id;
                }

                return FireAfterDelayAsync(value, currentId, cancellationToken);
            }

            /// <inheritdoc/>
            ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                _ = StartDelayAsync(value, cancellationToken);
                return default;
            }

            /// <inheritdoc/>
            ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
            {
                lock (_gate)
                {
                    _id++;
                }

                return downstream.OnErrorResumeAsync(error, cancellationToken);
            }

            /// <inheritdoc/>
            ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result)
            {
                lock (_gate)
                {
                    _id++;
                }

                return downstream.OnCompletedAsync(result);
            }

            /// <summary>Waits the debounce window, then forwards the value when <see cref="TryClaimEmission"/> approves it.</summary>
            /// <param name="value">The candidate value.</param>
            /// <param name="id">The id stamped when this delay was started.</param>
            /// <param name="cancellationToken">The cancellation token.</param>
            /// <returns>A task representing the asynchronous wait-and-maybe-forward operation.</returns>
            private async Task FireAfterDelayAsync(T value, long id, CancellationToken cancellationToken)
            {
                try
                {
                    await DelayAsync(dueTime, timeProvider, cancellationToken).ConfigureAwait(false);

                    if (!TryClaimEmission(value, id))
                    {
                        return;
                    }

                    await downstream.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    UnhandledExceptionHandler.ReportUnhandledException(e);
                }
            }
        }
    }

    /// <summary>Runs the action for one value at a time and drops the values that arrive while it is running.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="asyncAction">The async side-effect invoked for accepted values.</param>
    internal sealed class DropIfBusySignal<T>(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask> asyncAction) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken) =>
            WitnessSubscription.SubscribeAsync(
                source,
                new DropIfBusyWitness(observer, asyncAction, cancellationToken),
                observer,
                cancellationToken);

        /// <summary>Per-subscription witness that drops upstream emissions while a prior action is pending.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="asyncAction">The async side-effect invoked for accepted values.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain.</param>
        [DebuggerDisplay("DropIfBusyWitness: {_witness}")]
        internal sealed class DropIfBusyWitness(
            IObserverAsync<T> downstream,
            Func<T, CancellationToken, ValueTask> asyncAction,
            CancellationToken subscribeToken) : IWitnessAsync<T>
        {
            /// <summary>0 when idle, 1 while an emission is being processed.</summary>
            private int _isBusy;

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
                if (Interlocked.CompareExchange(ref _isBusy, 1, 0) != 0)
                {
                    return default;
                }

                ValueTask actionTask;
                try
                {
                    actionTask = asyncAction(value, cancellationToken);
                }
                catch
                {
                    Volatile.Write(ref _isBusy, 0);
                    throw;
                }

                if (actionTask.IsCompletedSuccessfully)
                {
                    var forward = downstream.OnNextAsync(value, cancellationToken);
                    if (forward.IsCompletedSuccessfully)
                    {
                        Volatile.Write(ref _isBusy, 0);
                        return default;
                    }

                    return AwaitForwardAsync(forward);
                }

                return AwaitFullAsync(actionTask, value, cancellationToken);
            }

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);

            /// <summary>Slow path when the downstream forwarding is asynchronous but the inner action completed sync.</summary>
            /// <param name="forward">The pending downstream forward.</param>
            /// <returns>A task that completes after the forward resolves and the busy flag is reset.</returns>
            private async ValueTask AwaitForwardAsync(ValueTask forward)
            {
                try
                {
                    await forward.ConfigureAwait(false);
                }
                finally
                {
                    Volatile.Write(ref _isBusy, 0);
                }
            }

            /// <summary>Slow path when the inner action does not complete synchronously.</summary>
            /// <param name="actionTask">The pending inner action.</param>
            /// <param name="value">The value being processed.</param>
            /// <param name="cancellationToken">The cancellation token.</param>
            /// <returns>A task that completes after both the action and downstream forwarding resolve.</returns>
            private async ValueTask AwaitFullAsync(ValueTask actionTask, T value, CancellationToken cancellationToken)
            {
                try
                {
                    await actionTask.ConfigureAwait(false);
                    await downstream.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
                }
                finally
                {
                    Volatile.Write(ref _isBusy, 0);
                }
            }
        }
    }

    /// <summary>Forwards a value immediately when the condition holds, and otherwise after the debounce window, discarding values a newer one supersedes.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="debounce">The debounce window applied to bypass-false values.</param>
    /// <param name="condition">When <see langword="true"/> the value bypasses the delay and is forwarded immediately.</param>
    /// <param name="timeProvider">The time provider for the debounce timer.</param>
    internal sealed class DebounceUntilSignal<T>(
        IObservableAsync<T> source,
        TimeSpan debounce,
        Func<T, bool> condition,
        TimeProvider timeProvider) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken) =>
            WitnessSubscription.SubscribeAsync(
                source,
                new DebounceUntilWitness(observer, debounce, condition, timeProvider, cancellationToken),
                observer,
                cancellationToken);

        /// <summary>Per-subscription observer that fuses the bypass-condition + Switch-debounce pipeline.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="debounce">The debounce window.</param>
        /// <param name="condition">The bypass-the-delay condition.</param>
        /// <param name="timeProvider">The time provider used for the debounce timer.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        [DebuggerDisplay("DebounceUntilWitness: {_witness}")]
        internal sealed class DebounceUntilWitness(
            IObserverAsync<T> downstream,
            TimeSpan debounce,
            Func<T, bool> condition,
            TimeProvider timeProvider,
            CancellationToken subscribeToken) : IWitnessAsync<T>
        {
            /// <summary>Synchronization gate protecting the id counter.</summary>
            private readonly Lock _gate = new();

            /// <summary>Monotonically increasing identifier stamped on each pending delay; a newer value supersedes the old id.</summary>
            private long _id;

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
            public ValueTask DisposeAsync()
            {
                lock (_gate)
                {
                    _id++;
                }

                return WitnessAsync.DisposeStateAsync(this);
            }

            /// <summary>Reports whether the stamped id is the current one, so no newer value has superseded this delay.</summary>
            /// <param name="id">The id stamped when this delay was started.</param>
            /// <returns><see langword="true"/> when the caller should forward the value; <see langword="false"/> when a
            /// newer upstream value superseded it.</returns>
            internal bool IsCurrentEmission(long id)
            {
                lock (_gate)
                {
                    return _id == id;
                }
            }

            /// <summary>Starts a delay for the value, superseding any pending one.</summary>
            /// <param name="value">The pending value.</param>
            /// <param name="cancellationToken">Cancellation for the delay.</param>
            /// <returns>The delay and any downstream notification.</returns>
            internal Task StartDelayAsync(T value, CancellationToken cancellationToken)
            {
                long currentId;
                lock (_gate)
                {
                    currentId = ++_id;
                }

                return DelayAndEmitAsync(value, currentId, cancellationToken);
            }

            /// <inheritdoc/>
            ValueTask IWitnessAsync<T>.OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (condition(value))
                {
                    // Bypass cancels pending delayed delivery and emits immediately.
                    lock (_gate)
                    {
                        _id++;
                    }

                    return downstream.OnNextAsync(value, cancellationToken);
                }

                _ = StartDelayAsync(value, cancellationToken);
                return default;
            }

            /// <inheritdoc/>
            ValueTask IWitnessAsync<T>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
            {
                lock (_gate)
                {
                    _id++;
                }

                return downstream.OnErrorResumeAsync(error, cancellationToken);
            }

            /// <inheritdoc/>
            ValueTask IWitnessAsync<T>.OnCompletedAsyncCore(Result result)
            {
                lock (_gate)
                {
                    _id++;
                }

                return downstream.OnCompletedAsync(result);
            }

            /// <summary>Waits the debounce window, then forwards the value when <see cref="IsCurrentEmission"/> confirms it.</summary>
            /// <param name="value">The candidate value.</param>
            /// <param name="id">The id stamped when this delay was started.</param>
            /// <param name="cancellationToken">The cancellation token.</param>
            /// <returns>A task representing the asynchronous wait-and-maybe-forward operation.</returns>
            private async Task DelayAndEmitAsync(T value, long id, CancellationToken cancellationToken)
            {
                try
                {
                    await DelayAsync(debounce, timeProvider, cancellationToken).ConfigureAwait(false);

                    if (!IsCurrentEmission(id))
                    {
                        return;
                    }

                    await downstream.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    UnhandledExceptionHandler.ReportUnhandledException(e);
                }
            }
        }
    }

    /// <summary>Forwards every element of each enumerable the source emits, in order.</summary>
    /// <typeparam name="T">The flattened element type.</typeparam>
    /// <param name="source">The upstream observable of <see cref="IEnumerable{T}"/> snapshots.</param>
    internal sealed class ForEachEnumerableSignal<T>(IObservableAsync<IEnumerable<T>> source) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken) =>
            WitnessSubscription.SubscribeAsync(
                source,
                new ForEachEnumerableWitness(observer, cancellationToken),
                observer,
                cancellationToken);

        /// <summary>Per-subscription witness that flattens each upstream enumerable inline.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        [DebuggerDisplay("ForEachEnumerableWitness: {_witness}")]
        internal sealed class ForEachEnumerableWitness(
            IObserverAsync<T> downstream,
            CancellationToken subscribeToken) : IWitnessAsync<IEnumerable<T>>
        {
            /// <summary>The notification gate, cancellation link and disposal state.</summary>
            private WitnessAsyncState _witness = new(subscribeToken);

            /// <inheritdoc/>
            ref WitnessAsyncState IWitnessState.Witness => ref _witness;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public ValueTask OnNextAsync(IEnumerable<T> value, CancellationToken cancellationToken) =>
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
            async ValueTask IWitnessAsync<IEnumerable<T>>.OnNextAsyncCore(
                IEnumerable<T> value,
                CancellationToken cancellationToken)
            {
                if (value is T[] array)
                {
                    for (var i = 0; i < array.Length; i++)
                    {
                        await downstream.OnNextAsync(array[i], cancellationToken).ConfigureAwait(false);
                    }

                    return;
                }

                if (value is IReadOnlyList<T> list)
                {
                    for (var i = 0; i < list.Count; i++)
                    {
                        await downstream.OnNextAsync(list[i], cancellationToken).ConfigureAwait(false);
                    }

                    return;
                }

                foreach (var item in value)
                {
                    await downstream.OnNextAsync(item, cancellationToken).ConfigureAwait(false);
                }
            }

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<IEnumerable<T>>.OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            ValueTask IWitnessAsync<IEnumerable<T>>.OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }
}
