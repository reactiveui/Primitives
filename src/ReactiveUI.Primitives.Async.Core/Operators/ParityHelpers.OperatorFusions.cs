// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async;

/// <summary>Fused operator observables backing the parity-helper extension methods in <see cref="SignalAsyncExtensions"/>.</summary>
public static partial class SignalAsyncExtensions
{
    /// <summary>
    /// Fuses <c>Return(initial).Concat(source.Scan(initial, accumulator))</c> into a single layer.
    /// The seed is emitted on subscribe and tracked as the initial accumulator; each upstream
    /// emission updates the accumulator and forwards the new value.
    /// </summary>
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

            if (observer is WitnessAsync<TAccumulate> downstreamBase)
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
        /// <param name="seed">The seed accumulator value already emitted during subscription.</param>
        /// <param name="accumulator">The synchronous accumulator.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class ScanWithInitialWitness(
            IObserverAsync<TAccumulate> downstream,
            TAccumulate seed,
            Func<TAccumulate, TSource, TAccumulate> accumulator,
            CancellationToken subscribeToken) : WitnessAsync<TSource>(subscribeToken)
        {
            /// <summary>Running accumulator state; seeded with the initial value.</summary>
            private TAccumulate _accumulator = seed;

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(TSource value, CancellationToken cancellationToken)
            {
                _accumulator = accumulator(_accumulator, value);
                return downstream.OnNextAsync(_accumulator, cancellationToken);
            }

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
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

            if (observer is WitnessAsync<TAccumulate> downstreamBase)
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
        /// <param name="seed">The seed accumulator value already emitted during subscription.</param>
        /// <param name="accumulator">The asynchronous accumulator.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class ScanWithInitialAsyncWitness(
            IObserverAsync<TAccumulate> downstream,
            TAccumulate seed,
            Func<TAccumulate, TSource, CancellationToken, ValueTask<TAccumulate>> accumulator,
            CancellationToken subscribeToken) : WitnessAsync<TSource>(subscribeToken)
        {
            /// <summary>Running accumulator state; seeded with the initial value.</summary>
            private TAccumulate _accumulator = seed;

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(TSource value, CancellationToken cancellationToken)
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
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
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

    /// <summary>
    /// Fuses <c>DistinctUntilChanged().Throttle(window).DistinctUntilChanged()</c> into a single
    /// observer that tracks upstream-distinct, debounce-timer supersession, and downstream-distinct
    /// state. Supersession follows the same id-based pattern used by <c>ThrottleSignal</c>: a
    /// superseded delay still runs but its result is discarded.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="dueTime">The debounce window.</param>
    /// <param name="timeProvider">The time provider used for the debounce timer.</param>
    internal sealed class ThrottleDistinctSignal<T>(
        IObservableAsync<T> source,
        TimeSpan dueTime,
        TimeProvider timeProvider) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            ThrottleDistinctWitness sink = new(observer, dueTime, timeProvider, cancellationToken);

            if (observer is WitnessAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription witness fusing upstream-distinct + debounce + downstream-distinct.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="dueTime">The debounce window.</param>
        /// <param name="timeProvider">The time provider used for the debounce timer.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class ThrottleDistinctWitness(
            IObserverAsync<T> downstream,
            TimeSpan dueTime,
            TimeProvider timeProvider,
            CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
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

            /// <summary>Monotonically increasing identifier used to detect supersession.</summary>
            private long _id;

            /// <summary>Post-delay decision: latches the emission if the id is still current and
            /// the value differs from the most-recently-emitted one. Extracted as an
            /// <see langword="internal"/> method so the decision is unit-testable directly
            /// without racing the delay timer in tests.</summary>
            /// <param name="value">The candidate value.</param>
            /// <param name="id">The id stamped when this delay was started.</param>
            /// <returns><see langword="true"/> if the caller should forward the value
            /// downstream; <see langword="false"/> if the emission was superseded or is a
            /// duplicate of the most-recently-forwarded value.</returns>
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

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                long currentId;
                lock (_gate)
                {
                    if (_hasUpstream && Comparer.Equals(value, _lastUpstream))
                    {
                        return default;
                    }

                    _lastUpstream = value;
                    _hasUpstream = true;
                    currentId = ++_id;
                }

                _ = FireAfterDelayAsync(value, currentId, cancellationToken);
                return default;
            }

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
            {
                lock (_gate)
                {
                    _id++;
                }

                return downstream.OnErrorResumeAsync(error, cancellationToken);
            }

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result)
            {
                lock (_gate)
                {
                    _id++;
                }

                return downstream.OnCompletedAsync(result);
            }

            /// <inheritdoc/>
            protected override ValueTask DisposeAsyncCore()
            {
                lock (_gate)
                {
                    _id++;
                }

                return base.DisposeAsyncCore();
            }

            /// <summary>Waits the debounce window, then forwards the value if
            /// <see cref="TryClaimEmission"/> approves it. The single catch routes everything
            /// through <see cref="UnhandledExceptionHandler.ReportUnhandledException"/>, which
            /// already filters out <see cref="OperationCanceledException"/> internally —
            /// so a separate OCE-only catch would just duplicate the same silent-drop behavior.</summary>
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

    /// <summary>
    /// Fuses the <c>DropIfBusy</c> closure-based pipeline into a single observer layer.
    /// Synchronously-completing async actions and downstream emissions take a zero-state-machine
    /// fast path; only when the inner action genuinely suspends does the slow path run.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="asyncAction">The async side-effect invoked for accepted values.</param>
    internal sealed class DropIfBusySignal<T>(
        IObservableAsync<T> source,
        Func<T, CancellationToken, ValueTask> asyncAction) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            DropIfBusyWitness sink = new(observer, asyncAction, cancellationToken);

            if (observer is WitnessAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription witness that drops upstream emissions while a prior action is still pending.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="asyncAction">The async side-effect invoked for accepted values.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain.</param>
        internal sealed class DropIfBusyWitness(
            IObserverAsync<T> downstream,
            Func<T, CancellationToken, ValueTask> asyncAction,
            CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
        {
            /// <summary>0 when idle, 1 while an emission is being processed.</summary>
            private int _isBusy;

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
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
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
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

    /// <summary>
    /// Fuses <c>Select(condition ? Return(value) : Return(value).Delay(...)).Switch()</c> into a
    /// single observer layer. Bypass-true values flow through with zero allocation; bypass-false
    /// values schedule a fire-and-forget delay with id-based supersession (the same pattern
    /// <see cref="ThrottleDistinctSignal{T}"/> uses) so the previous pending delay is
    /// effectively cancelled on every new upstream value.
    /// </summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="debounce">The debounce window applied to bypass-false values.</param>
    /// <param name="condition">When <see langword="true"/> the value bypasses the delay and is forwarded immediately.</param>
    /// <param name="timeProvider">The time provider used for the debounce timer.</param>
    internal sealed class DebounceUntilSignal<T>(
        IObservableAsync<T> source,
        TimeSpan debounce,
        Func<T, bool> condition,
        TimeProvider timeProvider) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            DebounceUntilWitness sink = new(observer, debounce, condition, timeProvider, cancellationToken);

            if (observer is WitnessAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer that fuses the bypass-condition + Switch-debounce pipeline.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="debounce">The debounce window.</param>
        /// <param name="condition">The bypass-the-delay condition.</param>
        /// <param name="timeProvider">The time provider used for the debounce timer.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class DebounceUntilWitness(
            IObserverAsync<T> downstream,
            TimeSpan debounce,
            Func<T, bool> condition,
            TimeProvider timeProvider,
            CancellationToken subscribeToken) : WitnessAsync<T>(subscribeToken)
        {
            /// <summary>Synchronization gate protecting the id counter.</summary>
            private readonly Lock _gate = new();

            /// <summary>Monotonically increasing identifier used to detect supersession of pending delays.</summary>
            private long _id;

            /// <summary>Post-delay supersession check. Extracted as an <see langword="internal"/>
            /// method so tests can verify the supersession decision directly without racing the
            /// delay timer.</summary>
            /// <param name="id">The id stamped when this delay was started.</param>
            /// <returns><see langword="true"/> if the caller should forward the value
            /// downstream; <see langword="false"/> if the emission was superseded by a newer
            /// upstream value.</returns>
            internal bool IsCurrentEmission(long id)
            {
                lock (_gate)
                {
                    return _id == id;
                }
            }

            /// <inheritdoc/>
            protected override ValueTask OnNextAsyncCore(T value, CancellationToken cancellationToken)
            {
                if (condition(value))
                {
                    // Bypass path: cancel any pending delay and emit immediately.
                    lock (_gate)
                    {
                        _id++;
                    }

                    return downstream.OnNextAsync(value, cancellationToken);
                }

                long currentId;
                lock (_gate)
                {
                    currentId = ++_id;
                }

                _ = DelayAndEmitAsync(value, currentId, cancellationToken);
                return default;
            }

            /// <inheritdoc/>
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken)
            {
                lock (_gate)
                {
                    _id++;
                }

                return downstream.OnErrorResumeAsync(error, cancellationToken);
            }

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result)
            {
                lock (_gate)
                {
                    _id++;
                }

                return downstream.OnCompletedAsync(result);
            }

            /// <inheritdoc/>
            protected override ValueTask DisposeAsyncCore()
            {
                lock (_gate)
                {
                    _id++;
                }

                return base.DisposeAsyncCore();
            }

            /// <summary>Waits the debounce window, then forwards the value if
            /// <see cref="IsCurrentEmission"/> confirms the emission was not superseded.
            /// The single catch routes everything through
            /// <see cref="UnhandledExceptionHandler.ReportUnhandledException"/>, which already
            /// filters out <see cref="OperationCanceledException"/> internally.</summary>
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

    /// <summary>
    /// Fuses <c>source.SelectMany(values =&gt; values.ToAsyncSignal())</c> into a single
    /// observer that iterates the inner enumerable inline and forwards each element. Avoids the
    /// <c>SelectMany</c>+<c>ToAsyncSignal</c> per-emission machinery; arrays and
    /// <see cref="IReadOnlyList{T}"/> snapshots are walked with an indexed <c>for</c> loop to
    /// dodge the enumerator-box allocation entirely.
    /// </summary>
    /// <typeparam name="T">The flattened element type.</typeparam>
    /// <param name="source">The upstream observable of <see cref="IEnumerable{T}"/> snapshots.</param>
    internal sealed class ForEachEnumerableSignal<T>(IObservableAsync<IEnumerable<T>> source) : IObservableAsync<T>
    {
        /// <inheritdoc/>
        async ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            ForEachEnumerableWitness sink = new(observer, cancellationToken);

            if (observer is WitnessAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription witness that flattens each upstream enumerable inline.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class ForEachEnumerableWitness(
            IObserverAsync<T> downstream,
            CancellationToken subscribeToken) : WitnessAsync<IEnumerable<T>>(subscribeToken)
        {
            /// <inheritdoc/>
            protected override async ValueTask OnNextAsyncCore(
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
            protected override ValueTask OnErrorResumeAsyncCore(Exception error, CancellationToken cancellationToken) =>
                downstream.OnErrorResumeAsync(error, cancellationToken);

            /// <inheritdoc/>
            protected override ValueTask OnCompletedAsyncCore(Result result) =>
                downstream.OnCompletedAsync(result);
        }
    }
}
