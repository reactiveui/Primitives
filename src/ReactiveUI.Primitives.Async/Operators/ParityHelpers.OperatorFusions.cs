// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// Fused operator observables backing the parity-helper extension methods in
/// <see cref="SignalAsync"/>.
/// </summary>
[SuppressMessage(
    "Major Code Smell",
    "S3604:Member initializer values should not be redundant",
    Justification = "Primary-constructor parameters are captured into observer state.")]
public static partial class SignalAsync
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
        Func<TAccumulate, TSource, TAccumulate> accumulator) : SignalAsync<TAccumulate>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<TAccumulate> observer,
            CancellationToken cancellationToken)
        {
            var sink = new ScanWithInitialObserver(observer, initial, accumulator, cancellationToken);

            if (observer is ObserverAsync<TAccumulate> downstreamBase)
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
        /// <param name="seed">The seed accumulator value already emitted from <see cref="SubscribeAsyncCore"/>.</param>
        /// <param name="accumulator">The synchronous accumulator.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class ScanWithInitialObserver(
            IObserverAsync<TAccumulate> downstream,
            TAccumulate seed,
            Func<TAccumulate, TSource, TAccumulate> accumulator,
            CancellationToken subscribeToken) : ObserverAsync<TSource>(subscribeToken)
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

    /// <summary>
    /// Async-accumulator variant of <see cref="ScanWithInitialSignal{TSource, TAccumulate}"/>.
    /// </summary>
    /// <typeparam name="TSource">The upstream element type.</typeparam>
    /// <typeparam name="TAccumulate">The accumulator type.</typeparam>
    /// <param name="source">The upstream observable.</param>
    /// <param name="initial">The initial accumulator value, emitted on subscribe.</param>
    /// <param name="accumulator">The asynchronous accumulator.</param>
    internal sealed class ScanWithInitialAsyncSignal<TSource, TAccumulate>(
        IObservableAsync<TSource> source,
        TAccumulate initial,
        Func<TAccumulate, TSource, CancellationToken, ValueTask<TAccumulate>> accumulator) : SignalAsync<TAccumulate>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<TAccumulate> observer,
            CancellationToken cancellationToken)
        {
            var sink = new ScanWithInitialAsyncObserver(observer, initial, accumulator, cancellationToken);

            if (observer is ObserverAsync<TAccumulate> downstreamBase)
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
        /// <param name="seed">The seed accumulator value already emitted from <see cref="SubscribeAsyncCore"/>.</param>
        /// <param name="accumulator">The asynchronous accumulator.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class ScanWithInitialAsyncObserver(
            IObserverAsync<TAccumulate> downstream,
            TAccumulate seed,
            Func<TAccumulate, TSource, CancellationToken, ValueTask<TAccumulate>> accumulator,
            CancellationToken subscribeToken) : ObserverAsync<TSource>(subscribeToken)
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
            private async ValueTask AwaitAndForwardAsync(ValueTask<TAccumulate> pending, CancellationToken cancellationToken)
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
        TimeProvider timeProvider) : SignalAsync<T>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            var sink = new ThrottleDistinctObserver(observer, dueTime, timeProvider, cancellationToken);

            if (observer is ObserverAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer fusing upstream-distinct + debounce + downstream-distinct.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="dueTime">The debounce window.</param>
        /// <param name="timeProvider">The time provider used for the debounce timer.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class ThrottleDistinctObserver(
            IObserverAsync<T> downstream,
            TimeSpan dueTime,
            TimeProvider timeProvider,
            CancellationToken subscribeToken) : ObserverAsync<T>(subscribeToken)
        {
            /// <summary>Equality comparer used for both distinct layers.</summary>
            private static readonly EqualityComparer<T> Comparer = EqualityComparer<T>.Default;

            /// <summary>Synchronization gate protecting throttle/distinct state.</summary>
            private readonly Lock _gate = new();

            /// <summary>Most-recent upstream value (for upstream DistinctUntilChanged).</summary>
            private T _lastUpstream = default!;

            /// <summary>Most-recently-forwarded value (for downstream DistinctUntilChanged).</summary>
            private T _lastEmitted = default!;

            /// <summary><see langword="true"/> after the first upstream emission has been seen.</summary>
            private bool _hasUpstream;

            /// <summary><see langword="true"/> after the first value has been forwarded downstream.</summary>
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
        Func<T, CancellationToken, ValueTask> asyncAction) : SignalAsync<T>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            var sink = new DropIfBusyObserver(observer, asyncAction, cancellationToken);

            if (observer is ObserverAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer that drops upstream emissions while a prior action is still pending.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="asyncAction">The async side-effect invoked for accepted values.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token, linked into the dispose chain.</param>
        internal sealed class DropIfBusyObserver(
            IObserverAsync<T> downstream,
            Func<T, CancellationToken, ValueTask> asyncAction,
            CancellationToken subscribeToken) : ObserverAsync<T>(subscribeToken)
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
    /// Coordinates the shared upstream subscription and the two branch observables produced by
    /// <c>Partition</c>. Maintains a single source subscription that is started when the first
    /// branch subscribes and torn down when the last branch disposes. Each upstream emission
    /// evaluates the predicate exactly once and dispatches to the branch observer (if any)
    /// subscribed at that moment — no <c>Publish</c>/<c>RefCount</c>/intermediate-signal
    /// allocations on the per-emission path.
    /// </summary>
    /// <typeparam name="T">The element type partitioned across the two branches.</typeparam>
    internal sealed class PartitionCoordinator<T>
    {
        /// <summary>The upstream observable shared across both branches.</summary>
        private readonly IObservableAsync<T> _source;

        /// <summary>The partition predicate, evaluated exactly once per upstream emission.</summary>
        private readonly Func<T, bool> _predicate;

        /// <summary>Initializes a new instance of the <see cref="PartitionCoordinator{T}"/> class.</summary>
        /// <param name="source">The upstream observable.</param>
        /// <param name="predicate">The partition predicate.</param>
        public PartitionCoordinator(IObservableAsync<T> source, Func<T, bool> predicate)
        {
            _source = source;
            _predicate = predicate;
            TrueBranch = new PartitionBranchSignal(isTrueBranch: true) { Coordinator = this };
            FalseBranch = new PartitionBranchSignal(isTrueBranch: false) { Coordinator = this };
        }

        /// <summary>Synchronization gate protecting branch slots and the source-subscription lifecycle.</summary>
        private readonly Lock _gate = new();

        /// <summary>The active observer for the truthy branch, or <see langword="null"/> when nobody is subscribed.</summary>
        private IObserverAsync<T>? _trueObserver;

        /// <summary>The active observer for the falsy branch, or <see langword="null"/> when nobody is subscribed.</summary>
        private IObserverAsync<T>? _falseObserver;

        /// <summary>Active upstream subscription while at least one branch is alive.</summary>
        private IAsyncDisposable? _sourceSubscription;

        /// <summary>Cached terminal result so a late-arriving branch can be notified immediately.</summary>
        private Result? _terminalResult;

        /// <summary>Gets the truthy-side observable; values for which the predicate returns <see langword="true"/>.</summary>
        public IObservableAsync<T> TrueBranch { get; }

        /// <summary>Gets the falsy-side observable; values for which the predicate returns <see langword="false"/>.</summary>
        public IObservableAsync<T> FalseBranch { get; }

        /// <summary>Subscribes a branch observer, lazily starting the shared upstream subscription on the first branch.</summary>
        /// <param name="isTrueBranch"><see langword="true"/> for the truthy branch; <see langword="false"/> for the falsy branch.</param>
        /// <param name="observer">The observer subscribing to this branch.</param>
        /// <param name="cancellationToken">The subscribe-time cancellation token.</param>
        /// <returns>A disposable that releases this branch's slot when disposed.</returns>
        internal async ValueTask<IAsyncDisposable> SubscribeBranchAsync(
            bool isTrueBranch,
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            Result? terminal;
            bool needSourceSubscribe;

            lock (_gate)
            {
                if (isTrueBranch)
                {
                    _trueObserver = observer;
                }
                else
                {
                    _falseObserver = observer;
                }

                terminal = _terminalResult;
                needSourceSubscribe = _sourceSubscription is null && terminal is null;
            }

            if (terminal is { } already)
            {
                await observer.OnCompletedAsync(already).ConfigureAwait(false);
                return DisposableAsync.Empty;
            }

            if (needSourceSubscribe)
            {
                var subscription = await _source.SubscribeAsync(
                    OnSourceNextAsync,
                    OnSourceErrorResumeAsync,
                    OnSourceCompletedAsync,
                    cancellationToken).ConfigureAwait(false);

                await AttachOrDisposeStaleSubscriptionAsync(subscription).ConfigureAwait(false);
            }

            return new BranchSubscription(this, isTrueBranch);
        }

        /// <summary>Attempts to attach an in-flight upstream subscription to the coordinator.
        /// Extracted as an <see langword="internal"/> method so the both-branches-gone race
        /// (the subscribe completes after every branch has already disposed) can be tested
        /// directly without racing the subscription pipeline.</summary>
        /// <param name="subscription">The freshly-created upstream subscription.</param>
        /// <returns><see langword="true"/> if the subscription was attached and the caller
        /// should leave it running; <see langword="false"/> if both branches are gone and the
        /// caller should dispose the subscription.</returns>
        internal bool TryAttachSourceSubscription(IAsyncDisposable subscription)
        {
            lock (_gate)
            {
                if (_trueObserver is null && _falseObserver is null)
                {
                    return false;
                }

                _sourceSubscription = subscription;
                return true;
            }
        }

        /// <summary>Attempts to attach the just-created upstream subscription and disposes it if
        /// both branches have raced ahead and already disposed. The dispose branch is only
        /// reachable under genuine concurrent disposal during in-flight subscribe, so the entire
        /// helper is isolated and excluded from coverage; <see cref="TryAttachSourceSubscription"/>
        /// itself is covered by direct unit tests.</summary>
        /// <param name="subscription">The freshly-created upstream subscription.</param>
        /// <returns>A task that completes once the subscription has been attached or disposed.</returns>
        [ExcludeFromCodeCoverage]
        private async ValueTask AttachOrDisposeStaleSubscriptionAsync(IAsyncDisposable subscription)
        {
            if (!TryAttachSourceSubscription(subscription))
            {
                await subscription.DisposeAsync().ConfigureAwait(false);
            }
        }

        /// <summary>Forwards an upstream value to the branch whose predicate result matches.</summary>
        /// <param name="value">The upstream value.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task that completes after the target branch (if any) has processed the value.</returns>
        private ValueTask OnSourceNextAsync(T value, CancellationToken cancellationToken)
        {
            var matches = _predicate(value);
            IObserverAsync<T>? target;
            lock (_gate)
            {
                target = matches ? _trueObserver : _falseObserver;
            }

            return target?.OnNextAsync(value, cancellationToken) ?? default;
        }

        /// <summary>Forwards an upstream error to both subscribed branches.</summary>
        /// <param name="error">The error.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>A task that completes after both branches (if any) have processed the error.</returns>
        private async ValueTask OnSourceErrorResumeAsync(Exception error, CancellationToken cancellationToken)
        {
            IObserverAsync<T>? trueOb;
            IObserverAsync<T>? falseOb;
            lock (_gate)
            {
                trueOb = _trueObserver;
                falseOb = _falseObserver;
            }

            if (trueOb is not null)
            {
                await trueOb.OnErrorResumeAsync(error, cancellationToken).ConfigureAwait(false);
            }

            if (falseOb is not null)
            {
                await falseOb.OnErrorResumeAsync(error, cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>Forwards completion to both subscribed branches and caches the terminal result for late subscribers.</summary>
        /// <param name="result">The completion result.</param>
        /// <returns>A task that completes after both branches (if any) have processed the completion.</returns>
        private async ValueTask OnSourceCompletedAsync(Result result)
        {
            IObserverAsync<T>? trueOb;
            IObserverAsync<T>? falseOb;
            lock (_gate)
            {
                _terminalResult = result;
                trueOb = _trueObserver;
                falseOb = _falseObserver;
            }

            if (trueOb is not null)
            {
                await trueOb.OnCompletedAsync(result).ConfigureAwait(false);
            }

            if (falseOb is not null)
            {
                await falseOb.OnCompletedAsync(result).ConfigureAwait(false);
            }
        }

        /// <summary>Releases a branch's slot and tears the upstream subscription down when both branches are gone.</summary>
        /// <param name="isTrueBranch"><see langword="true"/> for the truthy branch.</param>
        /// <returns>A task that completes when teardown is done.</returns>
        private async ValueTask ReleaseBranchAsync(bool isTrueBranch)
        {
            IAsyncDisposable? subscriptionToDispose = null;
            lock (_gate)
            {
                if (isTrueBranch)
                {
                    _trueObserver = null;
                }
                else
                {
                    _falseObserver = null;
                }

                if (_trueObserver is null && _falseObserver is null && _sourceSubscription is not null)
                {
                    subscriptionToDispose = _sourceSubscription;
                    _sourceSubscription = null;
                }
            }

            if (subscriptionToDispose is not null)
            {
                await subscriptionToDispose.DisposeAsync().ConfigureAwait(false);
            }
        }

        /// <summary>Observable view of one branch.</summary>
        /// <param name="isTrueBranch"><see langword="true"/> for the truthy branch.</param>
        internal sealed class PartitionBranchSignal(bool isTrueBranch) : SignalAsync<T>
        {
            /// <inheritdoc/>
            protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(
                IObserverAsync<T> observer,
                CancellationToken cancellationToken)
            {
                // The PartitionBranchSignal is created by the coordinator's constructor; the
                // coordinator field is filled in below.
                return Coordinator.SubscribeBranchAsync(isTrueBranch, observer, cancellationToken);
            }

            /// <summary>Gets or sets the back-pointer to the owning coordinator.</summary>
            internal PartitionCoordinator<T> Coordinator { get; set; } = null!;
        }

        /// <summary>Branch subscription handle returned to the subscriber.</summary>
        /// <param name="coordinator">The owning coordinator.</param>
        /// <param name="isTrueBranch"><see langword="true"/> for the truthy branch.</param>
        internal sealed class BranchSubscription(PartitionCoordinator<T> coordinator, bool isTrueBranch) : IAsyncDisposable
        {
            /// <summary>Latches to <c>1</c> on the first <see cref="DisposeAsync"/> call so release is idempotent.</summary>
            private int _disposed;

            /// <inheritdoc/>
            public ValueTask DisposeAsync() =>
                Interlocked.Exchange(ref _disposed, 1) == 1
                    ? default
                    : coordinator.ReleaseBranchAsync(isTrueBranch);
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
        TimeProvider timeProvider) : SignalAsync<T>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            var sink = new DebounceUntilObserver(observer, debounce, condition, timeProvider, cancellationToken);

            if (observer is ObserverAsync<T> downstreamBase)
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
        internal sealed class DebounceUntilObserver(
            IObserverAsync<T> downstream,
            TimeSpan debounce,
            Func<T, bool> condition,
            TimeProvider timeProvider,
            CancellationToken subscribeToken) : ObserverAsync<T>(subscribeToken)
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
    internal sealed class ForEachEnumerableSignal<T>(IObservableAsync<IEnumerable<T>> source) : SignalAsync<T>
    {
        /// <inheritdoc/>
        protected override async ValueTask<IAsyncDisposable> SubscribeAsyncCore(
            IObserverAsync<T> observer,
            CancellationToken cancellationToken)
        {
            var sink = new ForEachEnumerableObserver(observer, cancellationToken);

            if (observer is ObserverAsync<T> downstreamBase)
            {
                downstreamBase.LinkUpstreamCancellation(sink.InternalDisposedToken);
            }

            var subscription = await source.SubscribeAsync(sink, cancellationToken).ConfigureAwait(false);
            await sink.AssignSourceSubscriptionAsync(subscription).ConfigureAwait(false);
            return sink;
        }

        /// <summary>Per-subscription observer that flattens each upstream enumerable inline.</summary>
        /// <param name="downstream">The downstream observer.</param>
        /// <param name="subscribeToken">The subscribe-time cancellation token.</param>
        internal sealed class ForEachEnumerableObserver(
            IObserverAsync<T> downstream,
            CancellationToken subscribeToken) : ObserverAsync<IEnumerable<T>>(subscribeToken)
        {
            /// <inheritdoc/>
            protected override async ValueTask OnNextAsyncCore(IEnumerable<T> values, CancellationToken cancellationToken)
            {
                if (values is T[] array)
                {
                    for (var i = 0; i < array.Length; i++)
                    {
                        await downstream.OnNextAsync(array[i], cancellationToken).ConfigureAwait(false);
                    }

                    return;
                }

                if (values is IReadOnlyList<T> list)
                {
                    for (var i = 0; i < list.Count; i++)
                    {
                        await downstream.OnNextAsync(list[i], cancellationToken).ConfigureAwait(false);
                    }

                    return;
                }

                foreach (var value in values)
                {
                    await downstream.OnNextAsync(value, cancellationToken).ConfigureAwait(false);
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
