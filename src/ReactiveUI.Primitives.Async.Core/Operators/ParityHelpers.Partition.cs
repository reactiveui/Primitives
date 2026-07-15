// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async;

/// <summary>
/// The shared upstream coordinator backing the <c>Partition</c> parity helper. It lives apart from the
/// other fused operators because it is the only one that fans a single subscription out to two
/// observables, so its state is a branch table rather than a per-subscription witness.
/// </summary>
public static partial class SignalAsyncExtensions
{
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
        /// <summary>Synchronization gate protecting branch slots and the source-subscription lifecycle.</summary>
        private readonly Lock _gate = new();

        /// <summary>The upstream observable shared across both branches.</summary>
        private readonly IObservableAsync<T> _source;

        /// <summary>The partition predicate, evaluated exactly once per upstream emission.</summary>
        private readonly Func<T, bool> _predicate;

        /// <summary>The active observer for the truthy branch, or <see langword="null"/> when nobody is subscribed.</summary>
        private IObserverAsync<T>? _trueObserver;

        /// <summary>The active observer for the falsy branch, or <see langword="null"/> when nobody is subscribed.</summary>
        private IObserverAsync<T>? _falseObserver;

        /// <summary>Active upstream subscription while at least one branch is alive.</summary>
        private IAsyncDisposable? _sourceSubscription;

        /// <summary>Cached terminal result so a late-arriving branch can be notified immediately.</summary>
        private Result? _terminalResult;

        /// <summary>Initializes a new instance of the <see cref="PartitionCoordinator{T}"/> class.</summary>
        /// <param name="source">The upstream observable.</param>
        /// <param name="predicate">The partition predicate.</param>
        public PartitionCoordinator(IObservableAsync<T> source, Func<T, bool> predicate)
        {
            _source = source;
            _predicate = predicate;
            TrueBranch = new PartitionBranchSignal(true) { Coordinator = this };
            FalseBranch = new PartitionBranchSignal(false) { Coordinator = this };
        }

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
        internal sealed class PartitionBranchSignal(bool isTrueBranch) : IObservableAsync<T>
        {
            /// <summary>Gets or sets the back-pointer to the owning coordinator.</summary>
            internal PartitionCoordinator<T> Coordinator { get; set; } = null!;

            /// <inheritdoc/>
            ValueTask<IAsyncDisposable> IObservableAsync<T>.SubscribeAsync(
                IObserverAsync<T> observer,
                CancellationToken cancellationToken) =>

                // The PartitionBranchSignal is created by the coordinator's constructor; the
                // coordinator field is filled in below.
                Coordinator.SubscribeBranchAsync(isTrueBranch, observer, cancellationToken);
        }

        /// <summary>Branch subscription handle returned to the subscriber.</summary>
        /// <param name="coordinator">The owning coordinator.</param>
        /// <param name="isTrueBranch"><see langword="true"/> for the truthy branch.</param>
        internal sealed class BranchSubscription(PartitionCoordinator<T> coordinator, bool isTrueBranch)
            : IAsyncDisposable
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
}
