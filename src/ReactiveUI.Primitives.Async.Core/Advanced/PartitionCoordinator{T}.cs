// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>Shares an upstream subscription between predicate-selected branches until the last subscriber disposes.</summary>
/// <typeparam name="T">The element type partitioned across the two branches.</typeparam>
[System.Diagnostics.DebuggerDisplay("PartitionCoordinator: Gate = {_gate}")]
public sealed class PartitionCoordinator<T>
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
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(predicate);
        _source = source;
        _predicate = predicate;
        TrueBranch = new PartitionBranchSignal<T>(true) { Coordinator = this };
        FalseBranch = new PartitionBranchSignal<T>(false) { Coordinator = this };
    }

    /// <summary>Gets the truthy-side observable; values for which the predicate returns <see langword="true"/>.</summary>
    internal IObservableAsync<T> TrueBranch { get; }

    /// <summary>Gets the falsy-side observable; values for which the predicate returns <see langword="false"/>.</summary>
    internal IObservableAsync<T> FalseBranch { get; }

    /// <summary>Creates a shared <see cref="PartitionCoordinator{T}"/> and returns its truthy and falsy branch observables.</summary>
    /// <param name="source">The upstream observable.</param>
    /// <param name="predicate">The partition predicate.</param>
    /// <returns>A tuple of truthy and falsy branch sequences.</returns>
    public static (IObservableAsync<T> True, IObservableAsync<T> False) CreateBranches(
        IObservableAsync<T> source,
        Func<T, bool> predicate)
    {
        PartitionCoordinator<T> coordinator = new(source, predicate);
        return (coordinator.TrueBranch, coordinator.FalseBranch);
    }

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

        return new PartitionBranchSubscription<T>(this, isTrueBranch);
    }

    /// <summary>Attaches an in-flight upstream subscription unless both branches have gone away first.</summary>
    /// <param name="subscription">The freshly-created upstream subscription.</param>
    /// <returns><see langword="true"/> when the subscription was attached and the caller should leave it running;
    /// <see langword="false"/> when both branches are gone and the caller should dispose it.</returns>
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

    /// <summary>Attaches the new upstream subscription, disposing it when both branches disposed during the subscribe.</summary>
    /// <param name="subscription">The freshly-created upstream subscription.</param>
    /// <returns>A task that completes once the subscription has been attached or disposed.</returns>
    internal async ValueTask AttachOrDisposeStaleSubscriptionAsync(IAsyncDisposable subscription)
    {
        if (!TryAttachSourceSubscription(subscription))
        {
            await subscription.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Releases a branch's slot and tears the upstream subscription down when both branches are gone.</summary>
    /// <param name="isTrueBranch"><see langword="true"/> for the truthy branch.</param>
    /// <returns>A task that completes when teardown is done.</returns>
    internal async ValueTask ReleaseBranchAsync(bool isTrueBranch)
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
}
