// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals;

/// <summary>A signal that is both an observer and observable of values.</summary>
/// <typeparam name="T">The Type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public class Signal<T> : ISignal<T>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private const int InitialSubscriptionCapacity = 4;

    /// <summary>The factor the subscription array grows by when it fills.</summary>
    private const int SubscriptionGrowthFactor = 2;

    /// <summary>
    /// Guards observer-set and terminal-state mutations. Dispatch (OnNext) reads the observer
    /// slots lock-free via Volatile; only subscribe/remove/terminal take the lock, and they mutate
    /// reusable array slots in place rather than copying, so subscribe/unsubscribe churn does not
    /// allocate a new array per change.
    /// </summary>
    private readonly Lock _observerLock = new();

    /// <summary>Stores state for the signal implementation.</summary>
    private Exception? _exception;

    /// <summary>Stores state for the signal implementation.</summary>
    private SignalSubscription? _singleActionSubscription;

    /// <summary>Stores state for the signal implementation.</summary>
    private SignalSubscription? _singleObserverSubscription;

    /// <summary>Stores state for the signal implementation.</summary>
    private SignalSubscription?[]? _subscriptions;

    /// <summary>Stores state for the signal implementation.</summary>
    private int _subscriptionCount;

    /// <summary>Stores state for the signal implementation.</summary>
    private int _subscriptionTail;

    /// <summary>Stores state for the signal implementation.</summary>
    private bool _isDisposed;

    /// <summary>Stores state for the signal implementation.</summary>
    private bool _isStopped;

    /// <summary>Gets a value indicating whether indicates whether the subject has observers subscribed to it.</summary>
    public virtual bool HasObservers =>
        (_singleActionSubscription is not null || _singleObserverSubscription is not null || _subscriptionCount != 0)
        && !_isStopped;

    /// <summary>Gets a value indicating whether indicates whether the subject has been disposed.</summary>
    public virtual bool IsDisposed => _isDisposed;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>Called when [completed].</summary>
    public void OnCompleted()
    {
        SignalSubscription? singleObserver;
        SignalSubscription?[]? subscriptions;

        lock (_observerLock)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            singleObserver = _singleObserverSubscription;
            subscriptions = ClearObserversLocked();
            _isStopped = true;
        }

        Completed(singleObserver, subscriptions);
    }

    /// <summary>Called when [error].</summary>
    /// <param name="error">The error.</param>
    public void OnError(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

        SignalSubscription? singleObserver;
        SignalSubscription?[]? subscriptions;
        bool hasActionSubscribers;

        lock (_observerLock)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            _exception = error;
            hasActionSubscribers = _singleActionSubscription is not null || HasActionSubscribers(_subscriptions);
            singleObserver = _singleObserverSubscription;
            subscriptions = ClearObserversLocked();
            _isStopped = true;
        }

        Error(singleObserver, subscriptions, error);
        if (!hasActionSubscribers)
        {
            return;
        }

        ExceptionDispatchInfo.Capture(error).Throw();
    }

    /// <summary>Called when [next].</summary>
    /// <param name="value">The value.</param>
    public void OnNext(T value)
    {
        SignalSubscription? singleObserver;
        SignalSubscription? singleAction;
        SignalSubscription?[]? subscriptions;

        lock (_observerLock)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            singleObserver = _singleObserverSubscription;
            singleAction = _singleActionSubscription;
            subscriptions = _subscriptions;
        }

        if (singleObserver is not null)
        {
            singleObserver.Observer.OnNext(value);
            return;
        }

        if (singleAction is not null)
        {
            singleAction.Action(value);
            return;
        }

        DispatchSubscriptions(subscriptions, value);
        if (!Volatile.Read(ref _isDisposed))
        {
            return;
        }

        ThrowDisposed();
    }

    /// <summary>Subscribes the specified observer.</summary>
    /// <param name="observer">The observer.</param>
    /// <returns>
    /// A IDisposable.
    /// </returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        Exception? ex;
        bool stopped;
        SignalSubscription? subscription = null;

        lock (_observerLock)
        {
            ThrowIfDisposed();
            stopped = _isStopped;
            ex = _exception;
            if (!stopped)
            {
                if (_singleActionSubscription is null && _singleObserverSubscription is null && _subscriptionCount == 0)
                {
                    subscription = new(this, observer);
                    _singleObserverSubscription = subscription;
                }
                else
                {
                    PromoteSingleObserverLocked();
                    PromoteSingleActionObserverLocked();
                    subscription = new(this, observer);
                    AddSubscriptionLocked(subscription);
                }
            }
        }

        if (subscription is not null)
        {
            return subscription;
        }

        if (ex is not null)
        {
            observer.OnError(ex);
        }
        else
        {
            observer.OnCompleted();
        }

        return EmptyDisposable.Instance;
    }

    /// <summary>Executes the SubscribeAction operation.</summary>
    /// <param name="onNext">The onNext value.</param>
    /// <returns>The result.</returns>
    public IDisposable SubscribeAction(Action<T> onNext)
    {
        ArgumentExceptionHelper.ThrowIfNull(onNext);

        Exception? ex;
        bool stopped;
        SignalSubscription? subscription = null;

        lock (_observerLock)
        {
            ThrowIfDisposed();
            stopped = _isStopped;
            ex = _exception;
            if (!stopped)
            {
                subscription = new(this, onNext);
                if (_singleActionSubscription is null && _singleObserverSubscription is null && _subscriptionCount == 0)
                {
                    _singleActionSubscription = subscription;
                }
                else
                {
                    PromoteSingleObserverLocked();
                    PromoteSingleActionObserverLocked();
                    AddSubscriptionLocked(subscription);
                }
            }
        }

        if (subscription is not null)
        {
            return subscription;
        }

        if (ex is not null)
        {
            ExceptionDispatchInfo.Capture(ex).Throw();
        }

        return EmptyDisposable.Instance;
    }

    /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (IsDisposed)
        {
            return;
        }

        if (!disposing)
        {
            return;
        }

        SignalSubscription? singleActionSubscription;
        SignalSubscription? singleObserverSubscription;
        SignalSubscription?[]? subscriptions;

        lock (_observerLock)
        {
            singleActionSubscription = _singleActionSubscription;
            singleObserverSubscription = _singleObserverSubscription;
            subscriptions = ClearObserversLocked();
            _exception = null;
            _isDisposed = true;
        }

        singleActionSubscription?.Dispose();
        singleObserverSubscription?.Dispose();
        DisposeSubscriptions(subscriptions);
    }

    /// <summary>Executes the ThrowDisposed operation.</summary>
    private static void ThrowDisposed() => throw new ObjectDisposedException(string.Empty);

    /// <summary>Executes the Completed operation.</summary>
    /// <param name="singleObserver">The single observer fast-path subscription.</param>
    /// <param name="subscriptions">The subscriptions value.</param>
    private static void Completed(SignalSubscription? singleObserver, SignalSubscription?[]? subscriptions)
    {
        singleObserver?.OnCompleted();
        if (subscriptions is null)
        {
            return;
        }

        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i]?.OnCompleted();
        }
    }

    /// <summary>Executes the Error operation.</summary>
    /// <param name="singleObserver">The single observer fast-path subscription.</param>
    /// <param name="subscriptions">The subscriptions value.</param>
    /// <param name="exception">The exception value.</param>
    private static void Error(
        SignalSubscription? singleObserver,
        SignalSubscription?[]? subscriptions,
        Exception exception)
    {
        singleObserver?.OnError(exception);
        if (subscriptions is null)
        {
            return;
        }

        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i]?.OnError(exception);
        }
    }

    /// <summary>Executes the HasActionSubscribers operation.</summary>
    /// <param name="subscriptions">The subscriptions value.</param>
    /// <returns>The result.</returns>
    private static bool HasActionSubscribers(SignalSubscription?[]? subscriptions)
    {
        if (subscriptions is null)
        {
            return false;
        }

        for (var i = 0; i < subscriptions.Length; i++)
        {
            if (subscriptions[i]?.IsAction == true)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>Executes the DisposeSubscriptions operation.</summary>
    /// <param name="subscriptions">The subscriptions value.</param>
    private static void DisposeSubscriptions(SignalSubscription?[]? subscriptions)
    {
        if (subscriptions is null)
        {
            return;
        }

        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i]?.Dispose();
        }
    }

    /// <summary>Executes the DispatchSubscriptions operation.</summary>
    /// <param name="subscriptions">The subscription snapshot.</param>
    /// <param name="value">The value.</param>
    private static void DispatchSubscriptions(SignalSubscription?[]? subscriptions, T value)
    {
        if (subscriptions is null)
        {
            return;
        }

        for (var i = 0; i < subscriptions.Length; i++)
        {
            var subscription = Volatile.Read(ref subscriptions[i]);
            if (subscription is null)
            {
                continue;
            }

            subscription.OnNext(value);
        }
    }

    /// <summary>Executes the ThrowIfDisposed operation.</summary>
    private void ThrowIfDisposed()
    {
        if (!IsDisposed)
        {
            return;
        }

        ThrowDisposed();
    }

    /// <summary>Executes the AddSubscriptionLocked operation.</summary>
    /// <param name="subscription">The subscription value.</param>
    private void AddSubscriptionLocked(SignalSubscription subscription)
    {
        var subscriptions = _subscriptions;
        if (subscriptions is null)
        {
            subscriptions = new SignalSubscription[InitialSubscriptionCapacity];
            Volatile.Write(ref _subscriptions, subscriptions);
        }

        for (var i = 0; i < _subscriptionTail; i++)
        {
            if (subscriptions[i] is not null)
            {
                continue;
            }

            Volatile.Write(ref subscriptions[i], subscription);
            _subscriptionCount++;
            return;
        }

        if (_subscriptionTail == subscriptions.Length)
        {
            var copy = new SignalSubscription[subscriptions.Length * SubscriptionGrowthFactor];
            Array.Copy(subscriptions, copy, subscriptions.Length);
            subscriptions = copy;
            Volatile.Write(ref _subscriptions, subscriptions);
        }

        Volatile.Write(ref subscriptions[_subscriptionTail], subscription);
        _subscriptionTail++;
        _subscriptionCount++;
    }

    /// <summary>Executes the ClearObserversLocked operation.</summary>
    /// <returns>The result.</returns>
    private SignalSubscription?[]? ClearObserversLocked()
    {
        _singleActionSubscription = null;
        _singleObserverSubscription = null;
        var subscriptions = _subscriptions;
        Volatile.Write(ref _subscriptions, null);
        _subscriptionCount = 0;
        _subscriptionTail = 0;
        return subscriptions;
    }

    /// <summary>Executes the PromoteSingleActionObserverLocked operation.</summary>
    private void PromoteSingleActionObserverLocked()
    {
        var single = _singleActionSubscription;
        if (single is null)
        {
            return;
        }

        _singleActionSubscription = null;
        AddSubscriptionLocked(single);
    }

    /// <summary>Executes the PromoteSingleObserverLocked operation.</summary>
    private void PromoteSingleObserverLocked()
    {
        var single = _singleObserverSubscription;
        if (single is null)
        {
            return;
        }

        _singleObserverSubscription = null;
        AddSubscriptionLocked(single);
    }

    /// <summary>Executes the Remove operation.</summary>
    /// <param name="subscription">The subscription value.</param>
    private void Remove(SignalSubscription subscription)
    {
        lock (_observerLock)
        {
            if (RemoveSingleSubscriptionLocked(subscription))
            {
                return;
            }

            RemoveArraySubscriptionLocked(subscription);
        }
    }

    /// <summary>Removes a single-subscription fast path entry.</summary>
    /// <param name="subscription">The subscription value.</param>
    /// <returns><c>true</c> when a single subscription was removed; otherwise, <c>false</c>.</returns>
    private bool RemoveSingleSubscriptionLocked(SignalSubscription subscription)
    {
        if (ReferenceEquals(_singleActionSubscription, subscription))
        {
            _singleActionSubscription = null;
            return true;
        }

        if (!ReferenceEquals(_singleObserverSubscription, subscription))
        {
            return false;
        }

        _singleObserverSubscription = null;
        return true;
    }

    /// <summary>Removes an array-backed subscription.</summary>
    /// <param name="subscription">The subscription value.</param>
    private void RemoveArraySubscriptionLocked(SignalSubscription subscription)
    {
        var subscriptions = _subscriptions;
        if (subscriptions is null)
        {
            return;
        }

        for (var i = 0; i < subscriptions.Length; i++)
        {
            if (!ReferenceEquals(subscriptions[i], subscription))
            {
                continue;
            }

            Volatile.Write(ref subscriptions[i], null);
            _subscriptionCount--;
            if (_subscriptionCount != 0)
            {
                return;
            }

            _subscriptionTail = 0;
            return;
        }
    }

    /// <summary>Represents the SignalSubscription class.</summary>
    private sealed class SignalSubscription : IDisposable
    {
        /// <summary>The observer target, or <see langword="null"/> when this subscription stores an action callback.</summary>
        private readonly IObserver<T>? _observer;

        /// <summary>The action target, or <see langword="null"/> when this subscription stores an observer.</summary>
        private readonly Action<T>? _action;

        /// <summary>Stores state for the signal implementation.</summary>
        private Signal<T>? _subject;

        /// <summary>Initializes a new instance of the <see cref="SignalSubscription"/> class.</summary>
        /// <param name="subject">The subject value.</param>
        /// <param name="observer">The observer value.</param>
        public SignalSubscription(Signal<T> subject, IObserver<T> observer)
        {
            _subject = subject;
            _observer = observer;
        }

        /// <summary>Initializes a new instance of the <see cref="SignalSubscription"/> class.</summary>
        /// <param name="subject">The subject value.</param>
        /// <param name="onNext">The onNext value.</param>
        public SignalSubscription(Signal<T> subject, Action<T> onNext)
        {
            _subject = subject;
            _action = onNext;
        }

        /// <summary>Gets a value indicating whether this subscription stores an action callback.</summary>
        public bool IsAction => _action is not null;

        /// <summary>Gets the observer target.</summary>
        public IObserver<T> Observer => _observer!;

        /// <summary>Gets the action target.</summary>
        public Action<T> Action => _action!;

        /// <summary>Sends a value to the subscription target.</summary>
        /// <param name="value">The value.</param>
        public void OnNext(T value)
        {
            // Branch on a null check of the typed fields rather than an `is Action<T>` test plus cast: this runs
            // once per observer per value on the multicast dispatch hot path, where that overhead is measurable.
            var observer = _observer;
            if (observer is not null)
            {
                observer.OnNext(value);
                return;
            }

            _action!(value);
        }

        /// <summary>Sends an error to observer subscriptions.</summary>
        /// <param name="exception">The exception.</param>
        public void OnError(Exception exception) => _observer?.OnError(exception);

        /// <summary>Sends completion to observer subscriptions.</summary>
        public void OnCompleted() => _observer?.OnCompleted();

        /// <summary>Executes the Dispose operation.</summary>
        public void Dispose()
        {
            var subject = Interlocked.Exchange(ref _subject, null);
            subject?.Remove(this);
        }
    }
}
