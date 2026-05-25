// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Subject.
/// </summary>
/// <typeparam name="T">The Type.</typeparam>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
public class Signal<T> : ISignal<T>
{
    private static readonly Action<T> NoopOnNext = static _ => { };
    private static readonly Action<T> ThrowDisposedOnNext = static _ => ThrowDisposed();

    private readonly object _observerLock = new();
    private Exception? _exception;
    private SignalSubscription? _singleActionSubscription;
    private SignalSubscription?[]? _subscriptions;
    private int _subscriptionCount;
    private int _subscriptionTail;
    private Action<T> _onNext = NoopOnNext;
    private bool _isDisposed;
    private bool _isStopped;

    /// <summary>
    /// Gets a value indicating whether indicates whether the subject has observers subscribed to it.
    /// </summary>
    public virtual bool HasObservers => (_singleActionSubscription != null || _subscriptionCount != 0) && !_isStopped;

    /// <summary>
    /// Gets a value indicating whether indicates whether the subject has been disposed.
    /// </summary>
    public virtual bool IsDisposed => _isDisposed;

    /// <summary>
    /// Releases unmanaged and - optionally - managed resources.
    /// </summary>
    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Called when [completed].
    /// </summary>
    public void OnCompleted()
    {
        SignalSubscription?[]? subscriptions;

        lock (_observerLock)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            subscriptions = ClearObserversLocked();
            _isStopped = true;
        }

        Completed(subscriptions);
    }

    /// <summary>
    /// Called when [error].
    /// </summary>
    /// <param name="error">The error.</param>
    public void OnError(Exception error)
    {
        if (error == null)
        {
            throw new ArgumentNullException(nameof(error));
        }

        SignalSubscription?[]? subscriptions;
        var hasActionSubscribers = false;

        lock (_observerLock)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            _exception = error;
            hasActionSubscribers = _singleActionSubscription != null || HasActionSubscribers(_subscriptions);
            subscriptions = ClearObserversLocked();
            _isStopped = true;
        }

        Error(subscriptions, error);
        if (hasActionSubscribers)
        {
            ExceptionDispatchInfo.Capture(error).Throw();
        }
    }

    /// <summary>
    /// Called when [next].
    /// </summary>
    /// <param name="value">The value.</param>
    public void OnNext(T value) => _onNext(value);

    /// <summary>
    /// Subscribes the specified observer.
    /// </summary>
    /// <param name="observer">The observer.</param>
    /// <returns>
    /// A IDisposable.
    /// </returns>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

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
                PromoteSingleActionObserverLocked();
                subscription = new SignalSubscription(this, observer);
                AddSubscriptionLocked(subscription);
                _onNext = DispatchSubscriptions;
            }
        }

        if (subscription != null)
        {
            return subscription;
        }

        if (ex != null)
        {
            observer.OnError(ex);
        }
        else
        {
            observer.OnCompleted();
        }

        return Disposable.Empty;
    }

    internal IDisposable SubscribeAction(Action<T> onNext)
    {
        if (onNext == null)
        {
            throw new ArgumentNullException(nameof(onNext));
        }

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
                subscription = new SignalSubscription(this, onNext);
                if (_singleActionSubscription == null && _subscriptionCount == 0)
                {
                    _singleActionSubscription = subscription;
                    _onNext = onNext;
                }
                else
                {
                    PromoteSingleActionObserverLocked();
                    AddSubscriptionLocked(subscription);
                    _onNext = DispatchSubscriptions;
                }
            }
        }

        if (subscription != null)
        {
            return subscription;
        }

        if (ex != null)
        {
            ExceptionDispatchInfo.Capture(ex).Throw();
        }

        return Disposable.Empty;
    }

    /// <summary>
    /// Releases unmanaged and - optionally - managed resources.
    /// </summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (!IsDisposed)
        {
            if (disposing)
            {
                lock (_observerLock)
                {
                    ClearObserversLocked();
                    _exception = null;
                    _onNext = ThrowDisposedOnNext;
                    _isDisposed = true;
                }
            }
        }
    }

    private static void ThrowDisposed() => throw new ObjectDisposedException(string.Empty);

    private static void Completed(SignalSubscription?[]? subscriptions)
    {
        if (subscriptions == null)
        {
            return;
        }

        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i]?.Observer?.OnCompleted();
        }
    }

    private static void Error(SignalSubscription?[]? subscriptions, Exception error)
    {
        if (subscriptions == null)
        {
            return;
        }

        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i]?.Observer?.OnError(error);
        }
    }

    private static bool HasActionSubscribers(SignalSubscription?[]? subscriptions)
    {
        if (subscriptions == null)
        {
            return false;
        }

        for (var i = 0; i < subscriptions.Length; i++)
        {
            if (subscriptions[i]?.OnNext != null)
            {
                return true;
            }
        }

        return false;
    }

    private void ThrowIfDisposed()
    {
        if (IsDisposed)
        {
            ThrowDisposed();
        }
    }

    private void AddSubscriptionLocked(SignalSubscription subscription)
    {
        var subscriptions = _subscriptions;
        if (subscriptions == null)
        {
            subscriptions = new SignalSubscription[4];
            Volatile.Write(ref _subscriptions, subscriptions);
        }

        for (var i = 0; i < _subscriptionTail; i++)
        {
            if (subscriptions[i] != null)
            {
                continue;
            }

            subscription.Index = i;
            Volatile.Write(ref subscriptions[i], subscription);
            _subscriptionCount++;
            return;
        }

        if (_subscriptionTail == subscriptions.Length)
        {
            var copy = new SignalSubscription[subscriptions.Length * 2];
            Array.Copy(subscriptions, copy, subscriptions.Length);
            subscriptions = copy;
            Volatile.Write(ref _subscriptions, subscriptions);
        }

        subscription.Index = _subscriptionTail;
        Volatile.Write(ref subscriptions[_subscriptionTail], subscription);
        _subscriptionTail++;
        _subscriptionCount++;
    }

    private SignalSubscription?[]? ClearObserversLocked()
    {
        _singleActionSubscription = null;
        var subscriptions = _subscriptions;
        Volatile.Write(ref _subscriptions, null);
        _subscriptionCount = 0;
        _subscriptionTail = 0;
        _onNext = NoopOnNext;
        return subscriptions;
    }

    private void PromoteSingleActionObserverLocked()
    {
        var single = _singleActionSubscription;
        if (single == null)
        {
            return;
        }

        _singleActionSubscription = null;
        AddSubscriptionLocked(single);
    }

    private void Remove(SignalSubscription subscription)
    {
        lock (_observerLock)
        {
            if (ReferenceEquals(_singleActionSubscription, subscription))
            {
                _singleActionSubscription = null;
                _onNext = _subscriptionCount == 0 ? NoopOnNext : DispatchSubscriptions;
                return;
            }

            var subscriptions = _subscriptions;
            var index = subscription.Index;
            if (subscriptions == null ||
                (uint)index >= (uint)subscriptions.Length ||
                !ReferenceEquals(subscriptions[index], subscription))
            {
                return;
            }

            Volatile.Write(ref subscriptions[index], null);
            _subscriptionCount--;
            if (_subscriptionCount == 0)
            {
                _subscriptionTail = 0;
                _onNext = NoopOnNext;
            }
        }
    }

    private void DispatchSubscriptions(T value)
    {
        var subscriptions = Volatile.Read(ref _subscriptions);
        if (subscriptions == null)
        {
            return;
        }

        for (var i = 0; i < subscriptions.Length; i++)
        {
            var subscription = Volatile.Read(ref subscriptions[i]);
            if (subscription == null)
            {
                continue;
            }

            var onNext = subscription.OnNext;
            if (onNext != null)
            {
                onNext(value);
            }
            else
            {
                subscription.Observer!.OnNext(value);
            }
        }
    }

    private sealed class SignalSubscription : IDisposable
    {
        private Signal<T>? _subject;

        public SignalSubscription(Signal<T> subject, IObserver<T> observer)
        {
            _subject = subject;
            Observer = observer;
            Index = -1;
        }

        public SignalSubscription(Signal<T> subject, Action<T> onNext)
        {
            _subject = subject;
            OnNext = onNext;
            Index = -1;
        }

        public int Index { get; set; }

        public IObserver<T>? Observer { get; }

        public Action<T>? OnNext { get; }

        public void Dispose()
        {
            var subject = Interlocked.Exchange(ref _subject, null);
            subject?.Remove(this);
        }
    }
}
