// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
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

    /// <summary>Published in place of the observers once a terminal notification has been delivered.</summary>
    private static readonly object StoppedMarker = new();

    /// <summary>Published in place of the observers once the signal has been disposed.</summary>
    private static readonly object DisposedMarker = new();

    /// <summary>
    /// Guards observer-set and terminal-state mutations. Dispatch does not take it: subscribe, remove, and
    /// the terminal transitions each publish one new value to <see cref="_observers"/>, and they mutate
    /// reusable array slots in place rather than copying, so subscribe/unsubscribe churn does not allocate a
    /// new array per change.
    /// </summary>
    private readonly Lock _observerLock = new();

    /// <summary>Stores state for the signal implementation.</summary>
    private Exception? _exception;

    /// <summary>
    /// The dispatch target, and the only field <see cref="OnNext"/> reads: <see langword="null"/> while
    /// nobody is subscribed, the subscription itself for exactly one subscriber, the slot array for more, or
    /// one of the terminal markers. Every shape change publishes a single new value here, so a dispatch sees
    /// either the whole change or none of it. That is what lets the emit path run without taking the gate.
    /// </summary>
    private object? _observers;

    /// <summary>The reusable slot array backing the multi-subscriber shape, kept across an empty period.</summary>
    private SignalSubscription?[]? _slots;

    /// <summary>Stores state for the signal implementation.</summary>
    private int _subscriptionCount;

    /// <summary>Stores state for the signal implementation.</summary>
    private int _subscriptionTail;

    /// <summary>Stores state for the signal implementation.</summary>
    private bool _isDisposed;

    /// <summary>Stores state for the signal implementation.</summary>
    private bool _isStopped;

    /// <summary>Gets a value indicating whether indicates whether the subject has observers subscribed to it.</summary>
    public virtual bool HasObservers
    {
        get
        {
            var observers = Volatile.Read(ref _observers);
            return observers is SignalSubscription
                || (observers is SignalSubscription?[] && _subscriptionCount != 0);
        }
    }

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
        object? observers;

        lock (_observerLock)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            observers = ClearObserversLocked(StoppedMarker);
            _isStopped = true;
        }

        Completed(observers);
    }

    /// <summary>Called when [error].</summary>
    /// <param name="error">The error.</param>
    public void OnError(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

        object? observers;

        lock (_observerLock)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            _exception = error;
            observers = ClearObserversLocked(StoppedMarker);
            _isStopped = true;
        }

        Error(observers, error);
        if (!HasActionSubscribers(observers))
        {
            return;
        }

        ExceptionDispatchInfo.Capture(error).Throw();
    }

    /// <summary>Called when [next].</summary>
    /// <param name="value">The value.</param>
    /// <remarks>
    /// One volatile read of <see cref="_observers"/> decides the whole dispatch, so emitting never takes the
    /// observer gate. A terminal transition or a disposal publishes a marker to that same field, which is how
    /// a stopped signal stays silent and a disposed one still throws without a lock on the emit path.
    /// </remarks>
    public void OnNext(T value)
    {
        var observers = Volatile.Read(ref _observers);
        if (observers is SignalSubscription single)
        {
            single.OnNext(value);
            return;
        }

        if (observers is SignalSubscription?[] subscriptions)
        {
            DispatchToSlots(subscriptions, value);
            return;
        }

        if (!ReferenceEquals(observers, DisposedMarker))
        {
            return;
        }

        throw Disposed();
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
                subscription = new(this, observer);
                AddSubscriptionLocked(subscription);
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
                AddSubscriptionLocked(subscription);
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

        object? observers;

        lock (_observerLock)
        {
            _exception = null;

            // Set before the marker is published: the marker goes out with a release write, so a dispatch
            // that acquires it is guaranteed to see the disposed flag its trailing check reads.
            _isDisposed = true;
            observers = ClearObserversLocked(DisposedMarker);
        }

        DisposeSubscriptions(observers);
    }

    /// <summary>Creates the exception every use-after-disposal path throws.</summary>
    /// <returns>The exception to throw.</returns>
    /// <remarks>
    /// Returned rather than thrown so each caller ends in <c>throw</c>. The guard-clause shape the analyzers
    /// require would otherwise leave every one of those methods with an epilogue nothing can reach.
    /// </remarks>
    private static ObjectDisposedException Disposed() => new(string.Empty);

    /// <summary>Executes the Completed operation.</summary>
    /// <param name="observers">The observer shape captured while the signal was still running.</param>
    private static void Completed(object? observers)
    {
        if (observers is SignalSubscription single)
        {
            single.OnCompleted();
            return;
        }

        if (observers is not SignalSubscription?[] subscriptions)
        {
            return;
        }

        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i]?.OnCompleted();
        }
    }

    /// <summary>Executes the Error operation.</summary>
    /// <param name="observers">The observer shape captured while the signal was still running.</param>
    /// <param name="exception">The exception value.</param>
    private static void Error(object? observers, Exception exception)
    {
        if (observers is SignalSubscription single)
        {
            single.OnError(exception);
            return;
        }

        if (observers is not SignalSubscription?[] subscriptions)
        {
            return;
        }

        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i]?.OnError(exception);
        }
    }

    /// <summary>Executes the HasActionSubscribers operation.</summary>
    /// <param name="observers">The observer shape captured while the signal was still running.</param>
    /// <returns>The result.</returns>
    private static bool HasActionSubscribers(object? observers)
    {
        if (observers is SignalSubscription single)
        {
            return single.IsAction;
        }

        if (observers is not SignalSubscription?[] subscriptions)
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
    /// <param name="observers">The observer shape captured before disposal.</param>
    private static void DisposeSubscriptions(object? observers)
    {
        if (observers is SignalSubscription single)
        {
            single.Dispose();
            return;
        }

        if (observers is not SignalSubscription?[] subscriptions)
        {
            return;
        }

        for (var i = 0; i < subscriptions.Length; i++)
        {
            subscriptions[i]?.Dispose();
        }
    }

    /// <summary>Executes the DispatchSubscriptions operation.</summary>
    /// <param name="subscriptions">The subscription snapshot, which the observer field only ever holds non-null.</param>
    /// <param name="value">The value.</param>
    private static void DispatchSubscriptions(SignalSubscription?[] subscriptions, T value)
    {
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

    /// <summary>Dispatches to every live slot, then reports a disposal that raced the dispatch.</summary>
    /// <param name="subscriptions">The slot array this dispatch captured.</param>
    /// <param name="value">The value.</param>
    private void DispatchToSlots(SignalSubscription?[] subscriptions, T value)
    {
        DispatchSubscriptions(subscriptions, value);
        if (!Volatile.Read(ref _isDisposed))
        {
            return;
        }

        throw Disposed();
    }

    /// <summary>Executes the ThrowIfDisposed operation.</summary>
    private void ThrowIfDisposed()
    {
        if (!IsDisposed)
        {
            return;
        }

        throw Disposed();
    }

    /// <summary>Adds a subscription and publishes the resulting observer shape.</summary>
    /// <param name="subscription">The subscription value.</param>
    private void AddSubscriptionLocked(SignalSubscription subscription)
    {
        if (_observers is null)
        {
            _subscriptionCount = 1;
            Volatile.Write(ref _observers, subscription);
            return;
        }

        if (_observers is SignalSubscription single)
        {
            _subscriptionCount = 0;
            _subscriptionTail = 0;
            _ = AddToSlotsLocked(single);
        }

        Volatile.Write(ref _observers, AddToSlotsLocked(subscription));
    }

    /// <summary>Places a subscription in the reusable slot array, growing it when every slot is taken.</summary>
    /// <param name="subscription">The subscription value.</param>
    /// <returns>The slot array the subscription now lives in.</returns>
    private SignalSubscription?[] AddToSlotsLocked(SignalSubscription subscription)
    {
        var slots = _slots;
        if (slots is null)
        {
            slots = new SignalSubscription[InitialSubscriptionCapacity];
            _slots = slots;
        }

        for (var i = 0; i < _subscriptionTail; i++)
        {
            if (slots[i] is not null)
            {
                continue;
            }

            Volatile.Write(ref slots[i], subscription);
            _subscriptionCount++;
            return slots;
        }

        if (_subscriptionTail == slots.Length)
        {
            var copy = new SignalSubscription[slots.Length * SubscriptionGrowthFactor];
            Array.Copy(slots, copy, slots.Length);
            slots = copy;
            _slots = slots;
        }

        Volatile.Write(ref slots[_subscriptionTail], subscription);
        _subscriptionTail++;
        _subscriptionCount++;
        return slots;
    }

    /// <summary>Publishes a terminal marker and hands back the observer shape it replaced.</summary>
    /// <param name="marker">The marker to publish in place of the observers.</param>
    /// <returns>The observer shape that was active before the marker was published.</returns>
    private object? ClearObserversLocked(object marker)
    {
        var observers = _observers;
        _slots = null;
        _subscriptionCount = 0;
        _subscriptionTail = 0;
        Volatile.Write(ref _observers, marker);
        return observers;
    }

    /// <summary>Executes the Remove operation.</summary>
    /// <param name="subscription">The subscription value.</param>
    private void Remove(SignalSubscription subscription)
    {
        lock (_observerLock)
        {
            if (ReferenceEquals(_observers, subscription))
            {
                _subscriptionCount = 0;
                Volatile.Write(ref _observers, null);
                return;
            }

            if (_observers is SignalSubscription?[] slots)
            {
                RemoveFromSlotsLocked(slots, subscription);
            }
        }
    }

    /// <summary>Removes an array-backed subscription, keeping the array for the next subscriber.</summary>
    /// <param name="slots">The active slot array.</param>
    /// <param name="subscription">The subscription value.</param>
    /// <remarks>
    /// A subscription disposes itself once and only ever sits in the array it was added to, so the search
    /// always finds it. The index guard is what keeps that assumption from corrupting the slot array if it
    /// ever stops holding.
    /// </remarks>
    private void RemoveFromSlotsLocked(SignalSubscription?[] slots, SignalSubscription subscription)
    {
        var index = Array.IndexOf(slots, subscription);
        if (index < 0)
        {
            return;
        }

        ClearSlotLocked(slots, index);
    }

    /// <summary>Vacates one slot and drops back to the empty shape when it was the last live subscription.</summary>
    /// <param name="slots">The active slot array.</param>
    /// <param name="index">The slot the subscription occupies.</param>
    private void ClearSlotLocked(SignalSubscription?[] slots, int index)
    {
        Volatile.Write(ref slots[index], null);
        _subscriptionCount--;
        if (_subscriptionCount != 0)
        {
            return;
        }

        // Keep the array for the next subscriber; only the shape published to dispatch goes back to empty.
        _subscriptionTail = 0;
        Volatile.Write(ref _observers, null);
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception exception) => _observer?.OnError(exception);

        /// <summary>Sends completion to observer subscriptions.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => _observer?.OnCompleted();

        /// <summary>Executes the Dispose operation.</summary>
        public void Dispose()
        {
            var subject = Interlocked.Exchange(ref _subject, null);
            subject?.Remove(this);
        }
    }
}
