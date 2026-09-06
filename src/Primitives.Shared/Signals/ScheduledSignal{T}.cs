// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>A signal which emits items using the specified scheduler.</summary>
/// <typeparam name="T">The type of item to emit.</typeparam>
[System.Diagnostics.DebuggerDisplay("ScheduledSignal: Observers = {_observerRefCount}, Disposed = {_isDisposed}")]
public class ScheduledSignal<T> : ISignal<T>
{
    /// <summary>Guards default-observer and subscription-count state.</summary>
    private readonly Lock _observerLock = new();

    /// <summary>Stores the fallback observer for the signal implementation.</summary>
    private readonly IObserver<T>? _defaultObserver;

    /// <summary>Stores the scheduler for the signal implementation.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>Stores the underlying signal implementation.</summary>
    private readonly ISignal<T> _subject;

    /// <summary>Stores the active non-default observer count.</summary>
    private int _observerRefCount;

    /// <summary>Stores the active default-observer subscription.</summary>
    private IDisposable? _defaultObserverSub;

    /// <summary>Stores state for the signal implementation.</summary>
    private bool _isDisposed;

    /// <summary>Initializes a new instance of the <see cref="ScheduledSignal{T}"/> class.</summary>
    /// <param name="scheduler">The sequencer to emit items on.</param>
    public ScheduledSignal(ISequencer scheduler)
        : this(scheduler, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ScheduledSignal{T}"/> class.</summary>
    /// <param name="scheduler">The sequencer to emit items on.</param>
    /// <param name="defaultObserver">A default observer which will receive values when no other subscribers are active.</param>
    public ScheduledSignal(ISequencer scheduler, IObserver<T>? defaultObserver)
        : this(scheduler, defaultObserver, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ScheduledSignal{T}"/> class.</summary>
    /// <param name="scheduler">The sequencer to emit items on.</param>
    /// <param name="defaultObserver">A default observer which will receive values when no other subscribers are active.</param>
    /// <param name="defaultSubject">An optional backing signal this instance wraps; a new <see cref="Signal{T}"/> is used when null.</param>
    public ScheduledSignal(ISequencer scheduler, IObserver<T>? defaultObserver, ISignal<T>? defaultSubject)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        _scheduler = scheduler;
        _defaultObserver = defaultObserver;
        _subject = defaultSubject ?? new Signal<T>();
        _defaultObserverSub = defaultObserver is null ? null : SubscribeDefaultObserver(defaultObserver);
    }

    /// <inheritdoc />
    public bool HasObservers => _subject.HasObservers;

    /// <inheritdoc />
    public bool IsDisposed => _isDisposed || _subject.IsDisposed;

    /// <inheritdoc />
    public void OnCompleted()
    {
        ThrowIfDisposed();
        _subject.OnCompleted();
    }

    /// <inheritdoc />
    public void OnError(Exception error)
    {
        ThrowIfDisposed();
        _subject.OnError(error);
    }

    /// <inheritdoc />
    public void OnNext(T value)
    {
        ThrowIfDisposed();
        _subject.OnNext(value);
    }

    /// <inheritdoc />
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        IDisposable? defaultSubToDispose;

        lock (_observerLock)
        {
            ThrowIfDisposed();

            defaultSubToDispose = _defaultObserverSub;
            _defaultObserverSub = null;
            _observerRefCount++;
        }

        defaultSubToDispose?.Dispose();

        IDisposable observedSubscription;
        try
        {
            observedSubscription = _subject.ObserveOn(_scheduler).Subscribe(observer);
        }
        catch
        {
            ReleaseObserver();
            throw;
        }

        return MultipleDisposable.Create(observedSubscription, Scope.Create(ReleaseObserver));
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>Disposes managed resources that are disposable and handles cleanup of unmanaged items.</summary>
    /// <param name="disposing">If we are disposing managed resources.</param>
    protected virtual void Dispose(bool disposing)
    {
        IDisposable? defaultObserverSub = null;

        lock (_observerLock)
        {
            if (_isDisposed)
            {
                return;
            }

            _isDisposed = true;
            _observerRefCount = 0;

            if (disposing)
            {
                defaultObserverSub = _defaultObserverSub;
                _defaultObserverSub = null;
            }
        }

        if (!disposing)
        {
            return;
        }

        _subject.Dispose();
        defaultObserverSub?.Dispose();
    }

    /// <summary>Subscribes the default observer through the configured scheduler.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The subscription value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IDisposable SubscribeDefaultObserver(IObserver<T> observer) =>
        _subject.ObserveOn(_scheduler).Subscribe(observer);

    /// <summary>Releases one non-default observer and restores the default observer when needed.</summary>
    private void ReleaseObserver()
    {
        lock (_observerLock)
        {
            ReleaseObserverLocked(_defaultObserver);
        }
    }

    /// <summary>Releases one observer while the observer lock is held.</summary>
    /// <param name="defaultObserver">The default observer value.</param>
    private void ReleaseObserverLocked(IObserver<T>? defaultObserver)
    {
        if (_observerRefCount > 0)
        {
            _observerRefCount--;
        }

        if (defaultObserver is null || _isDisposed || _observerRefCount != 0 || _defaultObserverSub is not null)
        {
            return;
        }

        _defaultObserverSub = SubscribeDefaultObserver(defaultObserver);
    }

    /// <summary>Executes the ThrowIfDisposed operation.</summary>
    /// <exception cref="ObjectDisposedException">This instance or its backing signal has been disposed.</exception>
    private void ThrowIfDisposed()
    {
        if (!IsDisposed)
        {
            return;
        }

        throw new ObjectDisposedException(string.Empty);
    }
}
