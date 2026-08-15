// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions;

/// <summary>
/// Thread-safe subject that holds the most-recently-emitted value, replays it to new subscribers,
/// and broadcasts subsequent emissions. Per-emission hot path:
/// <list type="bullet">
///   <item>Lock taken only to read the current observer state and update the cached value.</item>
///   <item>Single-observer state lives in a dedicated field — no array allocated for the common case.</item>
///   <item>Multi-observer state uses a copy-on-write <c>IObserver{T}[]</c> snapshot iterated outside the lock.</item>
/// </list>
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Value = {_value}, Completed = {_completed}, Disposed = {_disposed}")]
public sealed class CurrentValueSubject<T> : IObservable<T>, IObserver<T>, IDisposable
{
    /// <summary>Lock guarding state mutations; held only across snapshot reads and field writes.</summary>
    private readonly Lock _gate = new();

    /// <summary>Single-observer fast path; non-null when exactly one observer is subscribed.</summary>
    private IObserver<T>? _observer;

    /// <summary>Multi-observer snapshot; non-null when two or more observers are subscribed. Copy-on-write.</summary>
    private IObserver<T>[]? _observers;

    /// <summary>Latest value, replayed to new subscribers.</summary>
    private T _value;

    /// <summary>Terminal error; non-null once <see cref="OnError"/> has fired.</summary>
    private Exception? _error;

    /// <summary>Latched when the source has completed.</summary>
    private bool _completed;

    /// <summary>Latched when <see cref="Dispose"/> has been called.</summary>
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="CurrentValueSubject{T}"/> class with the supplied current value.</summary>
    /// <param name="initialValue">The value replayed to subscribers until <see cref="OnNext"/> overwrites it.</param>
    public CurrentValueSubject(T initialValue) => _value = initialValue;

    /// <summary>Gets the most recently emitted value.</summary>
    public T Value
    {
        get
        {
            lock (_gate)
            {
                return _value;
            }
        }
    }

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        IObserver<T>? single;
        IObserver<T>[]? multi;
        lock (_gate)
        {
            if (_disposed || _completed || _error is not null)
            {
                return;
            }

            _value = value;
            single = _observer;
            multi = _observers;
        }

        if (single is not null)
        {
            single.OnNext(value);
            return;
        }

        if (multi is null)
        {
            return;
        }

        for (var i = 0; i < multi.Length; i++)
        {
            multi[i].OnNext(value);
        }
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

        IObserver<T>? single;
        IObserver<T>[]? multi;
        lock (_gate)
        {
            if (_disposed || _completed || _error is not null)
            {
                return;
            }

            _error = error;
            single = _observer;
            multi = _observers;
            _observer = null;
            _observers = null;
        }

        if (single is not null)
        {
            single.OnError(error);
            return;
        }

        if (multi is null)
        {
            return;
        }

        for (var i = 0; i < multi.Length; i++)
        {
            multi[i].OnError(error);
        }
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
        IObserver<T>? single;
        IObserver<T>[]? multi;
        lock (_gate)
        {
            if (_disposed || _completed || _error is not null)
            {
                return;
            }

            _completed = true;
            single = _observer;
            multi = _observers;
            _observer = null;
            _observers = null;
        }

        if (single is not null)
        {
            single.OnCompleted();
            return;
        }

        if (multi is null)
        {
            return;
        }

        for (var i = 0; i < multi.Length; i++)
        {
            multi[i].OnCompleted();
        }
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        lock (_gate)
        {
            if (_disposed)
            {
                observer.OnError(new ObjectDisposedException(nameof(CurrentValueSubject<>)));
                return EmptyDisposable.Instance;
            }

            if (_error is not null)
            {
                observer.OnError(_error);
                return EmptyDisposable.Instance;
            }

            observer.OnNext(_value);

            if (_completed)
            {
                observer.OnCompleted();
                return EmptyDisposable.Instance;
            }

            AddObserverNoLock(observer);
        }

        return new Subscription(this, observer);
    }

    /// <summary>Returns an <see cref="IObservable{T}"/> view that hides the <see cref="IObserver{T}"/> side.</summary>
    /// <returns>A read-only observable view.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IObservable<T> AsObservable() => new ReadOnlyView(this);

    /// <inheritdoc/>
    public void Dispose()
    {
        lock (_gate)
        {
            _disposed = true;
            _observer = null;
            _observers = null;
        }
    }

    /// <summary>Adds an observer to the subscriber state; assumes the caller holds <see cref="_gate"/>.</summary>
    /// <param name="observer">The observer to attach.</param>
    private void AddObserverNoLock(IObserver<T> observer)
    {
        if (_observer is null && _observers is null)
        {
            _observer = observer;
            return;
        }

        if (_observer is not null)
        {
            _observers = [_observer, observer];
            _observer = null;
            return;
        }

        var existing = _observers!;
        var grown = new IObserver<T>[existing.Length + 1];
        Array.Copy(existing, grown, existing.Length);
        grown[existing.Length] = observer;
        _observers = grown;
    }

    /// <summary>Removes the supplied observer from the subscriber state.</summary>
    /// <param name="observer">The observer to detach.</param>
    private void Unsubscribe(IObserver<T> observer)
    {
        lock (_gate)
        {
            if (ReferenceEquals(_observer, observer))
            {
                _observer = null;
                return;
            }

            var existing = _observers;
            if (existing is null)
            {
                return;
            }

            // Subscription.Dispose's Interlocked guard means a given observer reaches Unsubscribe
            // at most once, and Subscribe ensures that observer is present in _observers before
            // returning. OnError / OnCompleted / Dispose nullify _observers atomically, which the
            // is-null check above already short-circuits — so when we get here, IndexOf finds the
            // observer by construction.
            var index = Array.IndexOf(existing, observer);

            if (existing.Length == 2)
            {
                // Collapse back to the single-observer fast path.
                _observer = index == 0 ? existing[1] : existing[0];
                _observers = null;
                return;
            }

            var shrunk = new IObserver<T>[existing.Length - 1];
            if (index > 0)
            {
                Array.Copy(existing, 0, shrunk, 0, index);
            }

            if (index < existing.Length - 1)
            {
                Array.Copy(existing, index + 1, shrunk, index, existing.Length - index - 1);
            }

            _observers = shrunk;
        }
    }

    /// <summary>Per-subscription handle that detaches the observer on dispose. Idempotency is
    /// enforced via <see cref="Interlocked.Exchange{T}(ref T, T)"/> on <see cref="_observer"/> —
    /// the second dispose sees <see langword="null"/> and returns. Eliminates the previous
    /// dedicated <c>_disposed</c> int and shaves a field off every subscription.</summary>
    /// <param name="parent">The owning subject.</param>
    /// <param name="observer">The observer to detach.</param>
    private sealed class Subscription(CurrentValueSubject<T> parent, IObserver<T> observer) : IDisposable
    {
        /// <summary>Observer reference captured at attach time; nulled atomically on first dispose.</summary>
        private IObserver<T>? _observer = observer;

        /// <inheritdoc/>
        public void Dispose()
        {
            var captured = Interlocked.Exchange(ref _observer, null);
            if (captured is null)
            {
                return;
            }

            parent.Unsubscribe(captured);
        }
    }

    /// <summary>Read-only observable view that forwards subscription to the owning subject.</summary>
    /// <param name="parent">The owning subject.</param>
    private sealed class ReadOnlyView(CurrentValueSubject<T> parent) : IObservable<T>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Subscribe(IObserver<T> observer) => parent.Subscribe(observer);
    }
}
