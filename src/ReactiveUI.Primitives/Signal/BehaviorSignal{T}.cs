// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// BehaviourSignal.
/// </summary>
/// <typeparam name="T">The Type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public class BehaviorSignal<T> : ISignal<T>
{
    /// <summary>
    /// Protects observer and terminal-state mutations.
    /// </summary>
    private readonly Lock _gate = new();

#pragma warning disable S3459 // Broadcaster<T> is a mutable struct whose default value is the empty broadcaster.

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private Broadcaster<T> _broadcaster;
#pragma warning restore S3459

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private bool _isStopped;

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private T? _lastValue;

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private Exception? _lastError;

    /// <summary>
    /// Initializes a new instance of the <see cref="BehaviorSignal{T}"/> class.
    /// </summary>
    /// <param name="defaultValue">The default value.</param>
    public BehaviorSignal(T defaultValue)
    {
        _lastValue = defaultValue;
    }

    /// <summary>
    /// Gets the current value or throws an exception.
    /// </summary>
    /// <value>The initial value passed to the constructor until <see cref="OnNext"/> is called; after which, the last value passed to <see cref="OnNext"/>.</value>
    /// <remarks>
    /// <para><see cref="Value"/> is frozen after <see cref="OnCompleted"/> is called.</para>
    /// <para>After <see cref="OnError"/> is called, <see cref="Value"/> always throws the specified exception.</para>
    /// <para>An exception is always thrown after <see cref="Dispose()"/> is called.</para>
    /// <alert type="caller">
    /// Reading <see cref="Value"/> is a thread-safe operation, though there's a potential race condition when <see cref="OnNext"/> or <see cref="OnError"/> are being invoked concurrently.
    /// In some cases, it may be necessary for a caller to use external synchronization to avoid race conditions.
    /// </alert>
    /// </remarks>
    public T Value
    {
        get
        {
            ThrowIfDisposed();
            _lastError.Rethrow();

            return _lastValue!;
        }
    }

    /// <summary>
    /// Gets a value indicating whether this instance has observers.
    /// </summary>
    /// <value>
    ///   <c>true</c> if this instance has observers; otherwise, <c>false</c>.
    /// </value>
    public bool HasObservers => _broadcaster.HasObservers && !_isStopped && !IsDisposed;

    /// <summary>
    /// Gets a value indicating whether this instance is disposed.
    /// </summary>
    /// <value>
    ///   <c>true</c> if this instance is disposed; otherwise, <c>false</c>.
    /// </value>
    public bool IsDisposed { get; private set; }

    /// <summary>
    /// Gets the string representation of this object for debugger display purposes.
    /// </summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string? DebuggerDisplay
    {
        get
        {
            return ToString();
        }
    }

    /// <summary>
    /// Tries to get the current value or throws an exception.
    /// </summary>
    /// <param name="value">The initial value passed to the constructor until <see cref="OnNext"/> is called; after which, the last value passed to <see cref="OnNext"/>.</param>
    /// <returns>true if a value is available; false if the subject was disposed.</returns>
    /// <remarks>
    /// <para>The value returned from <see cref="TryGetValue"/> is frozen after <see cref="OnCompleted"/> is called.</para>
    /// <para>After <see cref="OnError"/> is called, <see cref="TryGetValue"/> always throws the specified exception.</para>
    /// <alert type="caller">
    /// Calling <see cref="TryGetValue"/> is a thread-safe operation, though there's a potential race condition when <see cref="OnNext"/> or <see cref="OnError"/> are being invoked concurrently.
    /// In some cases, it may be necessary for a caller to use external synchronization to avoid race conditions.
    /// </alert>
    /// </remarks>
    public bool TryGetValue(out T? value)
    {
        lock (_gate)
        {
            if (IsDisposed)
            {
                value = default;
                return false;
            }

            _lastError.Rethrow();

            value = _lastValue!;
            return true;
        }
    }

    /// <summary>
    /// Notifies all subscribed observers about the end of the sequence.
    /// </summary>
    public void OnCompleted()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            _isStopped = true;
        }

        _broadcaster.Completed();
        _broadcaster.Clear();
    }

    /// <summary>
    /// Notifies all subscribed observers about the exception.
    /// </summary>
    /// <param name="error">The exception to send to all observers.</param>
    /// <exception cref="ArgumentNullException"><paramref name="error"/> is <c>null</c>.</exception>
    public void OnError(Exception error)
    {
        if (error == null)
        {
            throw new ArgumentNullException(nameof(error));
        }

        lock (_gate)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            _isStopped = true;
            _lastError = error;
        }

        _broadcaster.Error(error);
        _broadcaster.Clear();
    }

    /// <summary>
    /// Notifies all subscribed observers about the arrival of the specified element in the sequence.
    /// </summary>
    /// <param name="value">The value to send to all observers.</param>
    public void OnNext(T value)
    {
        lock (_gate)
        {
            if (_isStopped)
            {
                return;
            }

            _lastValue = value;
        }

        _broadcaster.Next(value);
    }

    /// <summary>
    /// Subscribes an observer to the subject.
    /// </summary>
    /// <param name="observer">Observer to subscribe to the subject.</param>
    /// <returns>Disposable object that can be used to unsubscribe the observer from the subject.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <c>null</c>.</exception>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        var ex = default(Exception);
        var v = default(T);
        var subscription = default(ObserverHandler);

        lock (_gate)
        {
            ThrowIfDisposed();
            if (!_isStopped)
            {
                _broadcaster.Add(observer);
                v = _lastValue;
                subscription = new ObserverHandler(this, observer);
            }
            else
            {
                ex = _lastError;
            }
        }

        if (subscription != null)
        {
            observer.OnNext(v!);
            return subscription;
        }
        else if (ex != null)
        {
            observer.OnError(ex);
        }
        else
        {
            observer.OnCompleted();
        }

        return Disposable.Empty;
    }

    /// <summary>
    /// Releases unmanaged and - optionally - managed resources.
    /// </summary>
    public void Dispose()
    {
        // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Releases unmanaged and - optionally - managed resources.
    /// </summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing)
    {
        if (IsDisposed)
        {
            return;
        }

        if (disposing)
        {
            lock (_gate)
            {
                _broadcaster.Clear();
                _lastError = null;
                _lastValue = default;
            }
        }

        IsDisposed = true;
    }

    /// <summary>
    /// Executes the ThrowIfDisposed operation.
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (!IsDisposed)
        {
            return;
        }

        throw new ObjectDisposedException(string.Empty);
    }

    /// <summary>
    /// Represents the ObserverHandler class.
    /// </summary>
    private sealed class ObserverHandler : IDisposable
    {
        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private BehaviorSignal<T>? _subject;

        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private IObserver<T>? _observer;

        /// <summary>
        /// Initializes a new instance of the <see cref="ObserverHandler"/> class.
        /// </summary>
        /// <param name="subject">The subject value.</param>
        /// <param name="observer">The observer value.</param>
        public ObserverHandler(BehaviorSignal<T> subject, IObserver<T> observer)
        {
            _subject = subject;
            _observer = observer;
        }

        /// <summary>
        /// Executes the Dispose operation.
        /// </summary>
        public void Dispose()
        {
            var subject = Interlocked.Exchange(ref _subject, null);
            var observer = Interlocked.Exchange(ref _observer, null);
            if (subject == null || observer == null)
            {
                return;
            }

            lock (subject._gate)
            {
                subject._broadcaster.Remove(observer);
            }
        }
    }
}
