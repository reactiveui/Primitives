// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Mutable state and mechanics backing the latest-value (behavior) signals. A single signal instance owns one
/// of these inline (no separate heap object) and forwards its public surface here, so the latest-value logic
/// lives in one place without inheritance or composition between the signal types.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.CodeAnalysis.SuppressMessage(
    "Performance",
    "SST1803:Make record struct readonly",
    Justification = "This is mutable signal state; its members mutate the fields in place, so it cannot be readonly.")]
internal record struct BehaviorSignalState<T>
{
    /// <summary>Protects observer and terminal-state mutations.</summary>
    private readonly Lock _gate;

    /// <summary>The fan-out broadcaster.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Performance",
        "SST1424:Make field readonly",
        Justification =
            "Broadcaster<T> is a mutable struct; readonly fields would mutate defensive copies and lose observer updates.")]
    private Broadcaster<T> _broadcaster;

    /// <summary>The last error, when terminated exceptionally.</summary>
    private Exception? _lastError;

    /// <summary>The last observed value.</summary>
    private T? _lastValue;

    /// <summary>Whether the sequence has terminated.</summary>
    private bool _isStopped;

    /// <summary>Disposal latch; non-zero once the signal has been disposed.</summary>
    private int _isDisposed;

    /// <summary>Initializes a new instance of the <see cref="BehaviorSignalState{T}"/> struct.</summary>
    /// <param name="defaultValue">The initial current value.</param>
    public BehaviorSignalState(T defaultValue)
    {
        _gate = new();
        _broadcaster = default;
        _lastValue = defaultValue;
    }

    /// <summary>Gets a value indicating whether the signal has been disposed.</summary>
    internal readonly bool IsDisposed => _isDisposed != 0;

    /// <summary>Gets a value indicating whether the signal currently has observers.</summary>
    internal bool HasObservers => _broadcaster.HasObservers && !_isStopped && Volatile.Read(ref _isDisposed) == 0;

    /// <summary>Gets the current value, throwing if disposed or faulted.</summary>
    /// <returns>The current value.</returns>
    internal readonly T GetValue()
    {
        ThrowIfDisposed();
        _lastError.Rethrow();

        return _lastValue!;
    }

    /// <summary>Tries to read the current value without throwing when disposed.</summary>
    /// <param name="value">The current value, or <see langword="default"/> when disposed.</param>
    /// <returns><see langword="true"/> when a value is available.</returns>
    internal readonly bool TryGetValue(out T? value)
    {
        lock (_gate)
        {
            if (_isDisposed != 0)
            {
                value = default;
                return false;
            }

            _lastError.Rethrow();

            value = _lastValue!;
            return true;
        }
    }

    /// <summary>Notifies all observers about the end of the sequence.</summary>
    /// <remarks>
    /// The broadcast runs under <see cref="_gate"/> so it serializes against <see cref="Subscribe"/>: a new
    /// subscriber is either added before this completes (and is broadcast to here) or after (and replays the
    /// terminal state itself), never seeing an out-of-order or duplicated notification.
    /// </remarks>
    internal void OnCompleted()
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            _isStopped = true;
            _broadcaster.Completed();
            _broadcaster.Clear();
        }
    }

    /// <summary>Notifies all observers about the exception.</summary>
    /// <param name="error">The exception to send to all observers.</param>
    internal void OnError(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

        lock (_gate)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            _isStopped = true;
            _lastError = error;
            _broadcaster.Error(error);
            _broadcaster.Clear();
        }
    }

    /// <summary>Notifies all observers about the arrival of the specified value.</summary>
    /// <param name="value">The value to send to all observers.</param>
    /// <remarks>
    /// The latest-value update and the broadcast happen together under <see cref="_gate"/>, so they are atomic
    /// with respect to <see cref="Subscribe"/>; a new subscriber never observes a live value before the initial
    /// value it was promised, and never observes the same value twice.
    /// </remarks>
    internal void OnNext(T value)
    {
        lock (_gate)
        {
            if (_isStopped)
            {
                return;
            }

            _lastValue = value;
            _broadcaster.Next(value);
        }
    }

    /// <summary>Subscribes an observer, replaying the current value or terminal notification.</summary>
    /// <param name="owner">The owning signal used to remove the observer on disposal.</param>
    /// <param name="observer">The observer to subscribe.</param>
    /// <returns>A handle that unsubscribes the observer when disposed.</returns>
    internal IDisposable Subscribe(IWitnessRemovable<T> owner, IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var ex = default(Exception);

        lock (_gate)
        {
            ThrowIfDisposed();
            if (!_isStopped)
            {
                // Add and deliver the initial value under the same gate that serializes live broadcast, so
                // the new observer is either added before a concurrent OnNext (and sees the initial value
                // first, then the live value) or after it (and the live value becomes its initial value).
                // It can never observe a newer live value ahead of, or in addition to, its initial value.
                _broadcaster.Add(observer);
                var subscription = new BehaviorWitnessHandler<T>(owner, observer);
                observer.OnNext(_lastValue!);
                return subscription;
            }

            ex = _lastError;
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

    /// <summary>Removes a previously subscribed observer.</summary>
    /// <param name="observer">The observer to remove.</param>
    internal void RemoveObserver(IObserver<T> observer)
    {
        lock (_gate)
        {
            _broadcaster.Remove(observer);
        }
    }

    /// <summary>Releases the signal's observers and cached state.</summary>
    internal void Release()
    {
        if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
        {
            return;
        }

        lock (_gate)
        {
            _broadcaster.Clear();
            _lastError = null;
            _lastValue = default;
        }
    }

    /// <summary>Throws when the signal has been disposed.</summary>
    /// <exception cref="ObjectDisposedException">The signal has already been released.</exception>
    private readonly void ThrowIfDisposed()
    {
        if (_isDisposed == 0)
        {
            return;
        }

        throw new ObjectDisposedException(string.Empty);
    }
}
