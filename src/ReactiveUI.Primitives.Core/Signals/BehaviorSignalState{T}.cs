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
        Justification = "Broadcaster<T> is a mutable struct; readonly fields would mutate defensive copies and lose observer updates.")]
    private Broadcaster<T> _broadcaster;

    /// <summary>The last error, when terminated exceptionally.</summary>
    private Exception? _lastError;

    /// <summary>The last observed value.</summary>
    private T? _lastValue;

    /// <summary>Whether the sequence has terminated.</summary>
    private bool _isStopped;

    /// <summary>Whether the signal has been disposed.</summary>
    private bool _isDisposed;

    /// <summary>Initializes a new instance of the <see cref="BehaviorSignalState{T}"/> struct.</summary>
    /// <param name="defaultValue">The initial current value.</param>
    public BehaviorSignalState(T defaultValue)
    {
        _gate = new();
        _broadcaster = default;
        _lastValue = defaultValue;
    }

    /// <summary>Gets a value indicating whether the signal has been disposed.</summary>
    public readonly bool IsDisposed => _isDisposed;

    /// <summary>Gets a value indicating whether the signal currently has observers.</summary>
    public bool HasObservers => _broadcaster.HasObservers && !_isStopped && !_isDisposed;

    /// <summary>Gets the current value, throwing if disposed or faulted.</summary>
    /// <returns>The current value.</returns>
    public readonly T GetValue()
    {
        ThrowIfDisposed();
        _lastError.Rethrow();

        return _lastValue!;
    }

    /// <summary>Tries to read the current value without throwing when disposed.</summary>
    /// <param name="value">The current value, or <see langword="default"/> when disposed.</param>
    /// <returns><see langword="true"/> when a value is available.</returns>
    public readonly bool TryGetValue(out T? value)
    {
        lock (_gate)
        {
            if (_isDisposed)
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

    /// <summary>Notifies all observers about the exception.</summary>
    /// <param name="error">The exception to send to all observers.</param>
    public void OnError(Exception error)
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
        }

        _broadcaster.Error(error);
        _broadcaster.Clear();
    }

    /// <summary>Notifies all observers about the arrival of the specified value.</summary>
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

    /// <summary>Subscribes an observer, replaying the current value or terminal notification.</summary>
    /// <param name="owner">The owning signal used to remove the observer on disposal.</param>
    /// <param name="observer">The observer to subscribe.</param>
    /// <returns>A handle that unsubscribes the observer when disposed.</returns>
    public IDisposable Subscribe(IWitnessRemovable<T> owner, IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var ex = default(Exception);
        var v = default(T);
        BehaviorWitnessHandler<T>? subscription = null;

        lock (_gate)
        {
            ThrowIfDisposed();
            if (!_isStopped)
            {
                _broadcaster.Add(observer);
                v = _lastValue;
                subscription = new(owner, observer);
            }
            else
            {
                ex = _lastError;
            }
        }

        if (subscription is not null)
        {
            observer.OnNext(v!);
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

    /// <summary>Removes a previously subscribed observer.</summary>
    /// <param name="observer">The observer to remove.</param>
    public void RemoveObserver(IObserver<T> observer)
    {
        lock (_gate)
        {
            _broadcaster.Remove(observer);
        }
    }

    /// <summary>Releases the signal's observers and cached state.</summary>
    public void Release()
    {
        if (_isDisposed)
        {
            return;
        }

        lock (_gate)
        {
            _broadcaster.Clear();
            _lastError = null;
            _lastValue = default;
        }

        _isDisposed = true;
    }

    /// <summary>Throws when the signal has been disposed.</summary>
    private readonly void ThrowIfDisposed()
    {
        if (!_isDisposed)
        {
            return;
        }

        throw new ObjectDisposedException(string.Empty);
    }
}
