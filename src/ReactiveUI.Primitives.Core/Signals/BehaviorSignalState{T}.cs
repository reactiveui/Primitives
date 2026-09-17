// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Stores the latest value, subscribers, and terminal state of a behavior signal.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <remarks>
/// Notifications are posted to each subscriber under the gate, so every subscriber sees its initial value first and later values
/// in the order they were set, and are delivered after the gate is released; no observer runs while it is held.
/// </remarks>
[System.Diagnostics.CodeAnalysis.SuppressMessage(
    "Performance",
    "SST1803:Make record struct readonly",
    Justification = "The members mutate these fields in place.")]
internal record struct BehaviorSignalState<T>
{
    /// <summary>Protects the latest value, the subscriber set, and terminal-state mutations.</summary>
    private readonly Lock _gate;

    /// <summary>The subscribers notifications are posted to.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Performance",
        "SST1424:Make field readonly",
        Justification = "A readonly field would mutate a defensive copy of this mutable struct and lose observer updates.")]
    private SerializedBroadcaster<T> _broadcaster;

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

    /// <summary>Posts completion under the gate, preventing duplicate or out-of-order terminal notifications, then delivers it.</summary>
    internal void OnCompleted()
    {
        SerializedBroadcast<T> broadcast;
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            _isStopped = true;
            broadcast = _broadcaster.PostCompleted();
            _broadcaster.Clear();
        }

        broadcast.Flush();
    }

    /// <summary>Notifies all observers about the exception.</summary>
    /// <param name="error">The exception to send to all observers.</param>
    internal void OnError(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);

        SerializedBroadcast<T> broadcast;
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_isStopped)
            {
                return;
            }

            _isStopped = true;
            _lastError = error;
            broadcast = _broadcaster.PostError(error);
            _broadcaster.Clear();
        }

        broadcast.Flush();
    }

    /// <summary>Updates the latest value, posts it to every subscriber, and delivers it.</summary>
    /// <param name="value">The value to send to all observers.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void OnNext(T value) => Post(value).Flush();

    /// <summary>Updates the latest value and posts it to every subscriber under the gate, without delivering it.</summary>
    /// <param name="value">The value to post.</param>
    /// <returns>The batch to flush once any lock the caller holds is released.</returns>
    internal SerializedBroadcast<T> Post(T value)
    {
        lock (_gate)
        {
            if (_isStopped)
            {
                return default;
            }

            _lastValue = value;
            return _broadcaster.PostNext(value);
        }
    }

    /// <summary>Subscribes an observer, replaying the current value or terminal notification.</summary>
    /// <param name="owner">The owning signal that the returned handle removes the observer from.</param>
    /// <param name="observer">The observer to subscribe.</param>
    /// <returns>A handle that unsubscribes the observer when disposed.</returns>
    internal IDisposable Subscribe(IWitnessRemovable<T> owner, IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SerializedWitness<T>? witness = null;
        T? initial = default;
        Exception? error;
        lock (_gate)
        {
            ThrowIfDisposed();
            error = _lastError;
            if (!_isStopped)
            {
                // A new witness is always claimable; claiming it before it is added queues later values behind the initial value.
                witness = new(observer);
                _ = witness.TryClaim();
                initial = _lastValue;
                _broadcaster.Add(witness);
            }
        }

        if (witness is not null)
        {
            witness.DeliverClaimed(initial!);
            return new BehaviorWitnessHandler<T>(owner, witness);
        }

        if (error is not null)
        {
            observer.OnError(error);
        }
        else
        {
            observer.OnCompleted();
        }

        return EmptyDisposable.Instance;
    }

    /// <summary>Removes a subscribed observer from the subscriber set.</summary>
    /// <param name="observer">The subscriber's witness, as handed to its subscription handle.</param>
    internal void RemoveObserver(IObserver<T> observer)
    {
        lock (_gate)
        {
            _broadcaster.Remove((SerializedWitness<T>)observer);
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
    /// <exception cref="ObjectDisposedException">The signal is released.</exception>
    private readonly void ThrowIfDisposed()
    {
        if (_isDisposed == 0)
        {
            return;
        }

        throw new ObjectDisposedException(string.Empty);
    }
}
