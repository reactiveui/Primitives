// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Forwards notifications immediately unless delayed, then buffers them until Flush emits a batch with duplicates removed.</summary>
/// <typeparam name="T">The notification type.</typeparam>
/// <remarks>The gate only guards the buffer and terminal state; the delay check, the de-duplication and the observers run without it held.</remarks>
[System.Diagnostics.DebuggerDisplay("DelayableNotificationSignal: Stopped = {_stopped}, Buffer = {_buffer}")]
public sealed class DelayableNotificationSignal<T> : ISignal<T>
{
    /// <summary>Guards the observer set, buffer, and terminal state; never held while user code runs.</summary>
    private readonly Lock _gate = new();

    /// <summary>Returns whether notifications are currently delayed.</summary>
    private readonly Func<bool> _isDelayed;

    /// <summary>De-duplicates a buffered batch before it is emitted on flush.</summary>
    private readonly Func<IList<T>, IEnumerable<T>> _flushDistinct;

    /// <summary>The observers subscribed to this signal.</summary>
    private Broadcaster<T> _broadcaster;

    /// <summary>Holds notifications produced while delayed; null until the first buffered notification after a flush.</summary>
    private List<T>? _buffer;

    /// <summary>The terminal error, if the signal errored.</summary>
    private Exception? _error;

    /// <summary>Whether the signal has terminated (completed or errored).</summary>
    private bool _stopped;

    /// <summary>Whether the signal has been disposed.</summary>
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="DelayableNotificationSignal{T}"/> class.</summary>
    /// <param name="isDelayed">Returns whether notifications are currently delayed.</param>
    /// <param name="flushDistinct">De-duplicates a buffered batch before it is emitted on flush.</param>
    public DelayableNotificationSignal(Func<bool> isDelayed, Func<IList<T>, IEnumerable<T>> flushDistinct)
    {
        ArgumentExceptionHelper.ThrowIfNull(isDelayed);
        ArgumentExceptionHelper.ThrowIfNull(flushDistinct);

        _isDelayed = isDelayed;
        _flushDistinct = flushDistinct;
        _broadcaster = default;
    }

    /// <inheritdoc/>
    public bool HasObservers => _broadcaster.HasObservers && !_stopped;

    /// <inheritdoc/>
    public bool IsDisposed => _disposed;

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        var delayed = _isDelayed();
        lock (_gate)
        {
            if (_stopped)
            {
                return;
            }

            if (delayed)
            {
                (_buffer ??= []).Add(value);
                return;
            }
        }

        _broadcaster.Next(value);
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        lock (_gate)
        {
            if (_stopped)
            {
                return;
            }

            _stopped = true;
            _error = error;
        }

        _broadcaster.Error(error);
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
        lock (_gate)
        {
            if (_stopped)
            {
                return;
            }

            _stopped = true;
        }

        _broadcaster.Completed();
    }

    /// <summary>Emits any buffered notifications as a de-duplicated batch; call when the delay window opens or closes.</summary>
    public void Flush()
    {
        List<T> buffered;
        lock (_gate)
        {
            if (_stopped || _buffer is null)
            {
                return;
            }

            buffered = _buffer;
            _buffer = null;
        }

        foreach (var item in _flushDistinct(buffered))
        {
            _broadcaster.Next(item);
        }
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        Exception? error;
        bool stopped;
        lock (_gate)
        {
            error = _error;
            stopped = _stopped;
            if (!stopped)
            {
                _broadcaster.Add(observer);
            }
        }

        if (!stopped)
        {
            return new Subscription(this, observer);
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

    /// <inheritdoc/>
    public void Dispose()
    {
        lock (_gate)
        {
            _disposed = true;
        }
    }

    /// <summary>Removes an observer from the signal.</summary>
    /// <param name="observer">The observer to remove.</param>
    private void Unsubscribe(IObserver<T> observer)
    {
        lock (_gate)
        {
            _broadcaster.Remove(observer);
        }
    }

    /// <summary>Removes its observer from the signal when disposed.</summary>
    /// <param name="parent">The owning signal.</param>
    /// <param name="observer">The subscribed observer.</param>
    private sealed class Subscription(DelayableNotificationSignal<T> parent, IObserver<T> observer) : IDisposable
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => parent.Unsubscribe(observer);
    }
}
