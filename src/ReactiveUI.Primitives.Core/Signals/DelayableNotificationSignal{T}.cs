// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// A signal that passes notifications through immediately while they are not delayed, but buffers them while delayed
/// and emits a de-duplicated batch when <see cref="Flush"/> is called (typically as the delay window opens or closes).
/// Fuses the <c>Buffer(boundary).SelectMany(distinct).Publish().RefCount()</c> pipeline into one allocation-light sink.
/// </summary>
/// <typeparam name="T">The notification type.</typeparam>
[System.Diagnostics.DebuggerDisplay("DelayableNotificationSignal: Stopped = {_stopped}, Buffer = {_buffer}")]
public sealed class DelayableNotificationSignal<T> : ISignal<T>
{
    /// <summary>Guards the observer set, buffer, and terminal state.</summary>
    private readonly Lock _gate = new();

    /// <summary>Returns whether notifications are currently delayed.</summary>
    private readonly Func<bool> _isDelayed;

    /// <summary>De-duplicates a buffered batch before it is emitted on flush.</summary>
    private readonly Func<IList<T>, IEnumerable<T>> _flushDistinct;

    /// <summary>The observers subscribed to this signal.</summary>
    private Broadcaster<T> _broadcaster;

    /// <summary>Holds notifications produced while delayed; null until the first buffered notification.</summary>
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
        lock (_gate)
        {
            if (_stopped)
            {
                return;
            }

            if (_isDelayed())
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
        List<T> batch;
        lock (_gate)
        {
            if (_stopped || _buffer is null || _buffer.Count == 0)
            {
                return;
            }

            batch = [.. _flushDistinct(_buffer)];
            _buffer.Clear();
        }

        for (var i = 0; i < batch.Count; i++)
        {
            _broadcaster.Next(batch[i]);
        }
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);
        lock (_gate)
        {
            if (_error is not null)
            {
                observer.OnError(_error);
                return EmptyDisposable.Instance;
            }

            if (_stopped)
            {
                observer.OnCompleted();
                return EmptyDisposable.Instance;
            }

            _broadcaster.Add(observer);
        }

        return new Subscription(this, observer);
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
