// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Sink that batches source values into windows of a fixed size, opening a new window every skip values.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="observer">The downstream observer.</param>
/// <param name="count">The window size.</param>
/// <param name="skip">The number of values between the starts of consecutive windows; zero means the window size.</param>
[System.Diagnostics.DebuggerDisplay("BufferWitness: Count = {_count}, Skip = {_skip}, Index = {_index}, Done = {_done}")]
public sealed class BufferWitness<T>(IObserver<IList<T>> observer, int count, int skip) : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<IList<T>> _observer = observer;

    /// <summary>The window size.</summary>
    private readonly int _count = count;

    /// <summary>The number of values between window starts; zero is normalized to the window size.</summary>
    private readonly int _skip = skip <= 0 ? count : skip;

    /// <summary>The current window buffer while windows do not overlap; <see langword="null"/> between windows.</summary>
    private T[]? _buffer;

    /// <summary>The windows still filling while windows overlap, oldest first.</summary>
    private List<BufferWindow<T>>? _open;

    /// <summary>The window index while windows do not overlap, which doubles as the array slot while non-negative.</summary>
    private int _index;

    /// <summary>The number of values seen since the last window opened while windows overlap.</summary>
    private int _sinceOpen;

    /// <summary>The terminal latch; non-zero once the sink has terminated and must ignore further notifications.</summary>
    private int _done;

    /// <summary>The upstream subscription.</summary>
    private IDisposable? _subscription;

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (Volatile.Read(ref _done) != 0)
        {
            return;
        }

        if (_skip < _count)
        {
            AddOverlapping(value);
            return;
        }

        AddSequential(value);
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        _buffer = null;
        _open = null;
        if (Interlocked.Exchange(ref _done, 1) != 0)
        {
            return;
        }

        SinkTerminal.Fault(_observer, error, this);
    }

    /// <inheritdoc/>
    public void OnCompleted()
    {
        if (Interlocked.Exchange(ref _done, 1) != 0)
        {
            return;
        }

        var buffer = _buffer;
        var length = _index;
        var open = _open;
        _buffer = null;
        _open = null;

        try
        {
            if (open is not null)
            {
                foreach (var window in open)
                {
                    if (window.Count != 0)
                    {
                        _observer.OnNext(Trim(window.Items, window.Count));
                    }
                }
            }
            else if (buffer is not null && length > 0)
            {
                _observer.OnNext(Trim(buffer, length));
            }

            _observer.OnCompleted();
        }
        finally
        {
            Dispose();
        }
    }

    /// <summary>Assigns the upstream subscription, disposing the incoming one when this sink holds a subscription or has been disposed.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    public void Dispose()
    {
        // Values after teardown are ignored, including after observer failure.
        Volatile.Write(ref _done, 1);
        SinkSubscription.Dispose(ref _subscription);
    }

    /// <summary>Copies the partial trailing window into an exact-size array.</summary>
    /// <param name="buffer">The window buffer.</param>
    /// <param name="length">The number of filled elements.</param>
    /// <returns>The window array.</returns>
    private static T[] Trim(T[] buffer, int length)
    {
        var exact = new T[length];
        Array.Copy(buffer, exact, length);
        return exact;
    }

    /// <summary>Writes a value into the single open window, emitting it once full and then counting down any gap.</summary>
    /// <param name="value">The value to add.</param>
    private void AddSequential(T value)
    {
        var idx = _index;
        var buffer = _buffer;
        if (idx == 0)
        {
            buffer = new T[_count];
            _buffer = buffer;
        }

        if (idx >= 0)
        {
            buffer![idx] = value;
        }

        idx++;
        if (idx != _count)
        {
            _index = idx;
            return;
        }

        _buffer = null;

        // A skip wider than the window leaves a gap; the negative index counts it down.
        _index = _count - _skip;

        Emit(buffer!);
    }

    /// <summary>Writes a value into every open window, opening one every skip values and emitting those that fill.</summary>
    /// <param name="value">The value to add.</param>
    private void AddOverlapping(T value)
    {
        var open = _open ??= [];

        if (_sinceOpen == 0)
        {
            open.Add(new(new T[_count], 0));
        }

        _sinceOpen++;
        if (_sinceOpen == _skip)
        {
            _sinceOpen = 0;
        }

        for (var i = 0; i < open.Count; i++)
        {
            var window = open[i];
            window.Items[window.Count] = value;
            open[i] = new(window.Items, window.Count + 1);
        }

        // Windows fill in the order they opened, so at most the oldest completes per value.
        if (open.Count == 0 || open[0].Count != _count)
        {
            return;
        }

        var full = open[0].Items;
        open.RemoveAt(0);
        Emit(full);
    }

    /// <summary>Forwards a completed window, tearing down the sink if the observer throws.</summary>
    /// <param name="batch">The completed window.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Emit(IList<T> batch) => SinkDelivery.Next(_observer, batch, this);
}
