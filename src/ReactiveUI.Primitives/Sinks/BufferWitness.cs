// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>Sink that batches source values into fixed-size windows.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class BufferWitness<T> : SingleSourceWitness<T>
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<IList<T>> _observer;

    /// <summary>The window size.</summary>
    private readonly int _count;

    /// <summary>The number of elements skipped between windows.</summary>
    private readonly int _skip;

    /// <summary>The current window buffer, sized to <see cref="_count"/>; <see langword="null"/> between windows.</summary>
    private T[]? _buffer;

    /// <summary>The window index, which doubles as the array slot while non-negative.</summary>
    private int _index;

    /// <summary>Initializes a new instance of the <see cref="BufferWitness{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="count">The window size.</param>
    /// <param name="skip">The number of elements skipped between windows.</param>
    public BufferWitness(IObserver<IList<T>> observer, int count, int skip)
    {
        _observer = observer;
        _count = count;
        _skip = skip;
    }

    /// <inheritdoc/>
    public override void OnNext(T value)
    {
        var idx = _index;
        var buffer = _buffer;
        if (idx == 0)
        {
            // Window starts: allocate exactly one array of the known window size.
            buffer = new T[_count];
            _buffer = buffer;
        }

        // Take while not skipping; the window index doubles as the array slot.
        if (idx >= 0)
        {
            buffer![idx] = value;
        }

        if (++idx == _count)
        {
            _buffer = null;

            // Set the skip.
            idx = 0 - _skip;

            // The window is full, so the array is exactly the right size; emit it directly.
            Emit(buffer!);
        }

        _index = idx;
    }

    /// <inheritdoc/>
    public override void OnError(Exception error)
    {
        _buffer = null;
        SinkTerminal.Fault(_observer, error, this);
    }

    /// <inheritdoc/>
    public override void OnCompleted()
    {
        var buffer = _buffer;
        var length = _index;
        _buffer = null;

        try
        {
            if (buffer is not null && length > 0)
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

    /// <summary>Returns the window array, copying to an exact-size array only for a partial trailing window.</summary>
    /// <param name="buffer">The window buffer.</param>
    /// <param name="length">The number of filled elements.</param>
    /// <returns>The window array.</returns>
    private static T[] Trim(T[] buffer, int length)
    {
        if (length == buffer.Length)
        {
            return buffer;
        }

        var exact = new T[length];
        Array.Copy(buffer, exact, length);
        return exact;
    }

    /// <summary>Forwards a completed window, tearing down the sink if the observer throws.</summary>
    /// <param name="batch">The completed window.</param>
    private void Emit(IList<T> batch)
    {
        try
        {
            _observer.OnNext(batch);
        }
        catch
        {
            Dispose();
            throw;
        }
    }
}
