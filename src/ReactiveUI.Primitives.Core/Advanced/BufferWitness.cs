// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Advanced;

/// <summary>Sink that batches source values into fixed-size windows.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="observer">The downstream observer.</param>
/// <param name="count">The window size.</param>
/// <param name="skip">The number of elements skipped between windows.</param>
[System.Diagnostics.DebuggerDisplay("Count = {_count}, Skip = {_skip}, Index = {_index}, Done = {_done}")]
public sealed class BufferWitness<T>(IObserver<IList<T>> observer, int count, int skip) : IObserver<T>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<IList<T>> _observer = observer;

    /// <summary>The window size.</summary>
    private readonly int _count = count;

    /// <summary>The number of elements skipped between windows.</summary>
    private readonly int _skip = skip;

    /// <summary>The current window buffer, sized to <see cref="_count"/>; <see langword="null"/> between windows.</summary>
    private T[]? _buffer;

    /// <summary>The window index, which doubles as the array slot while non-negative.</summary>
    private int _index;

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

        idx++;
        if (idx != _count)
        {
            _index = idx;
            return;
        }

        // The window is full: hand the buffer over and reset to the skip *before* the hand-off. The
        // observer may throw, and it must never be able to leave this sink holding an index into a
        // buffer it has already released — the next value would index into null.
        _buffer = null;
        _index = 0 - _skip;

        // The window is full, so the array is exactly the right size; emit it directly.
        Emit(buffer!);
    }

    /// <inheritdoc/>
    public void OnError(Exception error)
    {
        _buffer = null;
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

    /// <summary>Assigns the upstream subscription, disposing it if one is already held.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);

    /// <inheritdoc/>
    public void Dispose()
    {
        // Latching here is what makes the sink terminal on every teardown path, including the one taken
        // when the downstream observer throws out of Emit: a source that ignores disposal is then a no-op.
        Volatile.Write(ref _done, 1);
        SinkSubscription.Dispose(ref _subscription);
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
