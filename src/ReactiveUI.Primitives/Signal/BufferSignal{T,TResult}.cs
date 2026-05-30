// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Represents the BufferSignal class.
/// </summary>
/// <typeparam name="T">The T type.</typeparam>
/// <typeparam name="TResult">The TResult type.</typeparam>
internal sealed class BufferSignal<T, TResult> : Signal<TResult>
    where TResult : class, IList<T>
{
    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly int _skip;

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly int _count;

    /// <summary>
    /// The current window buffer, sized to <see cref="_count"/>; <see langword="null"/> between windows.
    /// </summary>
    private T[]? _buffer;

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private int _index;

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private IDisposable? _subscription;

    /// <summary>
    /// Initializes a new instance of the <see cref="BufferSignal{T,TResult}"/> class.
    /// </summary>
    /// <param name="source">The source value.</param>
    /// <param name="count">The count value.</param>
    /// <param name="skip">The skip value.</param>
    public BufferSignal(IObservable<T> source, int count, int skip)
    {
        _skip = skip;
        _count = count;
        _subscription = source.Subscribe(
            next =>
            {
                if (IsDisposed)
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
                    buffer![idx] = next;
                }

                if (++idx == _count)
                {
                    _buffer = null;

                    // Set the skip.
                    idx = 0 - _skip;

                    // The window is full, so the array is exactly the right size; emit it directly.
                    OnNext((TResult)(IList<T>)buffer!);
                }

                _index = idx;
            },
            (ex) =>
            {
                _buffer = null;
                OnError(ex);
            },
            () =>
            {
                var buffer = _buffer;
                var length = _index;
                _buffer = null;

                if (buffer != null)
                {
                    OnNext(ToResult(buffer, length));
                }

                OnCompleted();
            });
    }

    /// <summary>
    /// Executes the Dispose operation.
    /// </summary>
    /// <param name="disposing">The disposing value.</param>
    protected override void Dispose(bool disposing)
    {
        if (IsDisposed || !disposing)
        {
            base.Dispose(disposing);
            return;
        }

        var buffer = _buffer;
        var length = _index;
        _buffer = null;

        if (buffer != null)
        {
            OnNext(ToResult(buffer, length));
        }

        _subscription?.Dispose();
        _subscription = null;
        base.Dispose(disposing);
    }

    /// <summary>
    /// Returns the buffer as a result, copying to an exactly-sized array only for a partial trailing window.
    /// </summary>
    /// <param name="buffer">The window buffer.</param>
    /// <param name="length">The number of filled elements.</param>
    /// <returns>The result list.</returns>
    private static TResult ToResult(T[] buffer, int length)
    {
        if (length == buffer.Length)
        {
            return (TResult)(IList<T>)buffer;
        }

        var exact = new T[length];
        Array.Copy(buffer, exact, length);
        return (TResult)(IList<T>)exact;
    }
}
