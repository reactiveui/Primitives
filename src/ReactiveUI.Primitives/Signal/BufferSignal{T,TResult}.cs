// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Represents the BufferSignal class.
/// </summary>
/// <typeparam name="T">The T type.</typeparam>
/// <typeparam name="TResult">The TResult type.</typeparam>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
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
    /// Stores state for the signal implementation.
    /// </summary>
    private TResult? _buffer;

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
                    // Reset buffer.
                    buffer = CreateBuffer();
                    _buffer = buffer;
                }

                // Take while not skipping
                if (idx >= 0)
                {
                    buffer?.Add(next);
                }

                if (++idx == _count)
                {
                    _buffer = null;

                    // Set the skip.
                    idx = 0 - _skip;
                    OnNext(buffer!);
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
                _buffer = null;

                if (buffer != null)
                {
                    OnNext(buffer);
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
        _buffer = null;

        if (buffer != null)
        {
            OnNext(buffer);
        }

        _subscription?.Dispose();
        _subscription = null;
        base.Dispose(disposing);
    }

    /// <summary>
    /// Executes the CreateBuffer operation.
    /// </summary>
    /// <returns>The result.</returns>
    private TResult CreateBuffer()
    {
        var buffer = new List<T>(_count);
        return (TResult)(IList<T>)buffer;
    }
}
