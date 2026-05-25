// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal class BufferSignal<T, TResult> : Signal<IList<T>>
    where TResult : IList<T>?
{
    private readonly int _skip;
    private readonly int _count;
    private IList<T>? _buffer;
    private int _index;
    private IDisposable? _subscription;

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
                    buffer = new List<T>();
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

    protected override void Dispose(bool disposing)
    {
        if (IsDisposed)
        {
            return;
        }

        Dispose(disposing);
        if (disposing)
        {
            var buffer = _buffer;
            _buffer = null;

            if (buffer != null)
            {
                OnNext(buffer);
            }

            _subscription?.Dispose();
            _subscription = null;
        }
    }
}
