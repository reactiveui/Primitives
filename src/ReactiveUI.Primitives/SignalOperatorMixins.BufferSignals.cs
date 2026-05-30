// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <content>
/// Dedicated cold signal/sink for <c>Buffer</c>. Each subscription gets its own lightweight sink
/// (no broadcaster, no eager subscription); windows are buffered into a single array of the known
/// window size and emitted directly, with only a partial trailing window copied to an exact size.
/// </content>
public static partial class LinqMixins
{
    /// <summary>Dedicated cold signal for <c>Buffer</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class BufferCountSignal<T> : IObservable<IList<T>>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The window size.</summary>
        private readonly int _count;

        /// <summary>The number of elements skipped between windows.</summary>
        private readonly int _skip;

        /// <summary>Initializes a new instance of the <see cref="BufferCountSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="count">The window size.</param>
        /// <param name="skip">The number of elements skipped between windows.</param>
        internal BufferCountSignal(IObservable<T> source, int count, int skip)
        {
            _source = source;
            _count = count;
            _skip = skip;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<IList<T>> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new BufferObserver<T>(observer, _count, _skip);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }

    /// <summary>Sink that batches source values into fixed-size windows.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class BufferObserver<T> : SingleSourceObserver<T>
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

        /// <summary>Initializes a new instance of the <see cref="BufferObserver{T}"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="count">The window size.</param>
        /// <param name="skip">The number of elements skipped between windows.</param>
        internal BufferObserver(IObserver<IList<T>> observer, int count, int skip)
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
            try
            {
                _observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        /// <inheritdoc/>
        public override void OnCompleted()
        {
            var buffer = _buffer;
            var length = _index;
            _buffer = null;

            try
            {
                if (buffer != null && length > 0)
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
}
