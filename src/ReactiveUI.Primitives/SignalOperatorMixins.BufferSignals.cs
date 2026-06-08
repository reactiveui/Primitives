// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives;

/// <summary>
/// Dedicated cold signal/sink for <c>Buffer</c>. Each subscription gets its own lightweight sink
/// (no broadcaster, no eager subscription); windows are buffered into a single array of the known
/// window size and emitted directly, with only a partial trailing window copied to an exact size.
/// </summary>
public static partial class LinqExtensions
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
            if (observer is null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var sink = new BufferObserver<T>(observer, _count, _skip);
            sink.SetSubscription(_source.Subscribe(sink));
            return sink;
        }
    }
}
