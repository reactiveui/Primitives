// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>
/// Emits (previous, current) pairs from a sequence.
/// </summary>
/// <typeparam name="T">The type of elements in the source sequence.</typeparam>
/// <param name="source">The source observable.</param>
internal sealed class PairwiseObservable<T>(IObservable<T> source) : IObservable<(T Previous, T Current)>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<(T Previous, T Current)> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return source.Subscribe(new PairwiseObserver(observer));
    }

    /// <summary>
    /// The observer for the pairwise operator.
    /// </summary>
    /// <param name="downstream">The downstream observer.</param>
    private sealed class PairwiseObserver(IObserver<(T Previous, T Current)> downstream) : IObserver<T>
    {
#if NET9_0_OR_GREATER
        /// <summary>
        /// The gate for state access.
        /// </summary>
        private readonly Lock _gate = new();
#else
        /// <summary>
        /// The gate for state access.
        /// </summary>
        private readonly object _gate = new();
#endif

        /// <summary>
        /// The previous value.
        /// </summary>
        private T? _previous;

        /// <summary>
        /// A value indicating whether there is a previous value.
        /// </summary>
        private bool _hasPrevious;

        /// <inheritdoc/>
        public void OnNext(T value)
        {
            lock (_gate)
            {
                if (_hasPrevious)
                {
                    downstream.OnNext((_previous!, value));
                }

                _previous = value;
                _hasPrevious = true;
            }
        }

        /// <inheritdoc/>
        public void OnError(Exception error) => downstream.OnError(error);

        /// <inheritdoc/>
        public void OnCompleted() => downstream.OnCompleted();
    }
}
