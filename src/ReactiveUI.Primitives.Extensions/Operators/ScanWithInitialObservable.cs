// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions;

/// <summary>
/// Scans the source sequence and emits the initial value immediately upon subscription.
/// </summary>
/// <typeparam name="TSource">The type of elements in the source sequence.</typeparam>
/// <typeparam name="TAccumulate">The type of the accumulated value.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="initial">The initial accumulated value.</param>
/// <param name="accumulator">The accumulator function.</param>
internal sealed class ScanWithInitialObservable<TSource, TAccumulate>(
    IObservable<TSource> source,
    TAccumulate initial,
    Func<TAccumulate, TSource, TAccumulate> accumulator) : IObservable<TAccumulate>
{
    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<TAccumulate> observer)
    {
        InvalidOperationExceptionHelper.ThrowIfNull(source);
        InvalidOperationExceptionHelper.ThrowIfNull(accumulator);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        var sink = new ScanWithInitialSink(observer, initial, accumulator);
        sink.Initialize();
        return source.Subscribe(sink);
    }

    /// <summary>
    /// Sink that implements the scan with initial logic.
    /// </summary>
    /// <param name="downstream">The observer to forward elements to.</param>
    /// <param name="initial">The initial accumulated value.</param>
    /// <param name="accumulator">The accumulator function.</param>
    private sealed class ScanWithInitialSink(
        IObserver<TAccumulate> downstream,
        TAccumulate initial,
        Func<TAccumulate, TSource, TAccumulate> accumulator) : IObserver<TSource>
    {
#if NET9_0_OR_GREATER
        /// <summary>
        /// The gate to synchronize access to the sink state.
        /// </summary>
        private readonly Lock _gate = new();
#else
        /// <summary>
        /// The gate to synchronize access to the sink state.
        /// </summary>
        private readonly object _gate = new();
#endif

        /// <summary>
        /// The current accumulated value.
        /// </summary>
        private TAccumulate _current = initial;

        /// <summary>
        /// Whether the sink has finished.
        /// </summary>
        private bool _done;

        /// <summary>
        /// Initializes the sink by emitting the initial value.
        /// </summary>
        public void Initialize() => downstream.OnNext(_current);

        /// <inheritdoc/>
        public void OnNext(TSource value)
        {
            TAccumulate current;
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                try
                {
                    _current = accumulator(_current, value);
                    current = _current;
                }
                catch (Exception ex)
                {
                    _done = true;
                    downstream.OnError(ex);
                    return;
                }
            }

            downstream.OnNext(current);
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
            }

            downstream.OnError(error);
        }

        /// <inheritdoc/>
        public void OnCompleted()
        {
            lock (_gate)
            {
                if (_done)
                {
                    return;
                }

                _done = true;
            }

            downstream.OnCompleted();
        }
    }
}
