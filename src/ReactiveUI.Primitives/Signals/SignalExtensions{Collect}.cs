// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Buffered collection extension operators.</summary>
public static partial class SignalExtensions
{
    /// <summary>Time-windowed batching operators for an observable source sequence.</summary>
    /// <param name="source">The source signal to collect into batches.</param>
    /// <typeparam name="TSource">The source value type.</typeparam>
    extension<TSource>(IObservable<TSource> source)
    {
        /// <summary>Collects values into time-windowed batches using the default sequencer.</summary>
        /// <param name="timeSpan">The duration of each buffer window.</param>
        /// <returns>A signal that emits batches of source values.</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<IList<TSource>> Collect(TimeSpan timeSpan) =>
            source.Collect(timeSpan, Sequencer.Default);

        /// <summary>Collects values into time-windowed batches.</summary>
        /// <param name="timeSpan">The duration of each buffer window.</param>
        /// <param name="sequencer">The sequencer used to schedule buffer flushes.</param>
        /// <returns>A signal that emits batches of source values.</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="source"/> or <paramref name="sequencer"/> is <see langword="null"/>.</exception>
        public IObservable<IList<TSource>> Collect(
            TimeSpan timeSpan,
            ISequencer sequencer)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(sequencer);

            if (timeSpan <= TimeSpan.Zero)
            {
                return source.Map(static value => (IList<TSource>)[value]);
            }

            return Signal.Create<IList<TSource>>(observer =>
                new Signal.CollectCoordinator<TSource>(observer, timeSpan, sequencer).Subscribe(source));
        }
    }
}
