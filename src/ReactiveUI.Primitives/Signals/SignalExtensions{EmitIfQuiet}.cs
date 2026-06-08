// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Throttling extension operators.</summary>
public static partial class SignalExtensions
{
    /// <summary>Throttling operators for an observable source sequence.</summary>
    /// <param name="source">The source signal.</param>
    /// <typeparam name="TSource">The source value type.</typeparam>
    extension<TSource>(IObservable<TSource> source)
    {
        /// <summary>Emits only the latest value after a quiet period using the default sequencer.</summary>
        /// <param name="dueTime">The quiet period before the latest value is emitted.</param>
        /// <returns>A throttled signal.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<TSource> EmitIfQuiet(TimeSpan dueTime) =>
            source.EmitIfQuiet(dueTime, Sequencer.Default);

        /// <summary>Emits only the latest value after a quiet period.</summary>
        /// <param name="dueTime">The quiet period before the latest value is emitted.</param>
        /// <param name="sequencer">The sequencer used to schedule delayed emissions.</param>
        /// <returns>A throttled signal.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="sequencer"/> is <see langword="null"/>.</exception>
        public IObservable<TSource> EmitIfQuiet(
            TimeSpan dueTime,
            ISequencer sequencer)
        {
            if (source is null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            if (sequencer is null)
            {
                throw new ArgumentNullException(nameof(sequencer));
            }

            if (dueTime <= TimeSpan.Zero)
            {
                return source;
            }

            return Signal.Create<TSource>(observer =>
                new Signal.EmitIfQuietCoordinator<TSource>(observer, dueTime, sequencer).Subscribe(source));
        }
    }
}
