// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>Throttling extension operators.</summary>
public static partial class SignalExtensions
{
    /// <summary>Throttling operators for an observable source sequence.</summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <param name="source">The source signal.</param>
    extension<TSource>(IObservable<TSource> source)
    {
        /// <summary>Emits only the latest value after a quiet period using the default sequencer.</summary>
        /// <param name="dueTime">The quiet period before the latest value is emitted.</param>
        /// <returns>A throttled signal.</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="source"/> is <see langword="null"/>.</exception>
        public IObservable<TSource> EmitIfQuiet(TimeSpan dueTime)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return dueTime <= TimeSpan.Zero
                ? source
                : new EmitIfQuietSignal<TSource>(source, dueTime, Sequencer.Default);
        }

        /// <summary>Emits only the latest value after a quiet period.</summary>
        /// <param name="dueTime">The quiet period before the latest value is emitted.</param>
        /// <param name="sequencer">The sequencer used to schedule delayed emissions.</param>
        /// <returns>A throttled signal.</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="source"/> or <paramref name="sequencer"/> is <see langword="null"/>.</exception>
        public IObservable<TSource> EmitIfQuiet(
            TimeSpan dueTime,
            ISequencer sequencer)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(sequencer);

            return dueTime <= TimeSpan.Zero
                ? source
                : new EmitIfQuietSignal<TSource>(source, dueTime, sequencer);
        }
    }
}
