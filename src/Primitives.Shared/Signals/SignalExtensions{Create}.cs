// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>Scheduling and materialization extension operators.</summary>
public static partial class SignalExtensions
{
    /// <summary>Scheduling and materialization operators for an observable source sequence.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">The source signal.</param>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Delivers the source notifications on the supplied sequencer.</summary>
        /// <param name="scheduler">The sequencer that notifications are delivered on.</param>
        /// <returns>A signal that forwards the source on <paramref name="scheduler"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> WitnessOn(ISequencer scheduler) =>
            new WitnessOnSignal<T>(source, scheduler);

        /// <summary>Delivers only the newest waiting source value on the supplied sequencer.</summary>
        /// <param name="scheduler">The sequencer that notifications are delivered on.</param>
        /// <returns>A signal that forwards the latest source value on <paramref name="scheduler"/>.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> WitnessLatestOn(ISequencer scheduler) =>
            new WitnessLatestOnSignal<T>(source, scheduler);

        /// <summary>Blocks until the signal completes and returns the observed values.</summary>
        /// <returns>The values observed before completion.</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="Exception">Rethrows the source error if the signal terminates with an error.</exception>
        public IEnumerable<T> ToEnumerable()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            List<T> values = [];
            Exception? error = null;
            using ManualResetEventSlim completed = new();
            using var subscription = source.Subscribe(
                values.Add,
                ex =>
                {
                    error = ex;
                    completed.Set();
                },
                completed.Set);

            WaitForCompletion(completed);

            return error is null ? values : Rethrow(error, values);
        }
    }

    /// <summary>Rethrows <paramref name="error"/> with its original stack, typed by <paramref name="fallback"/> so a value-returning caller can return the call.</summary>
    /// <typeparam name="TResult">The caller's return type.</typeparam>
    /// <param name="error">The failure to rethrow.</param>
    /// <param name="fallback">The value the caller would otherwise return; never returned.</param>
    /// <returns>Never returns; the return after the throw is unreachable.</returns>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static TResult Rethrow<TResult>(Exception error, TResult fallback)
    {
        ExceptionDispatchInfo.Capture(error).Throw();
        return fallback;
    }

    /// <summary>Blocks until the source signals completion.</summary>
    /// <param name="completed">The source's completion signal.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WaitForCompletion(ManualResetEventSlim completed) => completed.Wait();
}
