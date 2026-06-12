// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Scheduling and materialization extension operators.</summary>
public static partial class SignalExtensions
{
    /// <summary>Scheduling and materialization operators for an observable source sequence.</summary>
    /// <param name="source">The source signal.</param>
    /// <typeparam name="T">The value type.</typeparam>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Witnesses the on.</summary>
        /// <param name="scheduler">The scheduler.</param>
        /// <returns>An Observable.</returns>
        public IObservable<T> WitnessOn(ISequencer scheduler) =>
            new WitnessOnSignal<T>(source, scheduler);

        /// <summary>Blocks until the signal completes and returns the observed values.</summary>
        /// <returns>The values observed before completion.</returns>
        /// <exception cref="ArgumentExceptionHelper"><paramref name="source"/> is <see langword="null"/>.</exception>
        /// <exception cref="Exception">Rethrows the source error if the signal terminates with an error.</exception>
        public IEnumerable<T> ToEnumerable()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            var values = new List<T>();
            Exception? error = null;
            using var completed = new ManualResetEventSlim();
            using var subscription = source.Subscribe(
                values.Add,
                ex =>
                {
                    error = ex;
                    completed.Set();
                },
                completed.Set);

            completed.Wait();

            if (error is not null)
            {
                ExceptionDispatchInfo.Capture(error).Throw();
            }

            return values;
        }
    }
}
