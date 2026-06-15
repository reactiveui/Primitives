// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Signals;
#else
namespace ReactiveUI.Primitives.Signals;
#endif

/// <summary>Awaiter extension operators.</summary>
public static partial class SignalExtensions
{
    /// <summary>Awaiter operators for an observable source sequence.</summary>
    /// <param name="source">Source sequence to await.</param>
    /// <typeparam name="TSource">The type of the source.</typeparam>
    extension<TSource>(IObservable<TSource> source)
    {
        /// <summary>
        /// Gets an awaiter that returns the last value of the observable sequence or throws an exception if the sequence is empty.
        /// This operation subscribes to the observable sequence, making it hot.
        /// </summary>
        /// <returns>A final signal awaiter.</returns>
        /// <exception cref="ArgumentExceptionHelper">source.</exception>
        public IAwaitSignal<TSource> GetAwaiter()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return Signal.RunAsync(source, CancellationToken.None);
        }

        /// <summary>
        /// Gets an awaiter that returns the last value of the observable sequence or throws an exception if the sequence is empty.
        /// This operation subscribes to the observable sequence, making it hot.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>
        /// A final signal awaiter.
        /// </returns>
        /// <exception cref="ArgumentExceptionHelper">source.</exception>
        public IAwaitSignal<TSource> GetAwaiter(CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return Signal.RunAsync(source, cancellationToken);
        }
    }
}
