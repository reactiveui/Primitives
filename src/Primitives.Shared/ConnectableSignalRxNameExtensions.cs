// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>
/// The System.Reactive names for the hot-sharing operators, so code written against Rx reads unchanged here.
/// Each one builds its hub directly rather than forwarding to its Primitives-named twin in
/// <see cref="ConnectableSignalExtensions"/>, so an alias costs nothing at the call site.
/// </summary>
public static class ConnectableSignalRxNameExtensions
{
    /// <summary>Reference-counting operators for a connectable signal source.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Connectable signal to reference count.</param>
    extension<T>(ConnectableSignal<T> source)
    {
        /// <summary>Shares a single subscription while observers are present. System.Reactive name for <c>AutoShare</c>.</summary>
        /// <returns>A reference-counted sequence.</returns>
        public IObservable<T> RefCount()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new AutoShareSignal<T>(source);
        }
    }

    /// <summary>Hot-sharing operators for an observable source sequence.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Source sequence to share or replay.</param>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Creates a connectable live signal. System.Reactive name for <c>ShareLive</c>.</summary>
        /// <returns>A connectable live signal.</returns>
        public ConnectableSignal<T> Publish()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new(source, new Signal<T>());
        }

        /// <summary>Shares source values through a live signal hub.</summary>
        /// <returns>A connectable live signal.</returns>
        [SuppressMessage(
            "Design",
            "SST2318:Members should not have identical bodies",
            Justification = "Share and Publish are the same operator under two names callers expect; forwarding one to the other would only add a hop.")]
        public ConnectableSignal<T> Share()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new(source, new Signal<T>());
        }

        /// <summary>Multicasts source values through a live hub and applies a selector.</summary>
        /// <typeparam name="TResult">The selected value type.</typeparam>
        /// <param name="selector">The selector applied to the connectable signal before it is connected.</param>
        /// <returns>A sequence returned by <paramref name="selector"/> while the source is connected.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
        public IObservable<TResult> Publish<TResult>(Func<IObservable<T>, IObservable<TResult>> selector)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(selector);

            return new PublishSelectorSignal<T, TResult>(source, selector);
        }

        /// <summary>Replays all source values through an unbounded replay hub. System.Reactive name for <c>ReplayLive</c>.</summary>
        /// <returns>A connectable replay signal.</returns>
        public ConnectableSignal<T> Replay()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new(source, new ReplaySignal<T>());
        }

        /// <summary>Replays source values through a bounded replay hub. System.Reactive name for <c>ReplayLive</c>.</summary>
        /// <param name="bufferSize">Maximum number of values to replay.</param>
        /// <returns>A connectable replay signal.</returns>
        public ConnectableSignal<T> Replay(int bufferSize)
        {
            ReplaySignal<T> hub = new(bufferSize);
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new(source, hub);
        }

        /// <summary>Replays source values through a replay hub constrained by count and time. System.Reactive name for <c>ReplayLive</c>.</summary>
        /// <param name="bufferSize">Maximum number of values to replay.</param>
        /// <param name="window">Maximum replay window.</param>
        /// <returns>A connectable replay signal.</returns>
        public ConnectableSignal<T> Replay(int bufferSize, TimeSpan window)
        {
            ReplaySignal<T> hub = new(bufferSize, window);
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new(source, hub);
        }
    }
}
