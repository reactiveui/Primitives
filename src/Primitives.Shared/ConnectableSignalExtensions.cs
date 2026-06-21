// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive;
#else
namespace ReactiveUI.Primitives;
#endif

/// <summary>Hot-sharing operators for Primitives connectable signals.</summary>
public static class ConnectableSignalExtensions
{
    /// <summary>Reference-counting operators for a connectable signal source.</summary>
    /// <param name="source">Connectable signal to reference count or auto-connect.</param>
    /// <typeparam name="T">The value type.</typeparam>
    extension<T>(ConnectableSignal<T> source)
    {
        /// <summary>Connects on first subscriber and disconnects when the last subscriber disposes.</summary>
        /// <returns>A reference-counted sequence.</returns>
        public IObservable<T> AutoShare()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new AutoShareSignal<T>(source);
        }

        /// <summary>Shares a single subscription while observers are present. System.Reactive name for <c>AutoShare</c>.</summary>
        /// <returns>A reference-counted sequence.</returns>
        public IObservable<T> RefCount()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return (IObservable<T>)new AutoShareSignal<T>(source);
        }

        /// <summary>Connects on the first observer subscription.</summary>
        /// <returns>A sequence that connects after the first subscription.</returns>
        public IObservable<T> AutoConnect()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new AutoConnectSignal<T>(source, 1);
        }

        /// <summary>Connects after <paramref name="subscriberCount"/> observers have subscribed.</summary>
        /// <param name="subscriberCount">Number of observers required before connecting.</param>
        /// <returns>A sequence that connects after the requested number of subscriptions.</returns>
        public IObservable<T> AutoConnect(int subscriberCount)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(subscriberCount);

            return new AutoConnectSignal<T>(source, subscriberCount);
        }
    }

    /// <summary>Hot-sharing operators for an observable source sequence.</summary>
    /// <param name="source">Source sequence to multicast, share, or replay.</param>
    /// <typeparam name="T">The value type.</typeparam>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Multicasts source values through the supplied hub.</summary>
        /// <param name="hub">Hub that receives source values.</param>
        /// <returns>A connectable signal.</returns>
        public ConnectableSignal<T> Multicast(ISignal<T> hub)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(hub);

            return new(source, hub);
        }

        /// <summary>Shares source values through a live signal hub.</summary>
        /// <returns>A connectable live signal.</returns>
        public ConnectableSignal<T> ShareLive()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new ConnectableSignal<T>(source, new Signal<T>());
        }

        /// <summary>Shares source values through a live signal hub.</summary>
        /// <returns>A connectable live signal.</returns>
        public ConnectableSignal<T> Share()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new(source, new Signal<T>());
        }

        /// <summary>Creates a connectable live signal. System.Reactive name for <c>ShareLive</c>.</summary>
        /// <returns>A connectable live signal.</returns>
        public ConnectableSignal<T> Publish()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new ConnectableSignal<T>(source, hub: new Signal<T>());
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

        /// <summary>Replays all source values through an unbounded replay hub.</summary>
        /// <returns>A connectable replay signal.</returns>
        public ConnectableSignal<T> ReplayLive()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new ConnectableSignal<T>(source, new ReplaySignal<T>());
        }

        /// <summary>Replays source values through a bounded replay hub.</summary>
        /// <param name="bufferSize">Maximum number of values to replay.</param>
        /// <returns>A connectable replay signal.</returns>
        public ConnectableSignal<T> ReplayLive(int bufferSize)
        {
            var hub = new ReplaySignal<T>(bufferSize);
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new ConnectableSignal<T>(source, hub);
        }

        /// <summary>Replays source values through a replay hub constrained by count and time.</summary>
        /// <param name="bufferSize">Maximum number of values to replay.</param>
        /// <param name="window">Maximum replay window.</param>
        /// <returns>A connectable replay signal.</returns>
        public ConnectableSignal<T> ReplayLive(int bufferSize, TimeSpan window)
        {
            var hub = new ReplaySignal<T>(bufferSize, window);
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new ConnectableSignal<T>(source, hub);
        }

        /// <summary>Replays all source values through an unbounded replay hub.</summary>
        /// <returns>A connectable replay signal.</returns>
        public ConnectableSignal<T> Replay()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new(source, new ReplaySignal<T>());
        }

        /// <summary>Replays source values through a bounded replay hub.</summary>
        /// <param name="bufferSize">Maximum number of values to replay.</param>
        /// <returns>A connectable replay signal.</returns>
        public ConnectableSignal<T> Replay(int bufferSize)
        {
            ReplaySignal<T> hub = new(bufferSize);
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new(source, hub);
        }

        /// <summary>Replays source values through a replay hub constrained by count and time.</summary>
        /// <param name="bufferSize">Maximum number of values to replay.</param>
        /// <param name="window">Maximum replay window.</param>
        /// <returns>A connectable replay signal.</returns>
        public ConnectableSignal<T> Replay(int bufferSize, TimeSpan window)
        {
            ReplaySignal<T> hub = new(bufferSize, window);
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new(source, hub);
        }

        /// <summary>Shares one live source subscription while at least one observer is subscribed.</summary>
        /// <returns>A reference-counted live sequence.</returns>
        public IObservable<T> ShareLatest()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new AutoShareSignal<T>(new ConnectableSignal<T>(source, new Signal<T>()));
        }
    }
}
