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
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Connectable signal to reference count or auto-connect.</param>
    extension<T>(ConnectableSignal<T> source)
    {
        /// <summary>Connects on first subscriber and disconnects when the last subscriber disposes.</summary>
        /// <returns>A reference-counted sequence.</returns>
        public IObservable<T> AutoShare()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new AutoShareSignal<T>(source);
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

        /// <summary>Connects after <paramref name="subscriberCount"/> observers have subscribed and reports the connection.</summary>
        /// <param name="subscriberCount">Number of observers required before connecting.</param>
        /// <param name="onConnect">Action invoked with the connection disposable when the source connects.</param>
        /// <returns>A sequence that connects after the requested number of subscriptions.</returns>
        public IObservable<T> AutoConnect(int subscriberCount, Action<IDisposable> onConnect)
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            ArgumentExceptionHelper.ThrowIfNull(onConnect);

            ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(subscriberCount);

            return new AutoConnectSignal<T>(source, subscriberCount, onConnect);
        }
    }

    /// <summary>Hot-sharing operators for an observable source sequence.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Source sequence to multicast, share, or replay.</param>
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

            return new(source, new Signal<T>());
        }

        /// <summary>Replays all source values through an unbounded replay hub.</summary>
        /// <returns>A connectable replay signal.</returns>
        public ConnectableSignal<T> ReplayLive()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new(source, new ReplaySignal<T>());
        }

        /// <summary>Replays source values through a bounded replay hub.</summary>
        /// <param name="bufferSize">Maximum number of values to replay.</param>
        /// <returns>A connectable replay signal.</returns>
        public ConnectableSignal<T> ReplayLive(int bufferSize)
        {
            var hub = new ReplaySignal<T>(bufferSize);
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new(source, hub);
        }

        /// <summary>Replays source values through a replay hub constrained by count and time.</summary>
        /// <param name="bufferSize">Maximum number of values to replay.</param>
        /// <param name="window">Maximum replay window.</param>
        /// <returns>A connectable replay signal.</returns>
        public ConnectableSignal<T> ReplayLive(int bufferSize, TimeSpan window)
        {
            var hub = new ReplaySignal<T>(bufferSize, window);
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new(source, hub);
        }

        /// <summary>Shares one live source subscription while at least one observer is subscribed.</summary>
        /// <returns>A reference-counted live sequence.</returns>
        public IObservable<T> ShareLatest()
        {
            ArgumentExceptionHelper.ThrowIfNull(source);

            return new AutoShareSignal<T>(new(source, new Signal<T>()));
        }
    }
}
