// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> AutoShare() => new AutoShareSignal<T>(source);

        /// <summary>Connects on the first observer subscription.</summary>
        /// <returns>A sequence that connects after the first subscription.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> AutoConnect() => new AutoConnectSignal<T>(source);

        /// <summary>Connects after <paramref name="subscriberCount"/> observers have subscribed.</summary>
        /// <param name="subscriberCount">Number of observers required before connecting.</param>
        /// <returns>A sequence that connects after the requested number of subscriptions.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> AutoConnect(int subscriberCount) => new AutoConnectSignal<T>(source, subscriberCount);

        /// <summary>Connects after <paramref name="subscriberCount"/> observers have subscribed and reports the connection.</summary>
        /// <param name="subscriberCount">Number of observers required before connecting.</param>
        /// <param name="onConnect">Action invoked with the connection disposable when the source connects.</param>
        /// <returns>A sequence that connects after the requested number of subscriptions.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> AutoConnect(int subscriberCount, Action<IDisposable> onConnect) =>
            AutoConnectSignal<T>.Create(source, subscriberCount, onConnect);
    }

    /// <summary>Hot-sharing operators for an observable source sequence.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="source">Source sequence to multicast, share, or replay.</param>
    extension<T>(IObservable<T> source)
    {
        /// <summary>Multicasts source values through the supplied hub.</summary>
        /// <param name="hub">Hub that receives source values.</param>
        /// <returns>A connectable signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ConnectableSignal<T> Multicast(ISignal<T> hub) => new(source, hub);

        /// <summary>Shares source values through a live signal hub.</summary>
        /// <returns>A connectable live signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ConnectableSignal<T> ShareLive() => AutoShareSignal<T>.CreateConnectable(source);

        /// <summary>Replays all source values through an unbounded replay hub.</summary>
        /// <returns>A connectable replay signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ConnectableSignal<T> ReplayLive() => ReplaySignal<T>.CreateConnectable(source);

        /// <summary>Replays source values through a bounded replay hub.</summary>
        /// <param name="bufferSize">Maximum number of values to replay.</param>
        /// <returns>A connectable replay signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ConnectableSignal<T> ReplayLive(int bufferSize) => ReplaySignal<T>.CreateConnectable(source, bufferSize);

        /// <summary>Replays source values through a replay hub constrained by count and time.</summary>
        /// <param name="bufferSize">Maximum number of values to replay.</param>
        /// <param name="window">Maximum replay window.</param>
        /// <returns>A connectable replay signal.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ConnectableSignal<T> ReplayLive(int bufferSize, TimeSpan window) =>
            ReplaySignal<T>.CreateConnectable(source, bufferSize, window);

        /// <summary>Shares one live source subscription while at least one observer is subscribed.</summary>
        /// <returns>A reference-counted live sequence.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IObservable<T> ShareLatest() => AutoShareSignal<T>.CreateShareLatest(source);
    }
}
