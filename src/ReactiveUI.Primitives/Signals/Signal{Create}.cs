// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Create Signals functionality.
/// </summary>
public static partial class Signal
{
    /// <summary>
    /// Create anonymous Signals. Observer has exception durability.
    /// This is recommended for make operator and event, generating a HotSignals.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="subscribe">The subscribe.</param>
    /// <returns>An Signals.</returns>
    /// <exception cref="System.ArgumentNullException">subscribe.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="subscribe" /> is <c>null</c>.</exception>
    public static IObservable<T> Create<T>(Func<IObserver<T>, IDisposable> subscribe)
    {
        if (subscribe == null)
        {
            throw new ArgumentNullException(nameof(subscribe));
        }

        return new CreateSignal<T>(subscribe);
    }

    /// <summary>
    /// Create anonymous Signals. Observer has exception durability.
    /// This is recommended for make operator and event, generating a HotSignals.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="subscribe">The subscribe.</param>
    /// <param name="isRequiredSubscribeOnCurrentThread">if set to <c>true</c> [is required subscribe on current thread].</param>
    /// <returns>An Signals.</returns>
    /// <exception cref="System.ArgumentNullException">subscribe.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="subscribe" /> is <c>null</c>.</exception>
    public static IObservable<T> Create<T>(Func<IObserver<T>, IDisposable> subscribe, bool isRequiredSubscribeOnCurrentThread)
    {
        if (subscribe == null)
        {
            throw new ArgumentNullException(nameof(subscribe));
        }

        return new CreateSignal<T>(subscribe, isRequiredSubscribeOnCurrentThread);
    }

    /// <summary>
    /// Create anonymous Signals. Observer has exception durability.
    /// This is recommended for make operator and event, generating a HotSignals.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="state">The state.</param>
    /// <param name="subscribe">The subscribe.</param>
    /// <returns>An Signals.</returns>
    /// <exception cref="System.ArgumentNullException">subscribe.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="subscribe" /> is <c>null</c>.</exception>
    public static IObservable<T> CreateWithState<T, TState>(TState state, Func<TState, IObserver<T>, IDisposable> subscribe)
    {
        if (subscribe == null)
        {
            throw new ArgumentNullException(nameof(subscribe));
        }

        return new CreateSignal<T, TState>(state, subscribe);
    }

    /// <summary>
    /// Create anonymous Signals. Observer has exception durability.
    /// This is recommended for make operator and event, generating a HotSignals.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="state">The state.</param>
    /// <param name="subscribe">The subscribe.</param>
    /// <param name="isRequiredSubscribeOnCurrentThread">if set to <c>true</c> [is required subscribe on current thread].</param>
    /// <returns>An Signals.</returns>
    /// <exception cref="System.ArgumentNullException">subscribe.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="subscribe" /> is <c>null</c>.</exception>
    public static IObservable<T> CreateWithState<T, TState>(TState state, Func<TState, IObserver<T>, IDisposable> subscribe, bool isRequiredSubscribeOnCurrentThread)
    {
        if (subscribe == null)
        {
            throw new ArgumentNullException(nameof(subscribe));
        }

        return new CreateSignal<T, TState>(state, subscribe, isRequiredSubscribeOnCurrentThread);
    }

    /// <summary>
    /// Create anonymous Signals. Safe means auto detach when error raised in onNext pipeline.
    /// This is recommended for making a ColdSignals.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="subscribe">The subscribe.</param>
    /// <returns>An Signals.</returns>
    /// <exception cref="System.ArgumentNullException">subscribe.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="subscribe" /> is <c>null</c>.</exception>
    public static IObservable<T> CreateSafe<T>(Func<IObserver<T>, IDisposable> subscribe)
    {
        if (subscribe == null)
        {
            throw new ArgumentNullException(nameof(subscribe));
        }

        return new CreateSafeSignal<T>(subscribe);
    }

    /// <summary>
    /// Create anonymous Signals. Safe means auto detach when error raised in onNext pipeline.
    /// This is recommended for making a ColdSignals.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="subscribe">The subscribe.</param>
    /// <param name="isRequiredSubscribeOnCurrentThread">if set to <c>true</c> [is required subscribe on current thread].</param>
    /// <returns>An Observable.</returns>
    /// <exception cref="System.ArgumentNullException">subscribe.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="subscribe" /> is <c>null</c>.</exception>
    public static IObservable<T> CreateSafe<T>(Func<IObserver<T>, IDisposable> subscribe, bool isRequiredSubscribeOnCurrentThread)
    {
        if (subscribe == null)
        {
            throw new ArgumentNullException(nameof(subscribe));
        }

        return new CreateSafeSignal<T>(subscribe, isRequiredSubscribeOnCurrentThread);
    }

    /// <summary>
    /// Defers the specified observable factory.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="observableFactory">The observable factory.</param>
    /// <returns>An Observable.</returns>
    public static IObservable<T> Defer<T>(Func<IObservable<T>> observableFactory) =>
        new DeferSignal<T>(observableFactory);

    /// <summary>
    /// Witnesses the on.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="source">The source.</param>
    /// <param name="scheduler">The scheduler.</param>
    /// <returns>An Observable.</returns>
    public static IObservable<T> WitnessOn<T>(this IObservable<T> source, ISequencer scheduler) =>
        new WitnessOnSignal<T>(source, scheduler);
}
