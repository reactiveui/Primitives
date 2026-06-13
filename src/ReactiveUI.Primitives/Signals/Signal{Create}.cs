// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals.Core;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Create Signals functionality.</summary>
public static partial class Signal
{
    /// <summary>
    /// Create anonymous Signals. Observer has exception durability.
    /// This is recommended for make operator and event, generating a HotSignals.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="subscribe">The subscribe.</param>
    /// <returns>An Signals.</returns>
    /// <exception cref="ArgumentExceptionHelper">subscribe.</exception>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="subscribe" /> is <c>null</c>.</exception>
    public static IObservable<T> Create<T>(Func<IObserver<T>, IDisposable> subscribe)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscribe);

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
    /// <exception cref="ArgumentExceptionHelper">subscribe.</exception>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="subscribe" /> is <c>null</c>.</exception>
    public static IObservable<T> Create<T>(Func<IObserver<T>, IDisposable> subscribe, bool isRequiredSubscribeOnCurrentThread)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscribe);

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
    /// <exception cref="ArgumentExceptionHelper">subscribe.</exception>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="subscribe" /> is <c>null</c>.</exception>
    public static IObservable<T> CreateWithState<T, TState>(TState state, Func<TState, IObserver<T>, IDisposable> subscribe)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscribe);

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
    /// <exception cref="ArgumentExceptionHelper">subscribe.</exception>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="subscribe" /> is <c>null</c>.</exception>
    public static IObservable<T> CreateWithState<T, TState>(TState state, Func<TState, IObserver<T>, IDisposable> subscribe, bool isRequiredSubscribeOnCurrentThread)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscribe);

        return new CreateSignal<T, TState>(state, subscribe, isRequiredSubscribeOnCurrentThread);
    }

    /// <summary>
    /// Create anonymous Signals. Safe means auto detach when error raised in onNext pipeline.
    /// This is recommended for making a ColdSignals.
    /// </summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="subscribe">The subscribe.</param>
    /// <returns>An Signals.</returns>
    /// <exception cref="ArgumentExceptionHelper">subscribe.</exception>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="subscribe" /> is <c>null</c>.</exception>
    public static IObservable<T> CreateSafe<T>(Func<IObserver<T>, IDisposable> subscribe)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscribe);

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
    /// <exception cref="ArgumentExceptionHelper">subscribe.</exception>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="subscribe" /> is <c>null</c>.</exception>
    public static IObservable<T> CreateSafe<T>(Func<IObserver<T>, IDisposable> subscribe, bool isRequiredSubscribeOnCurrentThread)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscribe);

        return new CreateSafeSignal<T>(subscribe, isRequiredSubscribeOnCurrentThread);
    }

    /// <summary>Lazily creates the source sequence for each subscription.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="observableFactory">The observable factory.</param>
    /// <returns>An Observable.</returns>
    public static IObservable<T> Lazy<T>(Func<IObservable<T>> observableFactory)
    {
        ArgumentExceptionHelper.ThrowIfNull(observableFactory);

        return new DeferSignal<T>(observableFactory);
    }

    /// <summary>Creates a signal whose source is produced separately for each subscription.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="observableFactory">The factory that creates the source signal for a subscription.</param>
    /// <returns>A signal that subscribes to the factory-produced source for each observer.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="observableFactory"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Defer<T>(Func<IObservable<T>> observableFactory)
    {
        ArgumentExceptionHelper.ThrowIfNull(observableFactory);

        return Create<T>(observer =>
        {
            IObservable<T> source;
            try
            {
                source = observableFactory();
            }
            catch (Exception ex)
            {
                observer.OnError(ex);
                return Scope.Empty;
            }

            return source.Subscribe(observer);
        });
    }
}
