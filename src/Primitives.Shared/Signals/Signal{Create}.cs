// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Advanced;

namespace ReactiveUI.Primitives.Reactive.Signals;
#else
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Signals;
#endif

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

    /// <summary>Creates an observable from an asynchronous subscription function.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="subscribe">The asynchronous subscription function.</param>
    /// <returns>An observable sequence backed by the asynchronous subscription.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="subscribe"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create<T>(Func<IObserver<T>, Task<IDisposable>> subscribe)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscribe);

        return new AsyncCreateSignal<T>(subscribe);
    }

    /// <summary>Creates an observable from a cancellable asynchronous subscription function.</summary>
    /// <typeparam name="T">The type.</typeparam>
    /// <param name="subscribe">The asynchronous subscription function.</param>
    /// <returns>An observable sequence backed by the asynchronous subscription.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="subscribe"/> is <see langword="null"/>.</exception>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification =
            "A deliberate overload accepting a cancellable subscribe delegate. The body matches the non-cancellable "
            + "overload because AsyncCreateSignal<T> exposes a constructor for each delegate shape; the two take "
            + "different delegate types and cannot forward to one another.")]
    public static IObservable<T> Create<T>(Func<IObserver<T>, CancellationToken, Task<IDisposable>> subscribe)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscribe);

        return new AsyncCreateSignal<T>(subscribe);
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
    public static IObservable<T> Create<T>(
        Func<IObserver<T>, IDisposable> subscribe,
        bool isRequiredSubscribeOnCurrentThread)
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
    public static IObservable<T> CreateWithState<T, TState>(
        TState state,
        Func<TState, IObserver<T>, IDisposable> subscribe)
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
    public static IObservable<T> CreateWithState<T, TState>(
        TState state,
        Func<TState, IObserver<T>, IDisposable> subscribe,
        bool isRequiredSubscribeOnCurrentThread)
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
    public static IObservable<T> CreateSafe<T>(
        Func<IObserver<T>, IDisposable> subscribe,
        bool isRequiredSubscribeOnCurrentThread)
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

    /// <summary>Creates a signal whose source is produced asynchronously for each subscription.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="observableFactory">The asynchronous factory that creates the source signal for a subscription.</param>
    /// <returns>A signal that subscribes to the factory-produced source for each observer.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="observableFactory"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Defer<T>(Func<Task<IObservable<T>>> observableFactory)
    {
        ArgumentExceptionHelper.ThrowIfNull(observableFactory);

        return new AsyncDeferSignal<T>(observableFactory);
    }

    /// <summary>Creates a signal whose source is produced asynchronously for each subscription.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    /// <param name="observableFactory">The asynchronous factory that creates the source signal for a subscription.</param>
    /// <returns>A signal that subscribes to the factory-produced source for each observer.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="observableFactory"/> is <see langword="null"/>.</exception>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification =
            "A deliberate overload accepting a cancellable factory delegate. The body matches the non-cancellable "
            + "overload because AsyncDeferSignal<T> exposes a constructor for each delegate shape; the two take "
            + "different delegate types and cannot forward to one another.")]
    public static IObservable<T> Defer<T>(Func<CancellationToken, Task<IObservable<T>>> observableFactory)
    {
        ArgumentExceptionHelper.ThrowIfNull(observableFactory);

        return new AsyncDeferSignal<T>(observableFactory);
    }
}
