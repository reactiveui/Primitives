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

/// <summary>Factory methods that build signals from subscribe functions and deferred sources.</summary>
public static partial class Signal
{
    /// <summary>Runs the subscription factory for each observer and preserves the subscription when a downstream OnNext throws.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="subscribe">Invoked for each observer; returns the disposable that releases the subscription.</param>
    /// <returns>A signal backed by <paramref name="subscribe"/>.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="subscribe" /> is <c>null</c>.</exception>
    public static IObservable<T> Create<T>(Func<IObserver<T>, IDisposable> subscribe)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscribe);

        return new CreateSignal<T>(subscribe);
    }

    /// <summary>Creates an observable from an asynchronous subscription function.</summary>
    /// <typeparam name="T">The element type of the created sequence.</typeparam>
    /// <param name="subscribe">The asynchronous subscription function.</param>
    /// <returns>An observable sequence backed by the asynchronous subscription.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="subscribe"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create<T>(Func<IObserver<T>, Task<IDisposable>> subscribe)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscribe);

        return new AsyncCreateSignal<T>(subscribe);
    }

    /// <summary>Creates an observable from a cancellable asynchronous subscription function.</summary>
    /// <typeparam name="T">The element type of the created sequence.</typeparam>
    /// <param name="subscribe">The asynchronous subscription function.</param>
    /// <returns>An observable sequence backed by the asynchronous subscription.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="subscribe"/> is <see langword="null"/>.</exception>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2318:Members should not have identical bodies",
        Justification = "The overloads take different delegate types and cannot forward to one another.")]
    public static IObservable<T> Create<T>(Func<IObserver<T>, CancellationToken, Task<IDisposable>> subscribe)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscribe);

        return new AsyncCreateSignal<T>(subscribe);
    }

    /// <summary>Runs the subscription factory for each observer and preserves the subscription when a downstream OnNext throws.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="subscribe">Invoked for each observer; returns the disposable that releases the subscription.</param>
    /// <param name="isRequiredSubscribeOnCurrentThread">Whether subscription must be dispatched through the current-thread sequencer.</param>
    /// <returns>A signal backed by <paramref name="subscribe"/>.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="subscribe" /> is <c>null</c>.</exception>
    public static IObservable<T> Create<T>(
        Func<IObserver<T>, IDisposable> subscribe,
        bool isRequiredSubscribeOnCurrentThread)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscribe);

        return new CreateSignal<T>(subscribe, isRequiredSubscribeOnCurrentThread);
    }

    /// <summary>Creates a signal that passes the state to the subscribe function for each observer, so the function can be static instead of capturing a closure.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <typeparam name="TState">The type of the captured state.</typeparam>
    /// <param name="state">The state passed to <paramref name="subscribe"/> on each subscription.</param>
    /// <param name="subscribe">Invoked for each observer; returns the disposable that releases the subscription.</param>
    /// <returns>A signal backed by <paramref name="subscribe"/>.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="subscribe" /> is <c>null</c>.</exception>
    public static IObservable<T> CreateWithState<T, TState>(
        TState state,
        Func<TState, IObserver<T>, IDisposable> subscribe)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscribe);

        return new CreateSignal<T, TState>(state, subscribe);
    }

    /// <summary>Creates a signal that passes the state to the subscribe function for each observer, so the function can be static instead of capturing a closure.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <typeparam name="TState">The type of the captured state.</typeparam>
    /// <param name="state">The state passed to <paramref name="subscribe"/> on each subscription.</param>
    /// <param name="subscribe">Invoked for each observer; returns the disposable that releases the subscription.</param>
    /// <param name="isRequiredSubscribeOnCurrentThread">Whether subscription must be dispatched through the current-thread sequencer.</param>
    /// <returns>A signal backed by <paramref name="subscribe"/>.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="subscribe" /> is <c>null</c>.</exception>
    public static IObservable<T> CreateWithState<T, TState>(
        TState state,
        Func<TState, IObserver<T>, IDisposable> subscribe,
        bool isRequiredSubscribeOnCurrentThread)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscribe);

        return new CreateSignal<T, TState>(state, subscribe, isRequiredSubscribeOnCurrentThread);
    }

    /// <summary>Creates a signal that runs the subscribe function for each observer and releases the subscription when a downstream <c>OnNext</c> throws, which suits cold signals.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="subscribe">Invoked for each observer; returns the disposable that releases the subscription.</param>
    /// <returns>A signal backed by <paramref name="subscribe"/>.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="subscribe" /> is <c>null</c>.</exception>
    public static IObservable<T> CreateSafe<T>(Func<IObserver<T>, IDisposable> subscribe)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscribe);

        return new CreateSafeSignal<T>(subscribe);
    }

    /// <summary>Creates a signal that runs the subscribe function for each observer and releases the subscription when a downstream <c>OnNext</c> throws, which suits cold signals.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="subscribe">Invoked for each observer; returns the disposable that releases the subscription.</param>
    /// <param name="isRequiredSubscribeOnCurrentThread">Whether subscription must be dispatched through the current-thread sequencer.</param>
    /// <returns>A signal backed by <paramref name="subscribe"/>.</returns>
    /// <exception cref="ArgumentExceptionHelper"><paramref name="subscribe" /> is <c>null</c>.</exception>
    public static IObservable<T> CreateSafe<T>(
        Func<IObserver<T>, IDisposable> subscribe,
        bool isRequiredSubscribeOnCurrentThread)
    {
        ArgumentExceptionHelper.ThrowIfNull(subscribe);

        return new CreateSafeSignal<T>(subscribe, isRequiredSubscribeOnCurrentThread);
    }

    /// <summary>Lazily creates the source sequence for each subscription.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="observableFactory">Invoked once per subscription to build the source.</param>
    /// <returns>A signal that subscribes to the factory-produced source for each observer.</returns>
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
        Justification = "The overloads take different delegate types and cannot forward to one another.")]
    public static IObservable<T> Defer<T>(Func<CancellationToken, Task<IObservable<T>>> observableFactory)
    {
        ArgumentExceptionHelper.ThrowIfNull(observableFactory);

        return new AsyncDeferSignal<T>(observableFactory);
    }
}
