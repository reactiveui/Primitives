// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observer that turns downstream <c>OnNext</c> exceptions into a terminal error and upstream disposal.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("SubscribeSafeWitness: Stopped = {_stopped}, Observer = {_observer}")]
public sealed class SubscribeSafeWitness<T> : IObserver<T>, IDisposable
{
    /// <summary>The wrapped observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>The upstream subscription.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "Disposed atomically via WitnessLifetime.Dispose.")]
    private IDisposable? _subscription;

    /// <summary>Non-zero after terminal notification or disposal.</summary>
    private int _stopped;

    /// <summary>Initializes a new instance of the <see cref="SubscribeSafeWitness{T}"/> class.</summary>
    /// <param name="observer">The wrapped observer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> is <see langword="null"/>.</exception>
    public SubscribeSafeWitness(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        _observer = observer;
    }

    /// <summary>Subscribes <paramref name="observer"/> to <paramref name="source"/> with downstream exception protection.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="observer">The observer to subscribe.</param>
    /// <returns>The witness subscription.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="observer"/> is <see langword="null"/>.</exception>
    public static SubscribeSafeWitness<T> Subscribe(IObservable<T> source, IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SubscribeSafeWitness<T> safe = new(observer);
        safe.SetSubscription(source.Subscribe(safe));
        return safe;
    }

    /// <summary>Subscribes <paramref name="onNext"/> to <paramref name="source"/> with downstream exception protection.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The value callback.</param>
    /// <returns>The witness subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SubscribeSafeWitness<T> Subscribe(IObservable<T> source, Action<T> onNext) =>
        Subscribe(source, Witness.Create(onNext));

    /// <summary>Subscribes callbacks to <paramref name="source"/> with downstream exception protection.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The value callback.</param>
    /// <param name="onError">The error callback.</param>
    /// <returns>The witness subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SubscribeSafeWitness<T> Subscribe(
        IObservable<T> source,
        Action<T> onNext,
        Action<Exception> onError) =>
        Subscribe(source, Witness.Create(onNext, onError));

    /// <summary>Subscribes callbacks to <paramref name="source"/> with downstream exception protection.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="onNext">The value callback.</param>
    /// <param name="onError">The error callback.</param>
    /// <param name="onCompleted">The completion callback.</param>
    /// <returns>The witness subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SubscribeSafeWitness<T> Subscribe(
        IObservable<T> source,
        Action<T> onNext,
        Action<Exception> onError,
        Action onCompleted) =>
        Subscribe(source, Witness.Create(onNext, onError, onCompleted));

    /// <summary>Subscribes an error callback to <paramref name="source"/> with downstream exception protection.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The error callback.</param>
    /// <returns>The witness subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SubscribeSafeWitness<T> SubscribeErrors(IObservable<T> source, Action<Exception> onError) =>
        Subscribe(source, Witness.Create<T>(static _ => { }, onError));

    /// <summary>Subscribes error and completion callbacks to <paramref name="source"/> with downstream exception protection.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="onError">The error callback.</param>
    /// <param name="onCompleted">The completion callback.</param>
    /// <returns>The witness subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static SubscribeSafeWitness<T> SubscribeErrors(
        IObservable<T> source,
        Action<Exception> onError,
        Action onCompleted) =>
        Subscribe(source, Witness.Create<T>(static _ => { }, onError, onCompleted));

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => WitnessLifetime.Dispose(ref _stopped, ref _subscription);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => WitnessLifetime.Complete(ref _stopped, ref _subscription, _observer);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => WitnessLifetime.Error(ref _stopped, ref _subscription, _observer, error);

    /// <inheritdoc/>
    public void OnNext(T value)
    {
        if (WitnessLifetime.IsStopped(ref _stopped))
        {
            return;
        }

        try
        {
            _observer.OnNext(value);
        }
        catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
        {
            WitnessLifetime.Error(ref _stopped, ref _subscription, _observer, error);
        }
    }

    /// <summary>Assigns the upstream subscription.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) =>
        WitnessLifetime.SetSubscription(ref _stopped, ref _subscription, subscription);
}
