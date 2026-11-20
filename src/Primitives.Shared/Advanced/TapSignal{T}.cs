// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Internal;
#else
using ReactiveUI.Primitives.Internal;
#endif

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Pass-through signal that invokes side effects for forwarded notifications.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("TapSignal: Source = {Source}")]
public sealed class TapSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="TapSignal{T}"/> class for value side effects.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="onNext">The value side effect.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="onNext"/> is <see langword="null"/>.</exception>
    public TapSignal(IObservable<T> source, Action<T> onNext)
        : this(source, onNext, static _ => { }, static () => { })
    {
    }

    /// <summary>Initializes a new instance of the <see cref="TapSignal{T}"/> class for value and error side effects.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="onNext">The value side effect.</param>
    /// <param name="onError">The error side effect.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="onNext"/>, or <paramref name="onError"/> is <see langword="null"/>.</exception>
    public TapSignal(IObservable<T> source, Action<T> onNext, Action<Exception> onError)
        : this(source, onNext, onError, static () => { })
    {
    }

    /// <summary>Initializes a new instance of the <see cref="TapSignal{T}"/> class for value and completion side effects.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="onNext">The value side effect.</param>
    /// <param name="onCompleted">The completion side effect.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/>, <paramref name="onNext"/>, or <paramref name="onCompleted"/> is <see langword="null"/>.</exception>
    public TapSignal(IObservable<T> source, Action<T> onNext, Action onCompleted)
        : this(source, onNext, static _ => { }, onCompleted)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="TapSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="onNext">The value side effect.</param>
    /// <param name="onError">The error side effect.</param>
    /// <param name="onCompleted">The completion side effect.</param>
    /// <exception cref="ArgumentNullException">Any argument is <see langword="null"/>.</exception>
    public TapSignal(IObservable<T> source, Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(onNext);
        ArgumentExceptionHelper.ThrowIfNull(onError);
        ArgumentExceptionHelper.ThrowIfNull(onCompleted);

        Source = source;
        OnNextAction = onNext;
        OnErrorAction = onError;
        OnCompletedAction = onCompleted;
    }

    /// <summary>Gets the source observable.</summary>
    private IObservable<T> Source { get; }

    /// <summary>Gets the value side effect.</summary>
    private Action<T> OnNextAction { get; }

    /// <summary>Gets the error side effect.</summary>
    private Action<Exception> OnErrorAction { get; }

    /// <summary>Gets the completion side effect.</summary>
    private Action OnCompletedAction { get; }

    /// <summary>Creates a value-tapping sequence with empty/never short-circuiting.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="onNext">The value side effect.</param>
    /// <returns>The tapped observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="onNext"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> source, Action<T> onNext)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(onNext);

        return source is ImmutableEmptySignal<T> or ImmutableNeverSignal<T>
            ? source
            : new TapSignal<T>(source, onNext);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        StateCallbackPair<Action<T>, Action<Action<T>, T>> tap = new(
            OnNextAction,
            static (callback, item) => callback(item));
        if (DirectSequenceEmission.TrySubscribeTapped(Source, tap, OnCompletedAction, observer))
        {
            return EmptyDisposable.Instance;
        }

        TapWitness<T> sink = new(observer, OnNextAction, OnErrorAction, OnCompletedAction);
        sink.SetSubscription(Source.Subscribe(sink));
        return sink;
    }
}
