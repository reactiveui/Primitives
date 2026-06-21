// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Pass-through signal that invokes side effects for forwarded notifications.</summary>
/// <typeparam name="T">The value type.</typeparam>
public sealed class TapSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="TapSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="onNext">The value side effect.</param>
    /// <param name="onError">The error side effect.</param>
    /// <param name="onCompleted">The completion side effect.</param>
    public TapSignal(IObservable<T> source, Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
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

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        TapWitness<T> sink = new(observer, OnNextAction, OnErrorAction, OnCompletedAction);
        sink.SetSubscription(Source.Subscribe(sink));
        return sink;
    }
}
