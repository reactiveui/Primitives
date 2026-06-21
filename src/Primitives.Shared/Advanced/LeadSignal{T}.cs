// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Signal that emits one leading value before subscribing to the source.</summary>
/// <typeparam name="T">The source value type.</typeparam>
public sealed class LeadSignal<T> : IInlineSignal<T>
{
    /// <summary>Initializes a new instance of the <see cref="LeadSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="value">The leading value.</param>
    public LeadSignal(IObservable<T> source, T value)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        Source = source;
        Value = value;
    }

    /// <summary>Gets the source observable.</summary>
    private IObservable<T> Source { get; }

    /// <summary>Gets the leading value.</summary>
    private T Value { get; }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        observer.OnNext(Value);
        return Source.Subscribe(observer);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        ArgumentExceptionHelper.ThrowIfNull(onNext);

        onNext(Value);
        return Source.Subscribe(onNext, onError, onCompleted);
    }
}
