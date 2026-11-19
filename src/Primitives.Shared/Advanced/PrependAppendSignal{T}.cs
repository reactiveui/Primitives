// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Fuses a single prepended value and a single appended value around a source subscription.</summary>
/// <typeparam name="T">The source value type.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="prependValue">The prepended value.</param>
/// <param name="appendValue">The appended value.</param>
[System.Diagnostics.DebuggerDisplay("PrependAppendSignal: Source = {_source}")]
public sealed class PrependAppendSignal<T>(IObservable<T> source, T prependValue, T appendValue) : IInlineSignal<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source = source;

    /// <summary>The value emitted before source subscription.</summary>
    private readonly T _prependValue = prependValue;

    /// <summary>The value emitted after source completion.</summary>
    private readonly T _appendValue = appendValue;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        observer.OnNext(_prependValue);
        AppendWitness<T> sink = new(observer, _appendValue);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        onNext(_prependValue);
        AppendDelegateWitness<T> sink = new(onNext, onError, onCompleted, _appendValue);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
