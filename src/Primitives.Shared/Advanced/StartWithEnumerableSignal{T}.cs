// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Emits the values of an enumerable, then the source sequence.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("StartWithEnumerableSignal: Values = {_values}, Source = {_source}")]
public sealed class StartWithEnumerableSignal<T> : IInlineSignal<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>Values emitted before source subscription.</summary>
    private readonly IEnumerable<T> _values;

    /// <summary>Initializes a new instance of the <see cref="StartWithEnumerableSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="values">Values emitted before source subscription.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="values"/> is <see langword="null"/>.</exception>
    public StartWithEnumerableSignal(IObservable<T> source, IEnumerable<T> values)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(values);

        _source = source;
        _values = values;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        foreach (var value in _values)
        {
            observer.OnNext(value);
        }

        return _source.Subscribe(observer);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        foreach (var value in _values)
        {
            onNext(value);
        }

        return _source.Subscribe(onNext, onError, onCompleted);
    }
}
