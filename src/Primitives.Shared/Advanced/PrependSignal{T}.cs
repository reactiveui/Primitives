// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Emits one value, then the source sequence.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("PrependSignal: Value = {_value}, Source = {_source}")]
public sealed class PrependSignal<T> : IInlineSignal<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The value emitted before source subscription.</summary>
    private readonly T _value;

    /// <summary>Initializes a new instance of the <see cref="PrependSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="value">The prepended value.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public PrependSignal(IObservable<T> source, T value)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        _source = source;
        _value = value;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        observer.OnNext(_value);
        return _source.Subscribe(observer);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        onNext(_value);
        return _source.Subscribe(onNext, onError, onCompleted);
    }

    /// <summary>Gets the source observable for operator fusion.</summary>
    /// <returns>The source observable.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal IObservable<T> GetSource() => _source;

    /// <summary>Gets the prepended value for operator fusion.</summary>
    /// <returns>The prepended value.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal T GetValue() => _value;
}
