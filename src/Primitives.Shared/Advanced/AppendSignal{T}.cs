// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Appends a single value after source completion.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("AppendSignal: Source = {_source}")]
public sealed class AppendSignal<T> : IObservable<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The value emitted after source completion.</summary>
    private readonly T _value;

    /// <summary>Initializes a new instance of the <see cref="AppendSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="value">The appended value.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public AppendSignal(IObservable<T> source, T value)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        _source = source;
        _value = value;
    }

    /// <summary>Appends <paramref name="value"/> after <paramref name="source"/>, fusing <see cref="PrependSignal{T}"/> into <see cref="PrependAppendSignal{T}"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="value">The appended value.</param>
    /// <returns>The appended observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> source, T value)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return source is PrependSignal<T> prepended
            ? new PrependAppendSignal<T>(prepended.GetSource(), prepended.GetValue(), value)
            : new AppendSignal<T>(source, value);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        AppendWitness<T> sink = new(observer, _value);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
