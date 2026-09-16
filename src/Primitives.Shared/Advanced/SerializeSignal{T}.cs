// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Signal that delivers source notifications one at a time without holding a lock while the observer runs.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("SerializeSignal: Source = {_source}")]
public sealed class SerializeSignal<T> : IObservable<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>Initializes a new instance of the <see cref="SerializeSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public SerializeSignal(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        _source = source;
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SerializeWitness<T> sink = new(observer);
        sink.SetSubscription(_source.Subscribe(sink));
        return sink;
    }
}
