// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Signal that materializes source notifications into <see cref="Spark{T}"/> values.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("SparkSignal: Source = {Source}")]
public sealed class SparkSignal<T> : IObservable<Spark<T>>
{
    /// <summary>Initializes a new instance of the <see cref="SparkSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    public SparkSignal(IObservable<T> source) => Source = source;

    /// <summary>Gets the source observable.</summary>
    private IObservable<T> Source { get; }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<Spark<T>> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SparkWitness<T> sink = new(observer);
        sink.SetSubscription(Source.Subscribe(sink));
        return sink;
    }
}
