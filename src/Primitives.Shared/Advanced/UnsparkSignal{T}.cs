// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Signal that dematerializes <see cref="Spark{T}"/> values into ordinary notifications.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("UnsparkSignal: Source = {Source}")]
public sealed class UnsparkSignal<T> : IObservable<T>
{
    /// <summary>Initializes a new instance of the <see cref="UnsparkSignal{T}"/> class.</summary>
    /// <param name="source">The spark source.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public UnsparkSignal(IObservable<Spark<T>> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        Source = source;
    }

    /// <summary>Gets the spark source.</summary>
    private IObservable<Spark<T>> Source { get; }

    /// <summary>Dematerializes <paramref name="source"/> with empty/never reductions and <c>Spark().Unspark()</c> elision on built-in well-behaved sources.</summary>
    /// <param name="source">The spark source.</param>
    /// <returns>The dematerialized observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<Spark<T>> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        if (source is ImmutableEmptySignal<Spark<T>>)
        {
            return ImmutableEmptySignal<T>.Instance;
        }

        if (source is ImmutableNeverSignal<Spark<T>>)
        {
            return ImmutableNeverSignal<T>.Instance;
        }

        return source is SparkSignal<T>
            {
                InnerSource: RangeSignal
                    or ImmutableEmptySignal<T>
                    or ImmutableNeverSignal<T>
                    or ImmediateReturnSignal<T>
                    or FromEnumerableSignal<T>,
            } spark
            ? spark.InnerSource
            : new UnsparkSignal<T>(source);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        UnsparkWitness<T> sink = new(observer);
        sink.SetSubscription(Source.Subscribe(sink));
        return sink;
    }
}
