// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

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
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public SparkSignal(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        Source = source;
    }

    /// <summary>Gets the source observable for dematerialization fusion.</summary>
    internal IObservable<T> InnerSource => Source;

    /// <summary>Gets the source observable.</summary>
    private IObservable<T> Source { get; }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<Spark<T>> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (typeof(T) == typeof(int) && Source is RangeSignal range)
        {
            EmitRange(range, observer);
            return EmptyDisposable.Instance;
        }

        if (Source is FromEnumerableSignal<T> enumerable && enumerable.TryGetReadOnlyValues(out var values))
        {
            EmitReadOnlyList(values, observer);
            return EmptyDisposable.Instance;
        }

        SparkWitness<T> sink = new(observer);
        sink.SetSubscription(Source.Subscribe(sink));
        return sink;
    }

    /// <summary>Materializes a synchronous integer range straight into <paramref name="observer"/>.</summary>
    /// <param name="range">The range source.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitRange(RangeSignal range, IObserver<Spark<T>> observer)
    {
        var intObserver = Unsafe.As<IObserver<Spark<int>>>(observer);
        var start = range.Start;
        var count = range.Count;
        for (var i = 0; i < count; i++)
        {
            intObserver.OnNext(Spark.CreateOnNext(start + i));
        }

        observer.OnNext(Spark.CreateOnCompleted<T>());
        observer.OnCompleted();
    }

    /// <summary>Materializes an indexable synchronous list straight into <paramref name="observer"/>.</summary>
    /// <param name="values">The indexable values.</param>
    /// <param name="observer">The downstream observer.</param>
    private static void EmitReadOnlyList(IReadOnlyList<T> values, IObserver<Spark<T>> observer)
    {
        for (var i = 0; i < values.Count; i++)
        {
            observer.OnNext(Spark.CreateOnNext(values[i]));
        }

        observer.OnNext(Spark.CreateOnCompleted<T>());
        observer.OnCompleted();
    }
}
