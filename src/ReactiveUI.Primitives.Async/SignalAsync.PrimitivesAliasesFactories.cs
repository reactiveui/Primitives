// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Async;

/// <summary>Primitives-vocabulary factory aliases for the async observable surface.</summary>
public static partial class SignalAsync
{
    /// <summary>Emits a single value.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="value">The value to emit.</param>
    /// <returns>An observable sequence that emits the single value.</returns>
    public static IObservableAsync<T> Emit<T>(T value) => Return(value);

    /// <summary>Emits a single <see cref="RxVoid"/> value.</summary>
    /// <returns>An observable sequence that emits a single <see cref="RxVoid"/> value.</returns>
    public static IObservableAsync<RxVoid> EmitRxVoid() => Return(RxVoid.Default);

    /// <summary>Completes without emitting values.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <returns>An observable sequence that completes without emitting values.</returns>
    [SuppressMessage(
        "Minor Code Smell",
        "S4018:All type parameters should be used in the parameter list to enable type inference",
        Justification = "Deliberate lack of type inference.")]
    public static IObservableAsync<T> None<T>() => Empty<T>();

    /// <summary>Completes with a failure result.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="error">The error to fail with.</param>
    /// <returns>An observable sequence that completes with the failure.</returns>
    [SuppressMessage(
        "Minor Code Smell",
        "S4018:All type parameters should be used in the parameter list to enable type inference",
        Justification = "Deliberate lack of type inference.")]
    public static IObservableAsync<T> Fail<T>(Exception error) => Throw<T>(error);

    /// <summary>Creates a finite integer sequence.</summary>
    /// <param name="start">The first integer in the sequence.</param>
    /// <param name="count">The number of integers to emit.</param>
    /// <returns>An observable sequence of the requested integers.</returns>
    public static IObservableAsync<int> Sequence(int start, int count) => Range(start, count);

    /// <summary>Creates a source from an enumerable sequence.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="values">The enumerable to convert.</param>
    /// <returns>An observable sequence emitting the enumerable's values.</returns>
    public static IObservableAsync<T> FromEnumerable<T>(IEnumerable<T> values) => values.ToAsyncSignal();

    /// <summary>Creates a source from an async enumerable sequence.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="values">The async enumerable to convert.</param>
    /// <returns>An observable sequence emitting the async enumerable's values.</returns>
    public static IObservableAsync<T> FromAsyncEnumerable<T>(IAsyncEnumerable<T> values) => values.ToAsyncSignal();

    /// <summary>Emits a single zero tick after the due time.</summary>
    /// <param name="dueTime">The delay before the tick is emitted.</param>
    /// <returns>An observable sequence that emits a single tick.</returns>
    public static IObservableAsync<long> After(TimeSpan dueTime) => Timer(dueTime);

    /// <summary>Emits first after the due time and then at each period.</summary>
    /// <param name="dueTime">The delay before the first tick is emitted.</param>
    /// <param name="period">The interval between subsequent ticks.</param>
    /// <returns>An observable sequence of periodic ticks.</returns>
    public static IObservableAsync<long> After(TimeSpan dueTime, TimeSpan period) => Timer(dueTime, period);

    /// <summary>Emits monotonically increasing ticks at the specified period.</summary>
    /// <param name="period">The interval between ticks.</param>
    /// <returns>An observable sequence of periodic ticks.</returns>
    public static IObservableAsync<long> Every(TimeSpan period) => Timer(period, period);

    /// <summary>Alias for <see cref="Every(TimeSpan)"/>.</summary>
    /// <param name="period">The interval between ticks.</param>
    /// <returns>An observable sequence of periodic ticks.</returns>
    public static IObservableAsync<long> Pulse(TimeSpan period) => Every(period);

    /// <summary>Creates a source whose subscription lifetime owns an async disposable resource.</summary>
    /// <typeparam name="TResource">The async disposable resource type.</typeparam>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="resourceFactory">The factory that asynchronously creates the resource.</param>
    /// <param name="signalFactory">The factory that creates the source from the resource.</param>
    /// <returns>An observable sequence whose subscription owns the resource.</returns>
    public static IObservableAsync<T> Use<TResource, T>(
        Func<CancellationToken, ValueTask<TResource>> resourceFactory,
        Func<TResource, IObservableAsync<T>> signalFactory)
        where TResource : IAsyncDisposable =>
        Using(resourceFactory, signalFactory);

    /// <summary>Concatenates the supplied sources.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="sources">The sources to concatenate.</param>
    /// <returns>An observable sequence that concatenates the sources.</returns>
    public static IObservableAsync<T> Chain<T>(params IObservableAsync<T>[] sources) =>
        sources.Concat();

    /// <summary>Merges the supplied sources.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="sources">The sources to merge.</param>
    /// <returns>An observable sequence that merges the sources.</returns>
    public static IObservableAsync<T> Blend<T>(params IObservableAsync<T>[] sources) =>
        sources.Merge();
}
