// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#pragma warning disable SA1116, SA1117, SA1204, SA1402, SA1501, SA1611, SA1615, SA1618

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Read-only latest-value signal for projected or externally owned state.
/// </summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
public sealed class ReadOnlyState<T> : IObservable<T>, IDisposable
{
    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly StateSignal<T> _inner;

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly IDisposable _subscription;

    /// <summary>
    /// Initializes a new instance of the <see cref="ReadOnlyState{T}"/> class.
    /// </summary>
    /// <param name="source">The source values to mirror.</param>
    /// <param name="initialValue">The current value before source notifications arrive.</param>
    public ReadOnlyState(IObservable<T> source, T initialValue)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        _inner = new StateSignal<T>(initialValue);
        _subscription = source.Subscribe(_inner);
    }

    /// <summary>
    /// Gets the current value.
    /// </summary>
    public T Value => _inner.Value;

    /// <summary>
    /// Gets the stream of current and subsequent values.
    /// </summary>
    public IObservable<T> Changed => _inner;

    /// <summary>
    /// Executes the Subscribe operation.
    /// </summary>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(IObserver<T> observer) => _inner.Subscribe(observer);

    /// <summary>
    /// Executes the Dispose operation.
    /// </summary>
    public void Dispose()
    {
        _subscription.Dispose();
        _inner.Dispose();
    }
}

/// <summary>
/// State projection helpers.
/// </summary>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
public static class StateSignalMixins
{
    /// <summary>
    /// Projects an observable sequence into a read-only state signal.
    /// </summary>
    /// <typeparam name="TSource">The source value type.</typeparam>
    /// <typeparam name="TResult">The projected value type.</typeparam>
    /// <param name="source">The source sequence.</param>
    /// <param name="initialValue">The initial projected value.</param>
    /// <param name="selector">The projection function.</param>
    /// <returns>A read-only projected state.</returns>
    public static ReadOnlyState<TResult> ToReadOnlyState<TSource, TResult>(
        this IObservable<TSource> source,
        TResult initialValue,
        Func<TSource, TResult> selector)
    {
        if (source == null)
        {
            throw new ArgumentNullException(nameof(source));
        }

        if (selector == null)
        {
            throw new ArgumentNullException(nameof(selector));
        }

        return new ReadOnlyState<TResult>(
            ReactiveUI.Primitives.Signals.Signal.CreateSafe<TResult>(
                observer => source.Subscribe(
                    value => observer.OnNext(selector(value)),
                    observer.OnError,
                    observer.OnCompleted)),
            initialValue);
    }
}
