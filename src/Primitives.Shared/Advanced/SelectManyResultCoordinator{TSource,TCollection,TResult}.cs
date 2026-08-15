// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Coordinates concurrent observable <c>SelectMany</c> subscriptions with a result selector.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TCollection">The inner value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Active = {Active}, OuterCompleted = {OuterCompleted}, Done = {Done}")]
public sealed class SelectManyResultCoordinator<TSource, TCollection, TResult> : IObserver<TSource>, IDisposable
{
    /// <summary>Serializes downstream callbacks and counters.</summary>
    private readonly Lock _gate = new();

    /// <summary>Initializes a new instance of the <see cref="SelectManyResultCoordinator{TSource, TCollection, TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="collectionSelector">The selector that creates an inner observable for each source value.</param>
    /// <param name="resultSelector">The selector that combines outer and inner values.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/>, <paramref name="collectionSelector"/>, or <paramref name="resultSelector"/> is <see langword="null"/>.</exception>
    public SelectManyResultCoordinator(
        IObserver<TResult> observer,
        Func<TSource, IObservable<TCollection>> collectionSelector,
        Func<TSource, TCollection, TResult> resultSelector)
    {
        Observer = observer ?? throw new ArgumentNullException(nameof(observer));
        CollectionSelector = collectionSelector ?? throw new ArgumentNullException(nameof(collectionSelector));
        ResultSelector = resultSelector ?? throw new ArgumentNullException(nameof(resultSelector));
    }

    /// <summary>Gets the active subscriptions.</summary>
    private MultipleDisposable Subscriptions { get; } = [];

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<TResult> Observer { get; }

    /// <summary>Gets the selector that creates inner observables.</summary>
    private Func<TSource, IObservable<TCollection>> CollectionSelector { get; }

    /// <summary>Gets the selector that combines outer and inner values.</summary>
    private Func<TSource, TCollection, TResult> ResultSelector { get; }

    /// <summary>Gets or sets a value indicating whether the outer source has completed.</summary>
    private bool OuterCompleted { get; set; }

    /// <summary>Gets or sets the number of active inner subscriptions.</summary>
    private int Active { get; set; }

    /// <summary>Gets or sets a value indicating whether a terminal notification has been emitted.</summary>
    private bool Done { get; set; }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => Subscriptions.Dispose();

    /// <inheritdoc/>
    public void OnCompleted()
    {
        lock (_gate)
        {
            OuterCompleted = true;
        }

        TryComplete();
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => OnAnyError(error);

    /// <inheritdoc/>
    public void OnNext(TSource value)
    {
        IObservable<TCollection> inner;
        try
        {
            inner = CollectionSelector(value);
            ArgumentExceptionHelper.ThrowIfNull(inner);
        }
        catch (Exception error)
        {
            OnAnyError(error);
            return;
        }

        lock (_gate)
        {
            if (Done)
            {
                return;
            }

            Active++;
        }

        Subscriptions.Add(inner.Subscribe(
            innerValue => OnInnerNext(value, innerValue),
            OnAnyError,
            OnInnerCompleted));
    }

    /// <summary>Subscribes to the outer source.</summary>
    /// <param name="source">The outer source.</param>
    /// <returns>This coordinator as the subscription.</returns>
    public SelectManyResultCoordinator<TSource, TCollection, TResult> Run(IObservable<TSource> source)
    {
        Subscriptions.Add(source.Subscribe(this));
        return this;
    }

    /// <summary>Forwards the first terminal error.</summary>
    /// <param name="error">The error to forward.</param>
    public void OnAnyError(Exception error)
    {
        lock (_gate)
        {
            if (Done)
            {
                return;
            }

            Done = true;
            Observer.OnError(error);
        }
    }

    /// <summary>Projects and forwards an inner value.</summary>
    /// <param name="sourceValue">The source value.</param>
    /// <param name="innerValue">The inner value.</param>
    private void OnInnerNext(TSource sourceValue, TCollection innerValue)
    {
        TResult result;
        try
        {
            result = ResultSelector(sourceValue, innerValue);
        }
        catch (Exception error)
        {
            OnAnyError(error);
            return;
        }

        lock (_gate)
        {
            if (!Done)
            {
                Observer.OnNext(result);
            }
        }
    }

    /// <summary>Marks one inner source complete.</summary>
    private void OnInnerCompleted()
    {
        lock (_gate)
        {
            Active--;
        }

        TryComplete();
    }

    /// <summary>Completes once the outer and all inners are done.</summary>
    private void TryComplete()
    {
        lock (_gate)
        {
            if (Done || !OuterCompleted || Active != 0)
            {
                return;
            }

            Done = true;
            Observer.OnCompleted();
        }
    }
}
