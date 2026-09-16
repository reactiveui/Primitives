// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Coordinates concurrent observable <c>SelectMany</c> subscriptions with a result selector.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TCollection">The inner value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
/// <remarks>
/// Deliveries are serialized by a <see cref="SerializedDelivery{T}"/>, so no lock is held while a selector, an inner
/// subscription or the downstream observer runs. Values that arrive while another thread is delivering are queued and
/// delivered in arrival order; a value raised by the delivering thread itself is delivered after the observer returns.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("SelectManyResultCoordinator: Active = {Active}, OuterCompleted = {OuterCompleted}, Done = {Done}")]
public sealed class SelectManyResultCoordinator<TSource, TCollection, TResult> : IObserver<TSource>, IDisposable
{
    /// <summary>Serializes downstream deliveries.</summary>
    private SerializedDelivery<TResult> _delivery = new();

    /// <summary>The number of active inner subscriptions.</summary>
    private int _active;

    /// <summary>Whether the outer source has completed, as 0 or 1.</summary>
    private int _outerCompleted;

    /// <summary>Whether a terminal notification has been requested, as 0 or 1.</summary>
    private int _done;

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

    /// <summary>Gets a value indicating whether the outer source has completed.</summary>
    private bool OuterCompleted => Volatile.Read(ref _outerCompleted) != 0;

    /// <summary>Gets the number of active inner subscriptions.</summary>
    private int Active => Volatile.Read(ref _active);

    /// <summary>Gets a value indicating whether a terminal notification has been requested.</summary>
    private bool Done => Volatile.Read(ref _done) != 0;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => Subscriptions.Dispose();

    /// <inheritdoc/>
    public void OnCompleted()
    {
        Volatile.Write(ref _outerCompleted, 1);
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

        if (Done)
        {
            return;
        }

        _ = Interlocked.Increment(ref _active);
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
        Volatile.Write(ref _done, 1);
        _delivery.OnError(error, new PendingDrain(this));
    }

    /// <summary>Projects and forwards an inner value, directly when nothing else is delivering.</summary>
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

        _delivery.OnNext(Observer, result, new PendingDrain(this));
    }

    /// <summary>Marks one inner source complete.</summary>
    private void OnInnerCompleted()
    {
        _ = Interlocked.Decrement(ref _active);
        TryComplete();
    }

    /// <summary>Delivers completion once the outer and all inners are done.</summary>
    private void TryComplete()
    {
        if (!OuterCompleted || Active != 0)
        {
            return;
        }

        Volatile.Write(ref _done, 1);
        _delivery.OnCompleted(new PendingDrain(this));
    }

    /// <summary>Drains this coordinator's queued notifications for the delivery gate.</summary>
    /// <param name="Owner">The coordinator.</param>
    private readonly record struct PendingDrain(SelectManyResultCoordinator<TSource, TCollection, TResult> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain() => _ = Owner._delivery.DrainTo(Owner.Observer);
    }
}
