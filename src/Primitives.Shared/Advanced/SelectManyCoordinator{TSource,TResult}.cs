// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Coordinates concurrent observable <c>SelectMany</c> subscriptions.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TResult">The result value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("SelectManyCoordinator: Active = {Active}, OuterCompleted = {OuterCompleted}, Done = {Done}")]
public sealed class SelectManyCoordinator<TSource, TResult> : IObserver<TSource>, IDisposable
{
    /// <summary>Serializes downstream callbacks and counters.</summary>
    private readonly Lock _gate = new();

    /// <summary>Initializes a new instance of the <see cref="SelectManyCoordinator{TSource, TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="selector">The selector that creates an inner observable for each source value.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> or <paramref name="selector"/> is <see langword="null"/>.</exception>
    public SelectManyCoordinator(IObserver<TResult> observer, Func<TSource, IObservable<TResult>> selector)
    {
        Observer = observer ?? throw new ArgumentNullException(nameof(observer));
        Selector = selector ?? throw new ArgumentNullException(nameof(selector));
    }

    /// <summary>Initializes a new instance of the <see cref="SelectManyCoordinator{TSource, TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="inner">The inner observable used for each source value.</param>
    /// <exception cref="ArgumentNullException"><paramref name="observer"/> or <paramref name="inner"/> is <see langword="null"/>.</exception>
    public SelectManyCoordinator(IObserver<TResult> observer, IObservable<TResult> inner)
    {
        Observer = observer ?? throw new ArgumentNullException(nameof(observer));
        Inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    /// <summary>Gets the active subscriptions.</summary>
    private MultipleDisposable Subscriptions { get; } = [];

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<TResult> Observer { get; }

    /// <summary>Gets the selector, when selector-based.</summary>
    private Func<TSource, IObservable<TResult>>? Selector { get; }

    /// <summary>Gets the constant inner observable, when constant-inner based.</summary>
    private IObservable<TResult>? Inner { get; }

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
        IObservable<TResult> inner;
        try
        {
            inner = Selector is { } selector ? selector(value) : Inner!;
        }
        catch (Exception error)
        {
            OnAnyError(error);
            return;
        }

        if (inner is null)
        {
            OnAnyError(new InvalidOperationException("Blend source contained null."));
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

        Subscriptions.Add(inner.Subscribe(OnInnerNext, OnAnyError, OnInnerCompleted));
    }

    /// <summary>Subscribes to the outer source.</summary>
    /// <param name="source">The outer source.</param>
    /// <returns>This coordinator as the subscription.</returns>
    public SelectManyCoordinator<TSource, TResult> Run(IObservable<TSource> source)
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

    /// <summary>Forwards an inner value.</summary>
    /// <param name="value">The value to forward.</param>
    private void OnInnerNext(TResult value)
    {
        lock (_gate)
        {
            if (!Done)
            {
                Observer.OnNext(value);
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
