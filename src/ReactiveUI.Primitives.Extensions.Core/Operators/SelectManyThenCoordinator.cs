// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Operators;

/// <summary>Coordinates the two projection stages of <c>SelectManyThen</c> in one sink.</summary>
/// <typeparam name="TSource">The source value type.</typeparam>
/// <typeparam name="TMid">The intermediate value type produced by the first projection.</typeparam>
/// <typeparam name="TResult">The result value type produced by the second projection.</typeparam>
/// <remarks>
/// Both stages run against one active count, so completion arrives once, after the source and every sequence either
/// stage opened have finished. Deliveries are serialized by a <see cref="SerializedDelivery{T}"/>, so no lock is held
/// while a selector, an inner subscription or the downstream observer runs.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("SelectManyThenCoordinator: Active = {Active}, SourceCompleted = {SourceCompleted}, Done = {Done}")]
public sealed class SelectManyThenCoordinator<TSource, TMid, TResult> : IObserver<TSource>, IDisposable
{
    /// <summary>Serializes downstream deliveries.</summary>
    private SerializedDelivery<TResult> _delivery = new();

    /// <summary>The subscriptions this coordinator owns.</summary>
    private DisposableSet _subscriptions;

    /// <summary>The number of sequences either stage has opened and that have not yet finished.</summary>
    private int _active;

    /// <summary>Whether the source has completed, as 0 or 1.</summary>
    private int _sourceCompleted;

    /// <summary>Whether a terminal notification has been requested, as 0 or 1.</summary>
    private int _done;

    /// <summary>Initializes a new instance of the <see cref="SelectManyThenCoordinator{TSource, TMid, TResult}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="first">The first projection, from a source value to an intermediate sequence.</param>
    /// <param name="second">The second projection, from an intermediate value to a result sequence.</param>
    /// <exception cref="ArgumentNullException">Any argument is <see langword="null"/>.</exception>
    public SelectManyThenCoordinator(
        IObserver<TResult> observer,
        Func<TSource, IObservable<TMid>> first,
        Func<TMid, IObservable<TResult>> second)
    {
        Observer = observer ?? throw new ArgumentNullException(nameof(observer));
        First = first ?? throw new ArgumentNullException(nameof(first));
        Second = second ?? throw new ArgumentNullException(nameof(second));
    }

    /// <summary>Gets the downstream observer.</summary>
    private IObserver<TResult> Observer { get; }

    /// <summary>Gets the first projection.</summary>
    private Func<TSource, IObservable<TMid>> First { get; }

    /// <summary>Gets the second projection.</summary>
    private Func<TMid, IObservable<TResult>> Second { get; }

    /// <summary>Gets a value indicating whether the source has completed.</summary>
    private bool SourceCompleted => Volatile.Read(ref _sourceCompleted) != 0;

    /// <summary>Gets the number of sequences still running.</summary>
    private int Active => Volatile.Read(ref _active);

    /// <summary>Gets a value indicating whether a terminal notification has been requested.</summary>
    private bool Done => Volatile.Read(ref _done) != 0;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose()
    {
        Volatile.Write(ref _done, 1);
        _subscriptions.Dispose();
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(TSource value) =>
        SelectManyThenStageHelper.SubscribeProjectedStage(
            this,
            value,
            Done,
            ref _subscriptions,
            ref _active,
            new SelectManyThenStageSpec<SelectManyThenCoordinator<TSource, TMid, TResult>, TSource, TMid>(
                First,
                "SelectManyThen first projection returned null.",
                static (self, mid) => self.OnMidNext(mid),
                static (self, err) => self.OnAnyError(err),
                static (self, sub) => self.OnStageCompleted(sub)));

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => OnAnyError(error);

    /// <inheritdoc/>
    public void OnCompleted()
    {
        if (Interlocked.Exchange(ref _sourceCompleted, 1) != 0)
        {
            return;
        }

        TryComplete();
    }

    /// <summary>Subscribes to the source sequence.</summary>
    /// <param name="source">The source sequence.</param>
    /// <returns>This coordinator, which is the subscription handle.</returns>
    public SelectManyThenCoordinator<TSource, TMid, TResult> Run(IObservable<TSource> source)
    {
        _subscriptions.Add(source.Subscribe(this));
        return this;
    }

    /// <summary>Forwards the first terminal error.</summary>
    /// <param name="error">The error to forward.</param>
    public void OnAnyError(Exception error)
    {
        if (Interlocked.Exchange(ref _done, 1) != 0)
        {
            return;
        }

        _subscriptions.Dispose();
        _delivery.OnError(error, new PendingDrain(this));
    }

    /// <summary>Runs the second projection for an intermediate value and subscribes its sequence.</summary>
    /// <param name="value">The intermediate value.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnMidNext(TMid value) =>
        SelectManyThenStageHelper.SubscribeProjectedStage(
            this,
            value,
            Done,
            ref _subscriptions,
            ref _active,
            new SelectManyThenStageSpec<SelectManyThenCoordinator<TSource, TMid, TResult>, TMid, TResult>(
                Second,
                "SelectManyThen second projection returned null.",
                static (self, res) => self.OnInnerNext(res),
                static (self, err) => self.OnAnyError(err),
                static (self, sub) => self.OnStageCompleted(sub)));

    /// <summary>Forwards a result value, directly when nothing else is delivering.</summary>
    /// <param name="value">The value to forward.</param>
    private void OnInnerNext(TResult value)
    {
        try
        {
            _delivery.OnNext(Observer, value, new PendingDrain(this));
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    /// <summary>Marks one opened sequence, from either stage, as finished and removes its subscription.</summary>
    /// <param name="stageSubscription">The completed stage witness to remove from the active set.</param>
    private void OnStageCompleted(IDisposable stageSubscription)
    {
        _ = _subscriptions.Remove(stageSubscription);
        if (Interlocked.Decrement(ref _active) == 0)
        {
            TryComplete();
        }
    }

    /// <summary>Delivers completion once the source and every opened sequence are finished.</summary>
    private void TryComplete()
    {
        if (!SourceCompleted || Active != 0 || Interlocked.CompareExchange(ref _done, 1, 0) != 0)
        {
            return;
        }

        _subscriptions.Dispose();
        _delivery.OnCompleted(new PendingDrain(this));
    }

    /// <summary>Drains queued notifications and tears down subscriptions upon terminal delivery or fault.</summary>
    private void DrainPending()
    {
        try
        {
            if (_delivery.DrainTo(Observer))
            {
                _subscriptions.Dispose();
            }
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    /// <summary>Drains this coordinator's queued notifications for the delivery gate.</summary>
    /// <param name="Owner">The coordinator.</param>
    private readonly record struct PendingDrain(SelectManyThenCoordinator<TSource, TMid, TResult> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain() => Owner.DrainPending();
    }
}
