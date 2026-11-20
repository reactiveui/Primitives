// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Internal;
#else
using ReactiveUI.Primitives.Internal;
#endif

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Applies both projections to each source element.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TMiddle">The intermediate element type.</typeparam>
/// <typeparam name="TResult">The final projected element type.</typeparam>
/// <param name="observer">The downstream observer.</param>
/// <param name="first">The first projection.</param>
/// <param name="second">The second projection.</param>
[System.Diagnostics.DebuggerDisplay("MapMapWitness: Observer = {_observer}")]
public sealed class MapMapWitness<TSource, TMiddle, TResult>(
    IObserver<TResult> observer,
    Func<TSource, TMiddle> first,
    Func<TMiddle, TResult> second) : IObserver<TSource>, IDisposable
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<TResult> _observer = observer;

    /// <summary>The chained projections.</summary>
    private readonly StagePair<Func<TSource, TMiddle>, Func<TMiddle, TResult>> _stages = new(first, second);

    /// <summary>Tracks the upstream subscription and stop flag.</summary>
    private UnaryWitnessSlot _slot;

    /// <summary>Assigns the upstream subscription.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SetSubscription(IDisposable subscription) => _slot.Attach(subscription);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => _slot.Dispose();

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => _slot.Complete(_observer, this);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => _slot.Fault(_observer, error, this);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(TSource value) =>
        _slot.NextProjected(value, _stages, static (item, s) => s.Second(s.First(item)), _observer, this);
}
