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

/// <summary>Projects source values and forwards non-null results.</summary>
/// <typeparam name="TSource">The source element type.</typeparam>
/// <typeparam name="TResult">The non-null reference result type.</typeparam>
/// <param name="observer">The downstream observer.</param>
/// <param name="selector">The projection.</param>
[System.Diagnostics.DebuggerDisplay("MapKeepNotNullWitness: Observer = {_observer}")]
public sealed class MapKeepNotNullWitness<TSource, TResult>(IObserver<TResult> observer, Func<TSource, TResult?> selector) : IObserver<TSource>, IDisposable
    where TResult : class
{
    /// <summary>The downstream observer.</summary>
    private readonly IObserver<TResult> _observer = observer;

    /// <summary>The projection.</summary>
    private readonly Func<TSource, TResult?> _selector = selector;

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
        _slot.NextFilteredProjection(
            value,
            _selector,
            static (item, sel) => sel(item) is { } nonNull ? new(true, nonNull) : default,
            _observer,
            this);
}
