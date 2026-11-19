// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observes the source stream and routes its notifications through the coordinator.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("TakeUntilSourceWitness<T>: Coordinator = {_coordinator}")]
public sealed class TakeUntilSourceWitness<T> : IObserver<T>
{
    /// <summary>The owning coordinator.</summary>
    private readonly TakeUntilCoordinator<T> _coordinator;

    /// <summary>Initializes a new instance of the <see cref="TakeUntilSourceWitness{T}"/> class.</summary>
    /// <param name="coordinator">The owning coordinator.</param>
    public TakeUntilSourceWitness(TakeUntilCoordinator<T> coordinator) => _coordinator = coordinator;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(T value) => _coordinator.Next(value);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => _coordinator.Error(error);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => _coordinator.Complete();
}
