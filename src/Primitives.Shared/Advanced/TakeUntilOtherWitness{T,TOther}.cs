// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Observes the cancellation stream; its first value or error stops the source.</summary>
/// <typeparam name="T">The source value type.</typeparam>
/// <typeparam name="TOther">The cancellation value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("TakeUntilOtherWitness<T, TOther>: Coordinator = {_coordinator}")]
public sealed class TakeUntilOtherWitness<T, TOther> : IObserver<TOther>
{
    /// <summary>The owning coordinator.</summary>
    private readonly TakeUntilCoordinator<T> _coordinator;

    /// <summary>Initializes a new instance of the <see cref="TakeUntilOtherWitness{T, TOther}"/> class.</summary>
    /// <param name="coordinator">The owning coordinator.</param>
    public TakeUntilOtherWitness(TakeUntilCoordinator<T> coordinator) => _coordinator = coordinator;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(TOther value) => _coordinator.Complete();

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => _coordinator.Error(error);

    /// <inheritdoc/>
    public void OnCompleted()
    {
        // Completion of the cancellation stream without a value does not stop the source.
    }
}
