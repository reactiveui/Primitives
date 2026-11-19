// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
#if REACTIVE_SHIM
using ReactiveUI.Primitives.Reactive.Advanced;
#else
using ReactiveUI.Primitives.Advanced;
#endif

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Internal;
#else
namespace ReactiveUI.Primitives.Internal;
#endif

/// <summary>Observes one inner sequence of a concurrent <c>SelectMany</c> coordinator and detaches itself once completed.</summary>
/// <typeparam name="TOwner">The owning coordinator type.</typeparam>
/// <typeparam name="TState">Per-arm state paired with each inner element.</typeparam>
/// <typeparam name="TValue">The inner sequence element type.</typeparam>
/// <param name="owner">The owning coordinator.</param>
/// <param name="state">Per-arm state passed to <paramref name="onNext"/>.</param>
/// <param name="onNext">Forwards an inner element with <paramref name="state"/>.</param>
/// <param name="onError">Forwards a terminal error.</param>
/// <param name="onCompleted">Detaches this arm on single completion.</param>
internal sealed class SelectManyInnerArm<TOwner, TState, TValue>(
    TOwner owner,
    TState state,
    Action<TOwner, TState, TValue> onNext,
    Action<TOwner, Exception> onError,
    Action<TOwner, IDisposable> onCompleted) : IObserver<TValue>, IDisposable
{
    /// <summary>The inner subscription.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "Disposed atomically via SinkSubscription.Dispose.")]
    private IDisposable? _subscription;

    /// <summary>Set to 1 once this inner source has completed.</summary>
    private int _completed;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(TValue value) => onNext(owner, state, value);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => onError(owner, error);

    /// <inheritdoc/>
    public void OnCompleted()
    {
        if (Interlocked.Exchange(ref _completed, 1) != 0)
        {
            return;
        }

        onCompleted(owner, this);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);

    /// <summary>Assigns the inner subscription.</summary>
    /// <param name="subscription">The inner subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Attach(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);
}
