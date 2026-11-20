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

/// <summary>Observes one source arm of a multi-source coordinator and manages its upstream subscription handle.</summary>
/// <typeparam name="TOwner">The owning coordinator or witness type.</typeparam>
/// <typeparam name="T">The observed element type.</typeparam>
/// <param name="owner">The owning coordinator or witness.</param>
/// <param name="callbacks">The value, error, and completion callbacks.</param>
internal sealed class ForwardingObserverArm<TOwner, T>(
    TOwner owner,
    BinaryArmCallbacks<TOwner, T> callbacks) : IObserver<T>, IDisposable
{
    /// <summary>The upstream subscription handle.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "Disposed atomically via SinkSubscription.Dispose.")]
    private IDisposable? _subscription;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(T value) => callbacks.OnNext(owner, value);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => callbacks.OnError(owner, error);

    /// <inheritdoc/>
    public void OnCompleted()
    {
        if (callbacks.DisposeArmOnCompleted)
        {
            Dispose();
        }

        callbacks.OnCompleted(owner);
    }

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => SinkSubscription.Dispose(ref _subscription);

    /// <summary>Assigns the upstream subscription handle, disposing it immediately if this arm was already disposed.</summary>
    /// <param name="subscription">The upstream subscription.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Attach(IDisposable subscription) => SinkSubscription.Set(ref _subscription, subscription);
}
