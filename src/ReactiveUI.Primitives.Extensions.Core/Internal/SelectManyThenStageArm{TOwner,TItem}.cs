// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Extensions.Internal;

/// <summary>Self-removing observer arm for an intermediate or final sequence in <c>SelectManyThen</c>.</summary>
/// <typeparam name="TOwner">The owning coordinator type.</typeparam>
/// <typeparam name="TItem">The observed element type.</typeparam>
/// <param name="owner">The owning coordinator.</param>
/// <param name="onNext">Callback invoked for each value.</param>
/// <param name="onError">Callback invoked on error.</param>
/// <param name="onStageCompleted">Callback invoked once when the arm completes.</param>
internal sealed class SelectManyThenStageArm<TOwner, TItem>(
    TOwner owner,
    Action<TOwner, TItem> onNext,
    Action<TOwner, Exception> onError,
    Action<TOwner, IDisposable> onStageCompleted) : IObserver<TItem>, IDisposable
{
    /// <summary>Subscription to the stage sequence.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "Disposed atomically via SinkSubscription.Dispose.")]
    private IDisposable? _subscription;

    /// <summary>Non-zero once disposed or completed.</summary>
    private int _disposed;

    /// <summary>Gets a value indicating whether this arm has been disposed or completed.</summary>
    internal bool IsDisposed => Volatile.Read(ref _disposed) != 0;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(TItem value) => onNext(owner, value);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => onError(owner, error);

    /// <inheritdoc/>
    public void OnCompleted()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        SinkSubscription.Dispose(ref _subscription);
        onStageCompleted(owner, this);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        Volatile.Write(ref _disposed, 1);
        SinkSubscription.Dispose(ref _subscription);
    }

    /// <summary>Attaches the stage subscription handle when non-null.</summary>
    /// <param name="subscription">The stage subscription handle.</param>
    internal void Attach(IDisposable? subscription)
    {
        if (subscription is not null)
        {
            SinkSubscription.Set(ref _subscription, subscription);
        }
    }
}
