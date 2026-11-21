// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Internal;

/// <summary>Forwards notifications until disposed, then detaches from its owner once.</summary>
/// <typeparam name="TOwner">The owner type.</typeparam>
/// <typeparam name="T">The observed value type.</typeparam>
/// <param name="owner">The owning signal or window.</param>
/// <param name="observer">The downstream observer.</param>
/// <param name="remove">Detaches this observer from <paramref name="owner"/>.</param>
/// <param name="resource">An optional lease disposed after detaching.</param>
internal sealed class DetachableObserver<TOwner, T>(
    TOwner owner,
    IObserver<T> observer,
    Action<TOwner, IObserver<T>> remove,
    IDisposable? resource = null) : IObserver<T>, IDisposable
    where TOwner : class
{
    /// <summary>Detaches this observer from <see cref="_owner"/>.</summary>
    private readonly Action<TOwner, IObserver<T>> _remove = remove;

    /// <summary>The owning signal or window, cleared on the first disposal.</summary>
    [SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "Back-reference to the owner; detached via _remove rather than disposed.")]
    private TOwner? _owner = owner;

    /// <summary>The downstream observer, cleared on disposal so in-flight snapshots stop delivering.</summary>
    private IObserver<T>? _observer = observer;

    /// <summary>An optional lease disposed after detaching.</summary>
    [SuppressMessage(
        "Usage",
        "CA2213:Disposable fields should be disposed",
        Justification = "Cleared and disposed once on the first Dispose call.")]
    private IDisposable? _resource = resource;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnNext(T value) => Volatile.Read(ref _observer)?.OnNext(value);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnError(Exception error) => Volatile.Read(ref _observer)?.OnError(error);

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void OnCompleted() => Volatile.Read(ref _observer)?.OnCompleted();

    /// <inheritdoc/>
    public void Dispose()
    {
        Volatile.Write(ref _observer, null);
        var targetOwner = Interlocked.Exchange(ref _owner, null);
        if (targetOwner is null)
        {
            return;
        }

        _remove(targetOwner, this);
        var heldResource = _resource;
        _resource = null;
        heldResource?.Dispose();
    }
}
