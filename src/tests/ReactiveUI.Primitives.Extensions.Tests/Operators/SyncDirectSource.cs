// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Retains its observer for direct notification after disposal or termination.</summary>
/// <typeparam name="T">The element type.</typeparam>
internal sealed class SyncDirectSource<T> : IObservable<T>
{
    /// <summary>The captured observer from the most recent subscription.</summary>
    private IObserver<T>? _observer;

    /// <summary>Count of disposed subscription handles.</summary>
    private int _disposeCount;

    /// <summary>Gets the captured observer; throws if no one has subscribed yet.</summary>
    internal IObserver<T> Observer => _observer
                                    ?? throw new InvalidOperationException("No observer is currently subscribed.");

    /// <summary>Gets the number of subscription disposals observed.</summary>
    internal int DisposeCount => Volatile.Read(ref _disposeCount);

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        _observer = observer;
        return new ActionDisposable(() => _ = Interlocked.Increment(ref _disposeCount));
    }
}
