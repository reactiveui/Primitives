// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>
/// Synchronous test source that hands its observer back to the test so the test can
/// invoke <c>OnNext</c> / <c>OnError</c> / <c>OnCompleted</c> directly — including
/// sequences that <see cref="Subject{T}"/> would otherwise block (emit-after-complete,
/// double-terminal). Subscriptions return a no-op disposable so external dispose does
/// not detach the observer.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
internal sealed class SyncDirectSource<T> : IObservable<T>
{
    /// <summary>The captured observer from the most recent subscription.</summary>
    private IObserver<T>? _observer;

    /// <summary>Gets the captured observer; throws if no one has subscribed yet.</summary>
    public IObserver<T> Observer => _observer
        ?? throw new InvalidOperationException("No observer is currently subscribed.");

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        _observer = observer;
        return EmptyDisposable.Instance;
    }
}
