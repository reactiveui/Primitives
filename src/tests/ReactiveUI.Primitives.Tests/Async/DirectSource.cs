// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.SystemReactiveBridge;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// An async observable source that stores observer references for direct method invocation in tests.
/// Subscriptions return no-op disposables so that external disposal does not tear down observer access.
/// This enables testing race-condition guards inside operators like CombineLatest.
/// </summary>
/// <typeparam name="T">The element type.</typeparam>
internal sealed class DirectSource<T> : SignalAsync<T>
{
    /// <summary>The stored observer from the most recent subscription.</summary>
    private IObserverAsync<T>? _observer;

    /// <summary>Pushes a value to the stored observer.</summary>
    /// <param name="value">The value to emit.</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    internal ValueTask EmitNext(T value, CancellationToken cancellationToken = default) =>
        _observer!.OnNextAsync(value, cancellationToken);

    /// <summary>Pushes an error-resume to the stored observer.</summary>
    /// <param name="error">The exception to forward.</param>
    /// <param name="cancellationToken">Optional cancellation token.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    internal ValueTask EmitError(Exception error, CancellationToken cancellationToken = default) =>
        _observer!.OnErrorResumeAsync(error, cancellationToken);

    /// <summary>Signals completion to the stored observer.</summary>
    /// <param name="result">The completion result.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    internal ValueTask Complete(Result result) =>
        _observer!.OnCompletedAsync(result);

    /// <inheritdoc/>
    protected override ValueTask<IAsyncDisposable> SubscribeAsyncCore(
        IObserverAsync<T> observer,
        CancellationToken cancellationToken)
    {
        _observer = observer;
        return new(DisposableAsync.Empty);
    }
}
