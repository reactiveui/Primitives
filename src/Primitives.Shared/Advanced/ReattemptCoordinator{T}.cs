// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Coordinates retry-on-error resubscription for <c>Reattempt</c>.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="retryCount">The maximum number of retries.</param>
/// <param name="observer">The downstream observer.</param>
internal sealed class ReattemptCoordinator<T>(IObservable<T> source, int retryCount, IObserver<T> observer) : IDisposable
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source = source;

    /// <summary>The maximum number of retries.</summary>
    private readonly int _retryCount = retryCount;

    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer = observer;

    /// <summary>Active subscriptions across retries.</summary>
    private readonly MultipleDisposable _pocket = [];

    /// <summary>The number of retries attempted so far.</summary>
    private int _attempts;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => _pocket.Dispose();

    /// <summary>Starts the first subscription attempt.</summary>
    /// <returns>The coordinator that owns the subscription cleanup.</returns>
    internal ReattemptCoordinator<T> Run()
    {
        SubscribeNext();
        return this;
    }

    /// <summary>Subscribes to the source for the current attempt.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void SubscribeNext() =>
        _pocket.Add(_source.Subscribe(_observer.OnNext, OnError, _observer.OnCompleted));

    /// <summary>Retries the subscription, or forwards the error once retries are exhausted.</summary>
    /// <param name="error">The error raised by the source.</param>
    private void OnError(Exception error)
    {
        var attempt = _attempts;
        _attempts++;
        if (attempt < _retryCount)
        {
            SubscribeNext();
        }
        else
        {
            _observer.OnError(error);
        }
    }
}
