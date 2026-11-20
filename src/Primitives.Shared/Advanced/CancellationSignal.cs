// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>An observable that emits a single <see cref="RxVoid"/> value when its cancellation token is canceled; the stop source for <c>TakeUntil(CancellationToken)</c>.</summary>
[System.Diagnostics.DebuggerDisplay("CancellationSignal: CancellationToken = {_cancellationToken}")]
public sealed class CancellationSignal : IObservable<RxVoid>
{
    /// <summary>The token whose cancellation triggers the emission.</summary>
    private readonly CancellationToken _cancellationToken;

    /// <summary>Initializes a new instance of the <see cref="CancellationSignal"/> class.</summary>
    /// <param name="cancellationToken">The token whose cancellation triggers the emission.</param>
    public CancellationSignal(CancellationToken cancellationToken) => _cancellationToken = cancellationToken;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<RxVoid> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return _cancellationToken.UnsafeRegister(
            static state => ((IObserver<RxVoid>)state!).OnNext(RxVoid.Default),
            observer);
    }
}
