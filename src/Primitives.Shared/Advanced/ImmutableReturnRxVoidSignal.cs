// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Emits <see cref="RxVoid.Default"/> inline and completes, without allocating a subscription.</summary>
public sealed class ImmutableReturnRxVoidSignal : IRequireCurrentThread<RxVoid>, IInlineSignal<RxVoid>
{
    /// <summary>The shared singleton instance.</summary>
    public static readonly ImmutableReturnRxVoidSignal Instance = new();

    /// <summary>Initializes a new instance of the <see cref="ImmutableReturnRxVoidSignal"/> class.</summary>
    private ImmutableReturnRxVoidSignal()
    {
    }

    /// <summary>Reports that subscription needs no current-thread dispatch.</summary>
    /// <returns>Always <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Emits <see cref="RxVoid.Default"/> and completes before returning.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>An empty disposable; the sequence has ended and there is nothing to cancel.</returns>
    public IDisposable Subscribe(IObserver<RxVoid> observer)
    {
        observer.OnNext(RxVoid.Default);
        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Invokes <paramref name="onNext"/> with <see cref="RxVoid.Default"/> and then <paramref name="onCompleted"/>.</summary>
    /// <param name="onNext">The value callback.</param>
    /// <param name="onError">The error callback, which is never invoked.</param>
    /// <param name="onCompleted">The completion callback.</param>
    /// <returns>An empty disposable; the sequence has ended and there is nothing to cancel.</returns>
    public IDisposable Subscribe(Action<RxVoid> onNext, Action<Exception> onError, Action onCompleted)
    {
        onNext(RxVoid.Default);
        onCompleted();
        return EmptyDisposable.Instance;
    }
}
