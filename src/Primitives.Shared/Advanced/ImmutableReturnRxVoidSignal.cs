// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>An allocation-free observable that emits a single value and then completes; the concrete singleton backing for the RxVoid emit path.</summary>
public sealed class ImmutableReturnRxVoidSignal : IRequireCurrentThread<RxVoid>, IInlineSignal<RxVoid>
{
    /// <summary>The shared singleton instance.</summary>
    public static readonly ImmutableReturnRxVoidSignal Instance = new();

    /// <summary>Initializes a new instance of the <see cref="ImmutableReturnRxVoidSignal"/> class.</summary>
    private ImmutableReturnRxVoidSignal()
    {
    }

    /// <summary>Executes the IsRequiredSubscribeOnCurrentThread operation.</summary>
    /// <returns>The result.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(IObserver<RxVoid> observer)
    {
        observer.OnNext(RxVoid.Default);
        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="onNext">The onNext value.</param>
    /// <param name="onError">The onError value.</param>
    /// <param name="onCompleted">The onCompleted value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(Action<RxVoid> onNext, Action<Exception> onError, Action onCompleted)
    {
        onNext(RxVoid.Default);
        onCompleted();
        return EmptyDisposable.Instance;
    }
}
