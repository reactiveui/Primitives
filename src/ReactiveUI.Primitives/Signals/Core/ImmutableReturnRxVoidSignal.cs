// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal sealed class ImmutableReturnRxVoidSignal : IObservable<RxVoid>, IRequireCurrentThread<RxVoid>, IInlineSignal<RxVoid>
{
#pragma warning disable SA1401 // Fields should be private
    internal static ImmutableReturnRxVoidSignal Instance = new();
#pragma warning restore SA1401 // Fields should be private

    private ImmutableReturnRxVoidSignal()
    {
    }

    public bool IsRequiredSubscribeOnCurrentThread() => false;

    public IDisposable Subscribe(IObserver<RxVoid> observer)
    {
        observer.OnNext(RxVoid.Default);
        observer.OnCompleted();
        return Disposable.Empty;
    }

    public IDisposable Subscribe(Action<RxVoid> onNext, Action<Exception> onError, Action onCompleted)
    {
        onNext(RxVoid.Default);
        onCompleted();
        return Disposable.Empty;
    }
}
