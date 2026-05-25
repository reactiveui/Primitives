// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal class ImmutableReturnFalseSignal : IObservable<bool>, IRequireCurrentThread<bool>, IInlineSignal<bool>
{
#pragma warning disable SA1401 // Fields should be private
    internal static ImmutableReturnFalseSignal Instance = new();
#pragma warning restore SA1401 // Fields should be private

    private ImmutableReturnFalseSignal()
    {
    }

    public bool IsRequiredSubscribeOnCurrentThread() => false;

    public IDisposable Subscribe(IObserver<bool> observer)
    {
        observer.OnNext(false);
        observer.OnCompleted();
        return Disposable.Empty;
    }

    public IDisposable Subscribe(Action<bool> onNext, Action<Exception> onError, Action onCompleted)
    {
        onNext(false);
        onCompleted();
        return Disposable.Empty;
    }
}
