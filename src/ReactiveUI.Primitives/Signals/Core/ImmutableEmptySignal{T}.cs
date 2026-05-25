// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal sealed class ImmutableEmptySignal<T> : IRequireCurrentThread<T>, IInlineSignal<T>
{
#pragma warning disable SA1401 // Fields should be private
    internal static ImmutableEmptySignal<T> Instance = new();
#pragma warning restore SA1401 // Fields should be private

    private ImmutableEmptySignal()
    {
    }

    public bool IsRequiredSubscribeOnCurrentThread() => false;

    public IDisposable Subscribe(IObserver<T> observer)
    {
        observer.OnCompleted();
        return Disposable.Empty;
    }

    public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        onCompleted();
        return Disposable.Empty;
    }
}
