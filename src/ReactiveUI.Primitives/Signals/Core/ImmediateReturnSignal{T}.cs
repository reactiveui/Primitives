// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal class ImmediateReturnSignal<T> : IObservable<T>, IRequireCurrentThread<T>, IInlineSignal<T>
{
    private readonly T _value;

    public ImmediateReturnSignal(T value) => _value = value;

    public bool IsRequiredSubscribeOnCurrentThread() => false;

    public IDisposable Subscribe(IObserver<T> observer)
    {
        observer.OnNext(_value);
        observer.OnCompleted();
        return Disposable.Empty;
    }

    public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        onNext(_value);
        onCompleted();
        return Disposable.Empty;
    }
}
