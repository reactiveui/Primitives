// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal sealed class RepeatSignal<T> : IObservable<T>, IRequireCurrentThread<T>, IInlineSignal<T>
{
    private readonly T _value;
    private readonly int _count;

    public RepeatSignal(T value, int count)
    {
        _value = value;
        _count = count;
    }

    public bool IsRequiredSubscribeOnCurrentThread() => false;

    public IDisposable Subscribe(IObserver<T> observer)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        for (var i = 0; i < _count; i++)
        {
            observer.OnNext(_value);
        }

        observer.OnCompleted();
        return Disposable.Empty;
    }

    public IDisposable Subscribe(Action<T> onNext, Action<Exception> onError, Action onCompleted)
    {
        if (onNext == null)
        {
            throw new ArgumentNullException(nameof(onNext));
        }

        for (var i = 0; i < _count; i++)
        {
            onNext(_value);
        }

        onCompleted();
        return Disposable.Empty;
    }
}
