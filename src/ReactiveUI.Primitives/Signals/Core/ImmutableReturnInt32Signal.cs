// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

internal class ImmutableReturnInt32Signal : IObservable<int>, IRequireCurrentThread<int>, IInlineSignal<int>
{
    private static readonly ImmutableReturnInt32Signal[] Caches = new ImmutableReturnInt32Signal[]
    {
            new ImmutableReturnInt32Signal(-1),
            new ImmutableReturnInt32Signal(0),
            new ImmutableReturnInt32Signal(1),
            new ImmutableReturnInt32Signal(2),
            new ImmutableReturnInt32Signal(3),
            new ImmutableReturnInt32Signal(4),
            new ImmutableReturnInt32Signal(5),
            new ImmutableReturnInt32Signal(6),
            new ImmutableReturnInt32Signal(7),
            new ImmutableReturnInt32Signal(8),
            new ImmutableReturnInt32Signal(9),
    };

    private readonly int _x;

    internal ImmutableReturnInt32Signal(int x) => _x = x;

    public static IObservable<int> GetInt32Signals(int x)
    {
        if (x >= -1 && x <= 9)
        {
            return Caches[x + 1];
        }

        return new ImmediateReturnSignal<int>(x);
    }

    public bool IsRequiredSubscribeOnCurrentThread() => false;

    public IDisposable Subscribe(IObserver<int> observer)
    {
        observer.OnNext(_x);
        observer.OnCompleted();
        return Disposable.Empty;
    }

    public IDisposable Subscribe(Action<int> onNext, Action<Exception> onError, Action onCompleted)
    {
        onNext(_x);
        onCompleted();
        return Disposable.Empty;
    }
}
