// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal class CreateSignal<T, TState> : SignalsBase<T>
{
    private readonly TState _state;
    private readonly Func<TState, IObserver<T>, IDisposable> _subscribe;

    public CreateSignal(TState state, Func<TState, IObserver<T>, IDisposable> subscribe)
        : base(true) // fail safe
    {
        _state = state;
        _subscribe = subscribe;
    }

    public CreateSignal(TState state, Func<TState, IObserver<T>, IDisposable> subscribe, bool isRequiredSubscribeOnCurrentThread)
        : base(isRequiredSubscribeOnCurrentThread)
    {
        _state = state;
        _subscribe = subscribe;
    }

    protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        observer = new Create(observer, cancel);
        return _subscribe(_state, observer) ?? Disposable.Empty;
    }

    private class Create : WitnessBase<T, T>
    {
        public Create(IObserver<T> observer, IDisposable cancel)
            : base(observer, cancel)
        {
        }

        public override void OnNext(T value) => Observer.OnNext(value);

        public override void OnError(Exception error)
        {
            try
            {
                Observer.OnError(error);
            }
            finally
            {
                Dispose();
            }
        }

        public override void OnCompleted()
        {
            try
            {
                Observer.OnCompleted();
            }
            finally
            {
                Dispose();
            }
        }
    }
}
