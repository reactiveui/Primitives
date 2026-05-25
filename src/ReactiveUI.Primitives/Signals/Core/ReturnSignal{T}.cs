// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal class ReturnSignal<T> : SignalsBase<T>
{
    private readonly T _value;
    private readonly ISequencer _scheduler;

    public ReturnSignal(T value, ISequencer scheduler)
        : base(scheduler == Sequencer.CurrentThread)
    {
        _value = value;
        _scheduler = scheduler;
    }

    protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        observer = new Return(observer, cancel);

        if (_scheduler == Sequencer.Immediate)
        {
            observer.OnNext(_value);
            observer.OnCompleted();
            return Disposable.Empty;
        }

        return _scheduler.Schedule(() =>
        {
            observer.OnNext(_value);
            observer.OnCompleted();
        });
    }

    private class Return : WitnessBase<T, T>
    {
        public Return(IObserver<T> observer, IDisposable cancel)
            : base(observer, cancel)
        {
        }

        public override void OnNext(T value)
        {
            try
            {
                Observer.OnNext(value);
            }
            catch
            {
                Dispose();
                throw;
            }
        }

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
