// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal class ThrowSignal<T> : SignalsBase<T>
{
    private readonly Exception _error;
    private readonly ISequencer _scheduler;

    public ThrowSignal(Exception error, ISequencer scheduler)
        : base(scheduler == Sequencer.CurrentThread)
    {
        _error = error;
        _scheduler = scheduler;
    }

    protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        observer = new Throw(observer, cancel);

        if (_scheduler == Sequencer.Immediate)
        {
            observer.OnError(_error);
            return Disposable.Empty;
        }

        return _scheduler.Schedule(() =>
        {
            observer.OnError(_error);
            observer.OnCompleted();
        });
    }

    private class Throw : WitnessBase<T, T>
    {
        public Throw(IObserver<T> observer, IDisposable cancel)
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
