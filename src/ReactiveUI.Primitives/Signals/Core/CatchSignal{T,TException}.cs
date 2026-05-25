// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal class CatchSignal<T, TException> : SignalsBase<T>
        where TException : Exception
{
    private readonly IObservable<T> _source;
    private readonly Func<TException, IObservable<T>> _errorHandler;

    public CatchSignal(IObservable<T> source, Func<TException, IObservable<T>> errorHandler)
        : base(true)
    {
        _source = source;
        _errorHandler = errorHandler;
    }

    protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel) =>
        new Catch(this, observer, cancel).Run();

    private class Catch : WitnessBase<T, T>
    {
        private readonly CatchSignal<T, TException> _parent;
        private SingleDisposable? _sourceSubscription;
        private SingleDisposable? _exceptionSubscription;

        public Catch(CatchSignal<T, TException> parent, IObserver<T> observer, IDisposable cancel)
            : base(observer, cancel) => _parent = parent;

        public IDisposable Run()
        {
            _exceptionSubscription = new SingleDisposable();
            _sourceSubscription = new SingleDisposable(_parent._source.Subscribe(this));

            return new MultipleDisposable(_sourceSubscription, _exceptionSubscription);
        }

        public override void OnNext(T value) => Observer.OnNext(value);

        public override void OnError(Exception error)
        {
            if (error is TException e)
            {
                IObservable<T> next;
                try
                {
                    if (_parent._errorHandler == Handle.CatchIgnore<T>)
                    {
                        next = Signal.Empty<T>();
                    }
                    else
                    {
                        next = _parent._errorHandler(e);
                    }
                }
                catch (Exception ex)
                {
                    try
                    {
                        Observer.OnError(ex);
                    }
                    finally
                    {
                        Dispose();
                    }

                    return;
                }

                _exceptionSubscription?.Create(next.Subscribe(Observer));
            }
            else
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
