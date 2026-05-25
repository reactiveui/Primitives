// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal class FinallySignal<T> : SignalsBase<T>
{
    private readonly IObservable<T> _source;
    private readonly Action _finallyAction;

    public FinallySignal(IObservable<T> source, Action finallyAction)
        : base(true)
    {
        _source = source;
        _finallyAction = finallyAction;
    }

    protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel) =>
        new Finally(this, observer, cancel).Run();

    private class Finally : WitnessBase<T, T>
    {
        private readonly FinallySignal<T> _parent;

        public Finally(FinallySignal<T> parent, IObserver<T> observer, IDisposable cancel)
            : base(observer, cancel) => _parent = parent;

        public IDisposable Run()
        {
            IDisposable subscription;
            try
            {
                subscription = _parent._source.Subscribe(this);
            }
            catch
            {
                _parent._finallyAction();
                throw;
            }

            return new MultipleDisposable(subscription, Disposable.Create(() => _parent._finallyAction()));
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
