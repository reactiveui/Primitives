// Copyright (c) 2019-2023 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals.Core;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal class DeferSignal<T> : SignalsBase<T>
{
    private readonly Func<IObservable<T>> _observableFactory;

    public DeferSignal(Func<IObservable<T>> observableFactory)
        : base(false) => _observableFactory = observableFactory;

    protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        observer = new Defer(observer, cancel);

        IObservable<T> source;
        try
        {
            source = _observableFactory();
        }
        catch (Exception ex)
        {
            source = Signal.Throw<T>(ex);
        }

        return source.Subscribe(observer);
    }

    private class Defer : WitnessBase<T, T>
    {
        public Defer(IObserver<T> observer, IDisposable cancel)
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
