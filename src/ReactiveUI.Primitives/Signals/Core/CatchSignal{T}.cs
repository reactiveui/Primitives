// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal class CatchSignal<T> : SignalsBase<T>
{
    private readonly IEnumerable<IObservable<T>> _sources;

    public CatchSignal(IEnumerable<IObservable<T>> sources)
        : base(true) => _sources = sources;

    protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel) =>
        new Catch(this, observer, cancel).Run();

    private class Catch : WitnessBase<T, T>
    {
        private readonly CatchSignal<T> _parent;
        private readonly object _gate = new();
        private bool _isDisposed;
        private IEnumerator<IObservable<T>>? _e;
        private SingleReplaceableDisposable? _subscription;
        private Exception? _lastException;
        private Action? _nextSelf;

        public Catch(CatchSignal<T> parent, IObserver<T> observer, IDisposable cancel)
            : base(observer, cancel) => _parent = parent;

        public IDisposable Run()
        {
            _isDisposed = false;
            _e = _parent._sources.GetEnumerator();
            _subscription = new SingleReplaceableDisposable();

            var schedule = Sequencer.Immediate.Schedule(RecursiveRun);

            return new MultipleDisposable(schedule, _subscription, Disposable.Create(() =>
            {
                lock (_gate)
                {
                    _isDisposed = true;
                    _e.Dispose();
                }
            }));
        }

        public override void OnNext(T value) => Observer.OnNext(value);

        public override void OnError(Exception error)
        {
            _lastException = error;
            _nextSelf!();
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

        private void RecursiveRun(Action self)
        {
            lock (_gate)
            {
                _nextSelf = self;
                if (_isDisposed)
                {
                    return;
                }

                var current = default(IObservable<T>);
                var hasNext = false;
                var ex = default(Exception);

                try
                {
                    hasNext = _e!.MoveNext();
                    if (hasNext)
                    {
                        current = _e.Current;
                        if (current == null)
                        {
                            throw new InvalidOperationException("sequence is null.");
                        }
                    }
                    else
                    {
                        _e.Dispose();
                    }
                }
                catch (Exception exception)
                {
                    ex = exception;
                    _e?.Dispose();
                }

                if (ex != null)
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

                if (!hasNext)
                {
                    if (_lastException != null)
                    {
                        try
                        {
                            Observer.OnError(_lastException);
                        }
                        finally
                        {
                            Dispose();
                        }
                    }
                    else
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

                    return;
                }

                var source = current;
                _subscription?.Create(new SingleDisposable(source!.Subscribe(this)));
            }
        }
    }
}
