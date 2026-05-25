// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal class WitnessOnSignal<T> : SignalsBase<T>
{
    private readonly IObservable<T> _source;
    private readonly ISequencer _scheduler;

    public WitnessOnSignal(IObservable<T> source, ISequencer scheduler)
        : base(true)
    {
        _source = source;
        _scheduler = scheduler;
    }

    protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        if (_scheduler is not ThreadPoolSequencer queueing)
        {
            return new WitnessOn(this, observer, cancel).Run();
        }

        return new WitnessOn_(this, queueing, observer, cancel).Run();
    }

    private class WitnessOn : WitnessBase<T, T>
    {
        private readonly WitnessOnSignal<T> _parent;
        private readonly LinkedList<SchedulableAction> _actions = new();
        private bool _isDisposed;

        public WitnessOn(WitnessOnSignal<T> parent, IObserver<T> observer, IDisposable cancel)
            : base(observer, cancel) => _parent = parent;

        public IDisposable Run()
        {
            _isDisposed = false;

            var sourceDisposable = _parent._source.Subscribe(this);

            return new MultipleDisposable(sourceDisposable, Disposable.Create(() =>
            {
                lock (_actions)
                {
                    _isDisposed = true;

                    while (_actions.Count > 0)
                    {
                        // Dispose will both cancel the action (if not already running)
                        // and remove it from 'actions'
                        _actions.First?.Value.Dispose();
                    }
                }
            }));
        }

        public override void OnNext(T value) => QueueAction(new Spark<T>.OnNextSpark(value));

        public override void OnError(Exception error) => QueueAction(new Spark<T>.OnErrorSpark(error));

        public override void OnCompleted() => QueueAction(new Spark<T>.OnCompletedSpark());

        private void QueueAction(Spark<T> data)
        {
            var action = new SchedulableAction(data);
            lock (_actions)
            {
                if (_isDisposed)
                {
                    return;
                }

                action.Node = _actions.AddLast(action);
                ProcessNext();
            }
        }

        private void ProcessNext()
        {
            lock (_actions)
            {
                if (_actions.Count == 0 || _isDisposed)
                {
                    return;
                }

                var action = _actions.First?.Value;

                if (action?.IsScheduled == true)
                {
                    return;
                }

                action!.Schedule = _parent._scheduler.Schedule(() =>
                {
                    try
                    {
                        switch (action.Data?.Kind)
                        {
                            case SparkKind.OnNext:
                                Observer.OnNext(action.Data.Value);
                                break;
                            case SparkKind.OnError:
                                Observer.OnError(action.Data.Exception);
                                break;
                            case SparkKind.OnCompleted:
                                Observer.OnCompleted();
                                break;
                        }
                    }
                    finally
                    {
                        lock (_actions)
                        {
                            action.Dispose();
                        }

                        if (action.Data?.Kind == SparkKind.OnNext)
                        {
                            ProcessNext();
                        }
                        else
                        {
                            Dispose();
                        }
                    }
                });
            }
        }

        private class SchedulableAction : IDisposable
        {
            public SchedulableAction(Spark<T> data)
            {
                Data = data;
            }

            public Spark<T> Data { get; }

            public LinkedListNode<SchedulableAction>? Node { get; set; }

            public IDisposable? Schedule { get; set; }

            public bool IsScheduled => Schedule != null;

            public void Dispose()
            {
                Schedule?.Dispose();

                Schedule = null;

                if (Node?.List != null)
                {
                    Node.List.Remove(Node);
                }
            }
        }
    }

    private class WitnessOn_ : WitnessBase<T, T>
    {
        private readonly WitnessOnSignal<T> _parent;
        private readonly ThreadPoolSequencer _scheduler;
        private readonly BooleanDisposable _isDisposed;
        private readonly Action<T> _onNext;

        public WitnessOn_(WitnessOnSignal<T> parent, ThreadPoolSequencer scheduler, IObserver<T> observer, IDisposable cancel)
            : base(observer, cancel)
        {
            _parent = parent;
            _scheduler = scheduler;
            _isDisposed = new BooleanDisposable();
            _onNext = new Action<T>(OnNext_);
        }

        public IDisposable Run()
        {
            var sourceDisposable = _parent._source.Subscribe(this);
            return new MultipleDisposable(sourceDisposable, _isDisposed);
        }

        public override void OnNext(T value) =>
            _scheduler.Schedule(value, (s, v) =>
            {
                _onNext(v);
                return _isDisposed;
            });

        public override void OnError(Exception error) =>
            _scheduler.Schedule(error, (s, v) =>
            {
                OnError_(v);
                return _isDisposed;
            });

        public override void OnCompleted() =>
            _scheduler.Schedule(() => OnCompleted_(RxVoid.Default));

        private void OnNext_(T value) => Observer.OnNext(value);

        private void OnError_(Exception error)
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

        private void OnCompleted_(RxVoid v)
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
