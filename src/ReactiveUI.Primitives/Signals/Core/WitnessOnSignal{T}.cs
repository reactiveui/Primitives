// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Represents the WitnessOnSignal class.
/// </summary>
/// <typeparam name="T">The T type.</typeparam>
[System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
internal sealed class WitnessOnSignal<T> : SignalsBase<T>
{
    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly IObservable<T> _source;

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly ISequencer _scheduler;

    /// <summary>
    /// Initializes a new instance of the <see cref="WitnessOnSignal{T}"/> class.
    /// </summary>
    /// <param name="source">The source value.</param>
    /// <param name="scheduler">The scheduler value.</param>
    public WitnessOnSignal(IObservable<T> source, ISequencer scheduler)
        : base(true)
    {
        _source = source;
        _scheduler = scheduler;
    }

    /// <summary>
    /// Executes the SubscribeCore operation.
    /// </summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    /// <returns>The result.</returns>
    protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        if (_scheduler is not ThreadPoolSequencer queueing)
        {
            return new WitnessOn(this, observer, cancel).Run();
        }

        return new ThreadPoolWitnessOn(this, queueing, observer, cancel).Run();
    }

    /// <summary>
    /// Represents the WitnessOn class.
    /// </summary>
    private sealed class WitnessOn : WitnessBase<T, T>
    {
        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private readonly WitnessOnSignal<T> _parent;

        /// <summary>
        /// Executes the new operation.
        /// </summary>
        /// <returns>The result.</returns>
        private readonly LinkedList<SchedulableAction> _actions = new();

        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private bool _isDisposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="WitnessOn"/> class.
        /// </summary>
        /// <param name="parent">The parent value.</param>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        public WitnessOn(WitnessOnSignal<T> parent, IObserver<T> observer, IDisposable cancel)
            : base(observer, cancel) => _parent = parent;

        /// <summary>
        /// Executes the Run operation.
        /// </summary>
        /// <returns>The result.</returns>
        public MultipleDisposable Run()
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

        /// <summary>
        /// Executes the OnNext operation.
        /// </summary>
        /// <param name="value">The value.</param>
        public override void OnNext(T value) => QueueAction(new Spark<T>.OnNextSpark(value));

        /// <summary>
        /// Executes the OnError operation.
        /// </summary>
        /// <param name="error">The error value.</param>
        public override void OnError(Exception error) => QueueAction(new Spark<T>.OnErrorSpark(error));

        /// <summary>
        /// Executes the OnCompleted operation.
        /// </summary>
        public override void OnCompleted() => QueueAction(new Spark<T>.OnCompletedSpark());

        /// <summary>
        /// Executes the QueueAction operation.
        /// </summary>
        /// <param name="data">The data value.</param>
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

        /// <summary>
        /// Executes the ProcessNext operation.
        /// </summary>
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
                        Dispatch(action);
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

        /// <summary>
        /// Executes the Dispatch operation.
        /// </summary>
        /// <param name="action">The action value.</param>
        private void Dispatch(SchedulableAction action)
        {
            switch (action.Data.Kind)
            {
                case SparkKind.OnNext:
                    {
                        Observer.OnNext(action.Data.Value);
                        break;
                    }

                case SparkKind.OnError:
                    {
                        Observer.OnError(action.Data.Exception);
                        break;
                    }

                case SparkKind.OnCompleted:
                    {
                        Observer.OnCompleted();
                        break;
                    }
            }
        }

        /// <summary>
        /// Represents the SchedulableAction class.
        /// </summary>
        private sealed class SchedulableAction : IDisposable
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="SchedulableAction"/> class.
            /// </summary>
            /// <param name="data">The data value.</param>
            public SchedulableAction(Spark<T> data)
            {
                Data = data;
            }

            /// <summary>
            /// Gets the value.
            /// </summary>
            public Spark<T> Data { get; }

            /// <summary>
            /// Gets or sets the value.
            /// </summary>
            public LinkedListNode<SchedulableAction>? Node { get; set; }

            /// <summary>
            /// Gets or sets the value.
            /// </summary>
            public IDisposable? Schedule { get; set; }

            /// <summary>
            /// Gets a value indicating whether the condition is met.
            /// </summary>
            public bool IsScheduled => Schedule != null;

            /// <summary>
            /// Executes the Dispose operation.
            /// </summary>
            public void Dispose()
            {
                Schedule?.Dispose();

                Schedule = null;

                if (Node?.List == null)
                {
                    return;
                }

                Node.List.Remove(Node);
            }
        }
    }

    /// <summary>
    /// Represents the ThreadPoolWitnessOn class.
    /// </summary>
    private sealed class ThreadPoolWitnessOn : WitnessBase<T, T>
    {
        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private readonly WitnessOnSignal<T> _parent;

        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private readonly ThreadPoolSequencer _scheduler;

        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private readonly BooleanDisposable _isDisposed;

        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private readonly Action<T> _onNext;

        /// <summary>
        /// Initializes a new instance of the <see cref="ThreadPoolWitnessOn"/> class.
        /// </summary>
        /// <param name="parent">The parent value.</param>
        /// <param name="scheduler">The scheduler value.</param>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        public ThreadPoolWitnessOn(WitnessOnSignal<T> parent, ThreadPoolSequencer scheduler, IObserver<T> observer, IDisposable cancel)
            : base(observer, cancel)
        {
            _parent = parent;
            _scheduler = scheduler;
            _isDisposed = new BooleanDisposable();
            _onNext = OnNextCore;
        }

        /// <summary>
        /// Executes the Run operation.
        /// </summary>
        /// <returns>The result.</returns>
        public MultipleDisposable Run()
        {
            var sourceDisposable = _parent._source.Subscribe(this);
            return new MultipleDisposable(sourceDisposable, _isDisposed);
        }

        /// <summary>
        /// Executes the OnNext operation.
        /// </summary>
        /// <param name="value">The value.</param>
        public override void OnNext(T value) =>
            _scheduler.Schedule(value, (_, v) =>
            {
                _onNext(v);
                return _isDisposed;
            });

        /// <summary>
        /// Executes the OnError operation.
        /// </summary>
        /// <param name="error">The error value.</param>
        public override void OnError(Exception error) =>
            _scheduler.Schedule(error, (_, v) =>
            {
                OnErrorCore(v);
                return _isDisposed;
            });

        /// <summary>
        /// Executes the OnCompleted operation.
        /// </summary>
        public override void OnCompleted() =>
            _scheduler.Schedule(OnCompletedCore);

        /// <summary>
        /// Executes the Dispose operation.
        /// </summary>
        /// <param name="disposing">The disposing value.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _isDisposed.Dispose();
            }

            base.Dispose(disposing);
        }

        /// <summary>
        /// Executes the OnNextCore operation.
        /// </summary>
        /// <param name="value">The value.</param>
        private void OnNextCore(T value) => Observer.OnNext(value);

        /// <summary>
        /// Executes the OnErrorCore operation.
        /// </summary>
        /// <param name="error">The error value.</param>
        private void OnErrorCore(Exception error)
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

        /// <summary>
        /// Executes the OnCompletedCore operation.
        /// </summary>
        private void OnCompletedCore()
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
