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
    protected override IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel) =>
        new WitnessOn(this, observer, cancel).Run();

    /// <summary>
    /// Represents the WitnessOn class.
    /// </summary>
    private sealed class WitnessOn : WitnessBase<T, T>, IWorkItem, IsDisposed
    {
        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private readonly WitnessOnSignal<T> _parent;

        /// <summary>
        /// Executes the new operation.
        /// </summary>
        /// <returns>The result.</returns>
        private readonly Queue<Spark<T>> _actions = new();

        /// <summary>
        /// Stores state for the signal implementation.
        /// </summary>
        private bool _isDisposed;

        /// <summary>
        /// Tracks whether a drain has been scheduled.
        /// </summary>
        private bool _isScheduled;

        /// <summary>
        /// Initializes a new instance of the <see cref="WitnessOn"/> class.
        /// </summary>
        /// <param name="parent">The parent value.</param>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        public WitnessOn(WitnessOnSignal<T> parent, IObserver<T> observer, IDisposable cancel)
            : base(observer, cancel) => _parent = parent;

        /// <inheritdoc/>
        public bool IsDisposed
        {
            get
            {
                lock (_actions)
                {
                    return _isDisposed;
                }
            }
        }

        /// <summary>
        /// Executes the Run operation.
        /// </summary>
        /// <returns>The result.</returns>
        public MultipleDisposable Run()
        {
            _isDisposed = false;

            var sourceDisposable = _parent._source.Subscribe(this);

            return new MultipleDisposable(sourceDisposable, this);
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
        /// Executes the scheduled queue drain.
        /// </summary>
        public void Execute()
        {
            while (true)
            {
                Spark<T> action;
                lock (_actions)
                {
                    if (_isDisposed)
                    {
                        _isScheduled = false;
                        return;
                    }

                    if (_actions.Count == 0)
                    {
                        _isScheduled = false;
                        return;
                    }

                    action = _actions.Dequeue();
                }

                Dispatch(action);
                if (action.Kind == SparkKind.OnNext)
                {
                    continue;
                }

                Dispose();
                return;
            }
        }

        /// <summary>
        /// Executes the Dispose operation.
        /// </summary>
        /// <param name="disposing">The disposing value.</param>
        protected override void Dispose(bool disposing)
        {
            lock (_actions)
            {
                _isDisposed = true;
                _actions.Clear();
            }

            base.Dispose(disposing);
        }

        /// <summary>
        /// Executes the QueueAction operation.
        /// </summary>
        /// <param name="data">The data value.</param>
        private void QueueAction(Spark<T> data)
        {
            lock (_actions)
            {
                if (_isDisposed)
                {
                    return;
                }

                _actions.Enqueue(data);
                if (_isScheduled)
                {
                    return;
                }

                _isScheduled = true;
            }

            _parent._scheduler.Schedule(this);
        }

        /// <summary>
        /// Executes the Dispatch operation.
        /// </summary>
        /// <param name="action">The action value.</param>
        private void Dispatch(Spark<T> action)
        {
            switch (action.Kind)
            {
                case SparkKind.OnNext:
                    {
                        Observer.OnNext(action.Value);
                        break;
                    }

                case SparkKind.OnError:
                    {
                        Observer.OnError(action.Exception);
                        break;
                    }

                case SparkKind.OnCompleted:
                    {
                        Observer.OnCompleted();
                        break;
                    }
            }
        }
    }
}
