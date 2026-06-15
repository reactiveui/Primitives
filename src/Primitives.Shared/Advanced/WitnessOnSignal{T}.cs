// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Represents the WitnessOnSignal class.</summary>
/// <typeparam name="T">The T type.</typeparam>
internal sealed class WitnessOnSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>Stores state for the signal implementation.</summary>
    private readonly IObservable<T> _source;

    /// <summary>Stores state for the signal implementation.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>Initializes a new instance of the <see cref="WitnessOnSignal{T}"/> class.</summary>
    /// <param name="source">The source value.</param>
    /// <param name="scheduler">The scheduler value.</param>
    public WitnessOnSignal(IObservable<T> source, ISequencer scheduler)
    {
        _source = source;
        _scheduler = scheduler;
    }

    /// <summary>Executes the IsRequiredSubscribeOnCurrentThread operation.</summary>
    /// <returns>The result.</returns>
    public bool IsRequiredSubscribeOnCurrentThread() => true;

    /// <summary>Executes the Subscribe operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <returns>The result.</returns>
    public IDisposable Subscribe(IObserver<T> observer) =>
        SignalSubscription.Subscribe(observer, true, SubscribeCore);

    /// <summary>Executes the SubscribeCore operation.</summary>
    /// <param name="observer">The observer value.</param>
    /// <param name="cancel">The cancel value.</param>
    /// <returns>The result.</returns>
    private IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel) =>
        new WitnessOn(this, observer, cancel).Run();

    /// <summary>Represents the WitnessOn class.</summary>
    private sealed class WitnessOn : IObserver<T>, IWorkItem, IsDisposed
    {
        /// <summary>Stores state for the signal implementation.</summary>
        private readonly WitnessOnSignal<T> _parent;

        /// <summary>Stores the downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Synchronization gate guarding the queued actions and scheduling state.</summary>
        private readonly Lock _gate = new();

        /// <summary>
        /// Queued notifications awaiting dispatch on the scheduler. Stored as a value type so
        /// queueing a notification does not allocate a <see cref="Spark{T}"/> per OnNext.
        /// </summary>
        private readonly Queue<Notification> _actions = new();

        /// <summary>Upstream subscription disposed on teardown.</summary>
        private IDisposable? _cancel;

        /// <summary>Stores state for the signal implementation.</summary>
        private bool _isDisposed;

        /// <summary>Tracks whether a drain has been scheduled.</summary>
        private bool _isScheduled;

        /// <summary>Initializes a new instance of the <see cref="WitnessOn"/> class.</summary>
        /// <param name="parent">The parent value.</param>
        /// <param name="observer">The observer value.</param>
        /// <param name="cancel">The cancel value.</param>
        public WitnessOn(WitnessOnSignal<T> parent, IObserver<T> observer, IDisposable cancel)
        {
            _parent = parent;
            _observer = observer;
            _cancel = cancel;
        }

        /// <inheritdoc/>
        public bool IsDisposed
        {
            get
            {
                lock (_gate)
                {
                    return _isDisposed;
                }
            }
        }

        /// <summary>Executes the Run operation.</summary>
        /// <returns>The result.</returns>
        public MultipleDisposable Run()
        {
            _isDisposed = false;

            var sourceDisposable = _parent._source.Subscribe(this);

            return new(sourceDisposable, this);
        }

        /// <summary>Executes the OnNext operation.</summary>
        /// <param name="value">The value.</param>
        public void OnNext(T value) => QueueAction(Notification.OnNext(value));

        /// <summary>Executes the OnError operation.</summary>
        /// <param name="error">The error value.</param>
        public void OnError(Exception error) => QueueAction(Notification.OnError(error));

        /// <summary>Executes the OnCompleted operation.</summary>
        public void OnCompleted() => QueueAction(Notification.OnCompleted());

        /// <summary>Executes the scheduled queue drain.</summary>
        public void Execute()
        {
            while (true)
            {
                Notification action;
                lock (_gate)
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

                Dispatch(in action);
                if (action.Kind == SparkKind.OnNext)
                {
                    continue;
                }

                Dispose();
                return;
            }
        }

        /// <summary>Executes the Dispose operation.</summary>
        public void Dispose()
        {
            IDisposable? cancel;
            lock (_gate)
            {
                if (_isDisposed)
                {
                    return;
                }

                _isDisposed = true;
                _actions.Clear();
                cancel = _cancel;
                _cancel = null;
            }

            cancel?.Dispose();
        }

        /// <summary>Executes the QueueAction operation.</summary>
        /// <param name="data">The data value.</param>
        private void QueueAction(in Notification data)
        {
            lock (_gate)
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

        /// <summary>Executes the Dispatch operation.</summary>
        /// <param name="action">The action value.</param>
        private void Dispatch(in Notification action)
        {
            switch (action.Kind)
            {
                case SparkKind.OnNext:
                    {
                        _observer.OnNext(action.Value);
                        break;
                    }

                case SparkKind.OnError:
                    {
                        _observer.OnError(action.Exception);
                        break;
                    }

                case SparkKind.OnCompleted:
                    {
                        _observer.OnCompleted();
                        break;
                    }
            }
        }

        /// <summary>Value-type observer notification used for the dispatch queue to avoid a per-OnNext heap allocation.</summary>
        private readonly record struct Notification
        {
            /// <summary>Initializes a new instance of the <see cref="Notification"/> struct.</summary>
            /// <param name="kind">The notification kind.</param>
            /// <param name="value">The OnNext value.</param>
            /// <param name="exception">The OnError exception.</param>
            private Notification(SparkKind kind, T value, Exception? exception)
            {
                Kind = kind;
                Value = value;
                Exception = exception!;
            }

            /// <summary>Gets the notification kind.</summary>
            public SparkKind Kind { get; }

            /// <summary>Gets the value for an OnNext notification.</summary>
            public T Value { get; }

            /// <summary>Gets the exception for an OnError notification.</summary>
            public Exception Exception { get; }

            /// <summary>Creates an OnNext notification.</summary>
            /// <param name="value">The value.</param>
            /// <returns>The notification.</returns>
            public static Notification OnNext(T value) => new(SparkKind.OnNext, value, null);

            /// <summary>Creates an OnError notification.</summary>
            /// <param name="error">The error.</param>
            /// <returns>The notification.</returns>
            public static Notification OnError(Exception error) => new(SparkKind.OnError, default!, error);

            /// <summary>Creates an OnCompleted notification.</summary>
            /// <returns>The notification.</returns>
            public static Notification OnCompleted() => new(SparkKind.OnCompleted, default!, null);
        }
    }
}
