// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Re-dispatches source notifications onto a sequencer, preserving their order.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="scheduler">The sequencer that dispatches the notifications.</param>
internal sealed class WitnessOnSignal<T>(IObservable<T> source, ISequencer scheduler) : IRequireCurrentThread<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source = source;

    /// <summary>The sequencer that dispatches the notifications.</summary>
    private readonly ISequencer _scheduler = scheduler;

    /// <summary>Reports that subscription is dispatched through the current-thread sequencer.</summary>
    /// <returns>Always <see langword="true"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => true;

    /// <summary>Subscribes an observer through the current-thread sequencer.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The subscription.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer) =>
        SignalSubscription.Subscribe(observer, true, SubscribeCore);

    /// <summary>Builds the dispatching sink and subscribes it to the source.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The subscription handle owned by the subscription helper.</param>
    /// <returns>The source subscription together with the sink.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel) =>
        new WitnessOn(this, observer, cancel).Run();

    /// <summary>Queues source notifications and drains them on the sequencer.</summary>
    /// <param name="parent">The owning signal.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The subscription handle released on teardown.</param>
    private sealed class WitnessOn(WitnessOnSignal<T> parent, IObserver<T> observer, IDisposable cancel) : IObserver<T>, IWorkItem, IsDisposed
    {
        /// <summary>The owning signal.</summary>
        private readonly WitnessOnSignal<T> _parent = parent;

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer = observer;

        /// <summary>Synchronization gate guarding the queued actions and scheduling state.</summary>
        private readonly Lock _gate = new();

        /// <summary>Notifications awaiting dispatch on the sequencer.</summary>
        private readonly Queue<Notification> _actions = new();

        /// <summary>Upstream subscription disposed on teardown.</summary>
        private IDisposable? _cancel = cancel;

        /// <summary>Whether the sink has been torn down.</summary>
        private bool _isDisposed;

        /// <summary>Tracks whether a drain has been scheduled.</summary>
        private bool _isScheduled;

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

        /// <summary>Subscribes to the source.</summary>
        /// <returns>The source subscription together with this sink.</returns>
        public MultipleDisposable Run()
        {
            _isDisposed = false;

            var sourceDisposable = _parent._source.Subscribe(this);

            return new(sourceDisposable, this);
        }

        /// <summary>Queues a value for dispatch.</summary>
        /// <param name="value">The value to queue.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => QueueAction(Notification.OnNext(value));

        /// <summary>Queues an error for dispatch.</summary>
        /// <param name="error">The error to queue.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => QueueAction(Notification.OnError(error));

        /// <summary>Queues completion for dispatch.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

        /// <summary>Clears the queue and releases the upstream subscription, once.</summary>
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

        /// <summary>Queues a notification and schedules a drain when one is not pending.</summary>
        /// <param name="data">The notification to queue.</param>
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

        /// <summary>Forwards one notification to the downstream observer.</summary>
        /// <param name="action">The notification to forward.</param>
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
            /// <param name="value">The value to carry.</param>
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
