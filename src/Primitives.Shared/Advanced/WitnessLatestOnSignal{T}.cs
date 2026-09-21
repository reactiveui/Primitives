// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Re-dispatches source notifications onto a sequencer, delivering only the newest value that is waiting.</summary>
/// <typeparam name="T">The value type.</typeparam>
/// <param name="source">The source observable.</param>
/// <param name="scheduler">The sequencer that dispatches the notifications.</param>
[System.Diagnostics.DebuggerDisplay("WitnessLatestOnSignal<{typeof(T).Name,nq}>")]
public sealed class WitnessLatestOnSignal<T>(IObservable<T> source, ISequencer scheduler) : IRequireCurrentThread<T>
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
        new WitnessLatestOn(this, observer, cancel).Run();

    /// <summary>Holds the newest source value and delivers it on the sequencer.</summary>
    /// <param name="parent">The owning signal.</param>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The subscription handle released on teardown.</param>
    private sealed class WitnessLatestOn(WitnessLatestOnSignal<T> parent, IObserver<T> observer, IDisposable cancel) : IObserver<T>, IWorkItem, IsDisposed
    {
        /// <summary>The owning signal.</summary>
        private readonly WitnessLatestOnSignal<T> _parent = parent;

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer = observer;

        /// <summary>Synchronization gate guarding the pending slot and scheduling state.</summary>
        private readonly Lock _gate = new();

        /// <summary>Upstream subscription disposed on teardown.</summary>
        private readonly IDisposable _cancel = cancel;

        /// <summary>The newest value awaiting delivery.</summary>
        private T _pending = default!;

        /// <summary>The error awaiting delivery.</summary>
        private Exception? _error;

        /// <summary>Whether <see cref="_pending"/> holds a value.</summary>
        private bool _hasPending;

        /// <summary>Whether completion is awaiting delivery.</summary>
        private bool _hasCompleted;

        /// <summary>Whether the sink has been torn down.</summary>
        private bool _isDisposed;

        /// <summary>Tracks whether a drain is scheduled or running.</summary>
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
            var sourceDisposable = _parent._source.Subscribe(this);

            return new(sourceDisposable, this);
        }

        /// <summary>Stores a value as the newest pending value.</summary>
        /// <param name="value">The value to store.</param>
        public void OnNext(T value)
        {
            lock (_gate)
            {
                if (_isDisposed || _hasCompleted || _error is not null)
                {
                    return;
                }

                _pending = value;
                _hasPending = true;
                if (_isScheduled)
                {
                    return;
                }

                _isScheduled = true;
            }

            _parent._scheduler.Schedule(this);
        }

        /// <summary>Stores the error for delivery after any pending value.</summary>
        /// <param name="error">The error to store.</param>
        public void OnError(Exception error)
        {
            lock (_gate)
            {
                if (_isDisposed || _error is not null)
                {
                    return;
                }

                _error = error;
                if (_isScheduled)
                {
                    return;
                }

                _isScheduled = true;
            }

            _parent._scheduler.Schedule(this);
        }

        /// <summary>Stores completion for delivery after any pending value.</summary>
        public void OnCompleted()
        {
            lock (_gate)
            {
                if (_isDisposed || _hasCompleted || _error is not null)
                {
                    return;
                }

                _hasCompleted = true;
                if (_isScheduled)
                {
                    return;
                }

                _isScheduled = true;
            }

            _parent._scheduler.Schedule(this);
        }

        /// <summary>Executes the scheduled drain.</summary>
        public void Execute()
        {
            while (true)
            {
                var value = default(T)!;
                Exception? error = null;
                var isValue = false;
                lock (_gate)
                {
                    if (_isDisposed)
                    {
                        _isScheduled = false;
                        return;
                    }

                    if (_hasPending)
                    {
                        value = _pending;
                        _pending = default!;
                        _hasPending = false;
                        isValue = true;
                    }
                    else if (_error is not null)
                    {
                        error = _error;
                    }
                    else if (!_hasCompleted)
                    {
                        _isScheduled = false;
                        return;
                    }
                }

                if (isValue)
                {
                    _observer.OnNext(value);
                    continue;
                }

                if (error is not null)
                {
                    _observer.OnError(error);
                }
                else
                {
                    _observer.OnCompleted();
                }

                Dispose();
                return;
            }
        }

        /// <summary>Drops the pending value and releases the upstream subscription, once.</summary>
        public void Dispose()
        {
            lock (_gate)
            {
                if (_isDisposed)
                {
                    return;
                }

                _isDisposed = true;
                _pending = default!;
                _hasPending = false;
            }

            _cancel.Dispose();
        }
    }
}
