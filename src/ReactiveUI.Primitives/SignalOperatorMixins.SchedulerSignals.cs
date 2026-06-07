// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives;

/// <content>
/// Dedicated signals for the scheduler/time operators, replacing the per-subscription
/// <c>Signal.Create(observer =&gt; ...)</c> closures. The current-thread variants follow the
/// <c>ExpireSignal</c>/<c>ProbeSignal</c> pattern: implement <see cref="Core.IRequireCurrentThread{T}"/>
/// and schedule the subscription onto the current-thread sequencer when required.
/// </content>
public static partial class LinqMixins
{
    /// <summary>Dedicated signal for <c>Calm</c> (quiet-period debounce).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class CalmSignal<T> : Core.IRequireCurrentThread<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The quiet period.</summary>
        private readonly TimeSpan _dueTime;

        /// <summary>The sequencer used to schedule quiet-period timers.</summary>
        private readonly ISequencer _scheduler;

        /// <summary>Initializes a new instance of the <see cref="CalmSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="dueTime">The quiet period.</param>
        /// <param name="scheduler">The sequencer used to schedule quiet-period timers.</param>
        internal CalmSignal(IObservable<T> source, TimeSpan dueTime, ISequencer scheduler)
        {
            _source = source;
            _dueTime = dueTime;
            _scheduler = scheduler;
        }

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() => _scheduler == Sequencer.CurrentThread;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var coordinator = new CalmCoordinator<T>(_source, _dueTime, _scheduler);
            if (!IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired)
            {
                return coordinator.Run(observer);
            }

            var subscription = new SingleDisposable();
            Sequencer.CurrentThread.Schedule(() => subscription.Create(coordinator.Run(observer)));
            return subscription;
        }
    }

    /// <summary>Dedicated signal for <c>Shift</c> (delay each notification on a sequencer).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class ShiftSignal<T> : Core.IRequireCurrentThread<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The delay applied to each notification.</summary>
        private readonly TimeSpan _dueTime;

        /// <summary>The sequencer used to schedule delayed notifications.</summary>
        private readonly ISequencer _scheduler;

        /// <summary>Initializes a new instance of the <see cref="ShiftSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="dueTime">The delay applied to each notification.</param>
        /// <param name="scheduler">The sequencer used to schedule delayed notifications.</param>
        internal ShiftSignal(IObservable<T> source, TimeSpan dueTime, ISequencer scheduler)
        {
            _source = source;
            _dueTime = dueTime;
            _scheduler = scheduler;
        }

        /// <inheritdoc/>
        public bool IsRequiredSubscribeOnCurrentThread() => _scheduler == Sequencer.CurrentThread;

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            if (!IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired)
            {
                return RunCore(observer);
            }

            var subscription = new SingleDisposable();
            Sequencer.CurrentThread.Schedule(() => subscription.Create(RunCore(observer)));
            return subscription;
        }

        /// <summary>Subscribes to the source and schedules each notification by the delay.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <returns>The disposable that cancels the source subscription and pending timers.</returns>
        private MultipleDisposable RunCore(IObserver<T> observer)
        {
            var pocket = new MultipleDisposable();
            pocket.Add(_source.Subscribe(
                value => pocket.Add(_scheduler.Schedule(_dueTime, () => observer.OnNext(value))),
                error => pocket.Add(_scheduler.Schedule(_dueTime, () => observer.OnError(error))),
                () => pocket.Add(_scheduler.Schedule(_dueTime, observer.OnCompleted))));
            return pocket;
        }
    }

    /// <summary>Dedicated signal for <c>SubscribeOn</c> (defer subscription to a sequencer).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class SubscribeOnSignal<T> : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The sequencer the subscription is scheduled onto.</summary>
        private readonly ISequencer _scheduler;

        /// <summary>Initializes a new instance of the <see cref="SubscribeOnSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="scheduler">The sequencer the subscription is scheduled onto.</param>
        internal SubscribeOnSignal(IObservable<T> source, ISequencer scheduler)
        {
            _source = source;
            _scheduler = scheduler;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var subscription = new SingleReplaceableDisposable();
            var scheduled = _scheduler.Schedule(() => subscription.Create(_source.Subscribe(observer)));
            return new MultipleDisposable(scheduled, subscription);
        }
    }

    /// <summary>Dedicated signal for <c>DelayStart</c> (delay the subscription itself).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class DelayStartSignal<T> : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The delay before subscribing to the source.</summary>
        private readonly TimeSpan _dueTime;

        /// <summary>The sequencer used to schedule the delayed subscription.</summary>
        private readonly ISequencer _scheduler;

        /// <summary>Initializes a new instance of the <see cref="DelayStartSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="dueTime">The delay before subscribing to the source.</param>
        /// <param name="scheduler">The sequencer used to schedule the delayed subscription.</param>
        internal DelayStartSignal(IObservable<T> source, TimeSpan dueTime, ISequencer scheduler)
        {
            _source = source;
            _dueTime = dueTime;
            _scheduler = scheduler;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            var pocket = new MultipleDisposable();
            pocket.Add(_scheduler.Schedule(Sequencer.Normalize(_dueTime), () => pocket.Add(_source.Subscribe(observer))));
            return pocket;
        }
    }

    /// <summary>Dedicated signal for <c>Reattempt</c> (retry on error).</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class ReattemptSignal<T> : IObservable<T>
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The maximum number of retries after the initial subscription.</summary>
        private readonly int _retryCount;

        /// <summary>Initializes a new instance of the <see cref="ReattemptSignal{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="retryCount">The maximum number of retries after the initial subscription.</param>
        internal ReattemptSignal(IObservable<T> source, int retryCount)
        {
            _source = source;
            _retryCount = retryCount;
        }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            if (observer == null)
            {
                throw new ArgumentNullException(nameof(observer));
            }

            return new ReattemptCoordinator<T>(_source, _retryCount, observer).Run();
        }
    }

    /// <summary>Coordinates retry-on-error resubscription for <c>Reattempt</c>.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class ReattemptCoordinator<T> : IDisposable
    {
        /// <summary>The source observable.</summary>
        private readonly IObservable<T> _source;

        /// <summary>The maximum number of retries.</summary>
        private readonly int _retryCount;

        /// <summary>The downstream observer.</summary>
        private readonly IObserver<T> _observer;

        /// <summary>Active subscriptions across retries.</summary>
        private readonly MultipleDisposable _pocket = new();

        /// <summary>The number of retries attempted so far.</summary>
        private int _attempts;

        /// <summary>Initializes a new instance of the <see cref="ReattemptCoordinator{T}"/> class.</summary>
        /// <param name="source">The source observable.</param>
        /// <param name="retryCount">The maximum number of retries.</param>
        /// <param name="observer">The downstream observer.</param>
        internal ReattemptCoordinator(IObservable<T> source, int retryCount, IObserver<T> observer)
        {
            _source = source;
            _retryCount = retryCount;
            _observer = observer;
        }

        /// <inheritdoc/>
        public void Dispose() => _pocket.Dispose();

        /// <summary>Starts the first subscription attempt.</summary>
        /// <returns>The coordinator that owns the subscription cleanup.</returns>
        internal ReattemptCoordinator<T> Run()
        {
            SubscribeNext();
            return this;
        }

        /// <summary>Subscribes to the source for the current attempt.</summary>
        private void SubscribeNext() =>
            _pocket.Add(_source.Subscribe(_observer.OnNext, OnError, _observer.OnCompleted));

        /// <summary>Retries the subscription, or forwards the error once retries are exhausted.</summary>
        /// <param name="error">The error raised by the source.</param>
        private void OnError(Exception error)
        {
            if (_attempts++ < _retryCount)
            {
                SubscribeNext();
            }
            else
            {
                _observer.OnError(error);
            }
        }
    }
}
