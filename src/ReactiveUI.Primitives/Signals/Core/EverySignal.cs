// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Signals.Core;

/// <summary>
/// Dedicated signal for the interval timer factory (<c>Every</c>), replacing the self-referencing
/// <c>CreateSafe</c> closure with a coordinator that reschedules itself through a method group.
/// </summary>
internal sealed class EverySignal : IRequireCurrentThread<long>
{
    /// <summary>The interval between ticks.</summary>
    private readonly TimeSpan _period;

    /// <summary>The sequencer used to schedule ticks.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>Initializes a new instance of the <see cref="EverySignal"/> class.</summary>
    /// <param name="period">The interval between ticks.</param>
    /// <param name="scheduler">The sequencer used to schedule ticks.</param>
    internal EverySignal(TimeSpan period, ISequencer scheduler)
    {
        _period = period;
        _scheduler = scheduler;
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() => _scheduler == Sequencer.CurrentThread;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<long> observer)
    {
        if (observer is null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        var coordinator = new EveryCoordinator(observer, _scheduler, _period);
        if (!IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired)
        {
            return coordinator.Run();
        }

        var subscription = new SingleDisposable();
        Sequencer.CurrentThread.Schedule(() => subscription.Create(coordinator.Run()));
        return subscription;
    }

    /// <summary>Reschedules the recurring tick without a captured closure.</summary>
    private sealed class EveryCoordinator : IDisposable
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<long> _observer;

        /// <summary>The sequencer used to schedule ticks.</summary>
        private readonly ISequencer _scheduler;

        /// <summary>The interval between ticks.</summary>
        private readonly TimeSpan _period;

        /// <summary>The cancellation slot for the current scheduled tick.</summary>
        private readonly SingleReplaceableDisposable _slot = new();

        /// <summary>Cached tick callback, reused across reschedules to avoid per-tick delegate allocation.</summary>
        private readonly Action _tickAction;

        /// <summary>The next tick index to emit.</summary>
        private long _tick;

        /// <summary>Initializes a new instance of the <see cref="EveryCoordinator"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        /// <param name="scheduler">The sequencer used to schedule ticks.</param>
        /// <param name="period">The interval between ticks.</param>
        internal EveryCoordinator(IObserver<long> observer, ISequencer scheduler, TimeSpan period)
        {
            _observer = observer;
            _scheduler = scheduler;
            _period = period;
            _tickAction = Tick;
        }

        /// <inheritdoc/>
        public void Dispose() => _slot.Dispose();

        /// <summary>Schedules the first tick and returns the coordinator as the subscription.</summary>
        /// <returns>The disposable that cancels the recurring schedule.</returns>
        internal EveryCoordinator Run()
        {
            ScheduleNext();
            return this;
        }

        /// <summary>Schedules the next tick into the cancellation slot.</summary>
        private void ScheduleNext() => _slot.Create(_scheduler.Schedule(_period, _tickAction));

        /// <summary>Emits the current tick and reschedules unless cancelled.</summary>
        private void Tick()
        {
            _observer.OnNext(_tick++);
            if (_slot.IsDisposed)
            {
                return;
            }

            ScheduleNext();
        }
    }
}
