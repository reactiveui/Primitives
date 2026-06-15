// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>
/// Dedicated signal for the one-shot timer factory (<c>After</c>), replacing the
/// <c>CreateSafe</c> closure with a parameter-holding signal and a coordinator that carries the
/// observer without a captured display class.
/// </summary>
internal sealed class AfterSignal : IRequireCurrentThread<long>
{
    /// <summary>The delay before the single tick.</summary>
    private readonly TimeSpan _dueTime;

    /// <summary>The sequencer used to schedule the tick.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>Initializes a new instance of the <see cref="AfterSignal"/> class.</summary>
    /// <param name="dueTime">The delay before the single tick.</param>
    /// <param name="scheduler">The sequencer used to schedule the tick.</param>
    internal AfterSignal(TimeSpan dueTime, ISequencer scheduler)
    {
        _dueTime = dueTime;
        _scheduler = scheduler;
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() => _scheduler == Sequencer.CurrentThread;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<long> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (!IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired)
        {
            return Run(observer);
        }

        SingleDisposable subscription = new();
        Sequencer.CurrentThread.Schedule(() => subscription.Create(Run(observer)));
        return subscription;
    }

    /// <summary>Schedules the single tick on the sequencer.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The disposable that cancels the pending tick.</returns>
    private IDisposable Run(IObserver<long> observer) =>
        _scheduler.Schedule(Sequencer.Normalize(_dueTime), new AfterCoordinator(observer).Emit);

    /// <summary>Carries the observer for the one-shot tick without a captured closure.</summary>
    private sealed class AfterCoordinator
    {
        /// <summary>The downstream observer.</summary>
        private readonly IObserver<long> _observer;

        /// <summary>Initializes a new instance of the <see cref="AfterCoordinator"/> class.</summary>
        /// <param name="observer">The downstream observer.</param>
        internal AfterCoordinator(IObserver<long> observer) => _observer = observer;

        /// <summary>Emits the single tick and completes.</summary>
        internal void Emit()
        {
            _observer.OnNext(0L);
            _observer.OnCompleted();
        }
    }
}
