// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Dedicated signal for <c>Calm</c> (quiet-period debounce).</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("CalmSignal: DueTime = {_dueTime}, Source = {_source}")]
public sealed class CalmSignal<T> : IRequireCurrentThread<T>
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
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="scheduler"/> is <see langword="null"/>.</exception>
    public CalmSignal(IObservable<T> source, TimeSpan dueTime, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        _source = source;
        _dueTime = dueTime;
        _scheduler = scheduler;
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() => _scheduler == Sequencer.CurrentThread;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        CalmCoordinator<T> coordinator = new(_source, _dueTime, _scheduler);
        if (!IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired)
        {
            return coordinator.Run(observer);
        }

        SingleDisposable subscription = new();
        _ = Sequencer.CurrentThread.Schedule(
            (subscription, coordinator, observer),
            static (_, s) =>
            {
                s.subscription.Create(s.coordinator.Run(s.observer));
                return EmptyDisposable.Instance;
            });
        return subscription;
    }
}
