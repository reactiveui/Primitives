// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Terminates with a <see cref="TimeoutException"/> when the source stays quiet longer than the timeout period.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ExpireSignal: DueTime = {_dueTime}, Source = {_source}")]
public sealed class ExpireSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The timeout period.</summary>
    private readonly TimeSpan _dueTime;

    /// <summary>The sequencer that schedules the timeout.</summary>
    private readonly ISequencer _sequencer;

    /// <summary>Whether a value restarts the timeout window.</summary>
    private readonly bool _restartOnValue;

    /// <summary>Initializes a new instance of the <see cref="ExpireSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The timeout period.</param>
    /// <param name="sequencer">The sequencer that schedules the timeout.</param>
    public ExpireSignal(IObservable<T> source, TimeSpan dueTime, ISequencer sequencer)
        : this(source, dueTime, sequencer, restartOnValue: true)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ExpireSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The timeout period.</param>
    /// <param name="sequencer">The sequencer that schedules the timeout.</param>
    /// <param name="restartOnValue">When <see langword="true"/> each value restarts the window; when <see langword="false"/> the window runs once from subscription.</param>
    internal ExpireSignal(IObservable<T> source, TimeSpan dueTime, ISequencer sequencer, bool restartOnValue)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        ArgumentExceptionHelper.ThrowIfNull(sequencer);

        _source = source;
        _dueTime = dueTime;
        _sequencer = sequencer;
        _restartOnValue = restartOnValue;
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        _sequencer == Sequencer.CurrentThread
        || (_source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread());

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        ExpireCoordinator<T> coordinator = new(_source, _dueTime, _sequencer, observer, _restartOnValue);
        if (!IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired)
        {
            return coordinator.Run();
        }

        SingleDisposable subscription = new();
        _ = Sequencer.CurrentThread.Schedule(
            (subscription, coordinator),
            static (_, s) =>
            {
                s.subscription.Create(s.coordinator.Run());
                return EmptyDisposable.Instance;
            });
        return subscription;
    }
}
