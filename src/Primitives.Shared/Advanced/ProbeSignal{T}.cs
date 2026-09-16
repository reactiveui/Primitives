// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Sample signal with a direct subscription path.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ProbeSignal: Period = {_period}, Source = {_source}")]
public sealed class ProbeSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The sample period.</summary>
    private readonly TimeSpan _period;

    /// <summary>The sequencer used to schedule ticks.</summary>
    private readonly ISequencer _sequencer;

    /// <summary>Initializes a new instance of the <see cref="ProbeSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="period">The sample period.</param>
    /// <param name="sequencer">The sequencer used to schedule ticks.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="sequencer"/> is <see langword="null"/>.</exception>
    public ProbeSignal(IObservable<T> source, TimeSpan period, ISequencer sequencer)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(sequencer);

        _source = source;
        _period = period;
        _sequencer = sequencer;
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() => _sequencer == Sequencer.CurrentThread;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        LinqExtensions.ProbeCoordinator<T> coordinator = new(_source, _period, _sequencer, observer);
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
