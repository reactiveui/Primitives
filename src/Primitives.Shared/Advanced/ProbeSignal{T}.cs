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

    /// <summary>Initializes a new instance of the <see cref="ProbeSignal{T}"/> class on the thread-pool sequencer.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="period">The sample period.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="period"/> is negative.</exception>
    public ProbeSignal(IObservable<T> source, TimeSpan period)
        : this(source, period, ThreadPoolSequencer.Instance)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ProbeSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="period">The sample period.</param>
    /// <param name="sequencer">The sequencer used to schedule ticks.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="sequencer"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="period"/> is negative.</exception>
    public ProbeSignal(IObservable<T> source, TimeSpan period, ISequencer sequencer)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(period, TimeSpan.Zero);
        ArgumentExceptionHelper.ThrowIfNull(sequencer);

        _source = source;
        _period = period;
        _sequencer = sequencer;
    }

    /// <summary>Creates a <see cref="ProbeSignal{T}"/> on <paramref name="scheduler"/>, defaulting to the thread pool when <see langword="null"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="period">The sample period.</param>
    /// <param name="scheduler">The sequencer used to schedule ticks, or <see langword="null"/> for the thread pool.</param>
    /// <returns>The sampling signal.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="period"/> is negative.</exception>
    public static ProbeSignal<T> Create(IObservable<T> source, TimeSpan period, ISequencer? scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentOutOfRangeExceptionHelper.ThrowIfLessThan(period, TimeSpan.Zero);

        return new(source, period, scheduler ?? ThreadPoolSequencer.Instance);
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() => _sequencer == Sequencer.CurrentThread;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        ProbeCoordinator<T> coordinator = new(_source, _period, _sequencer, observer);
        return !IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired
            ? coordinator.Run()
            : CurrentThreadSequencer.RunTrampoline(
                coordinator,
                static c => c.Run());
    }
}
