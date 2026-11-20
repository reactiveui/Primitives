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

    /// <summary>Initializes a new instance of the <see cref="ExpireSignal{T}"/> class on the thread-pool sequencer.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The timeout period.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public ExpireSignal(IObservable<T> source, TimeSpan dueTime)
        : this(source, dueTime, ThreadPoolSequencer.Instance, restartOnValue: true)
    {
    }

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

    /// <summary>Creates an <see cref="ExpireSignal{T}"/> on <paramref name="scheduler"/>, defaulting to the thread pool when <see langword="null"/>.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The timeout period.</param>
    /// <param name="scheduler">The sequencer that schedules the timeout, or <see langword="null"/> for the thread pool.</param>
    /// <returns>The timeout signal.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static ExpireSignal<T> Create(IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        return new(source, dueTime, scheduler ?? ThreadPoolSequencer.Instance);
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
        return !IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired
            ? coordinator.Run()
            : CurrentThreadSequencer.RunTrampoline(
                coordinator,
                static c => c.Run());
    }
}
