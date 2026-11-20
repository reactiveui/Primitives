// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Dedicated signal for <c>Shift</c> (delay each notification on a sequencer).</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("ShiftSignal: DueTime = {_dueTime}, Source = {_source}")]
public sealed class ShiftSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>The source observable.</summary>
    private readonly IObservable<T> _source;

    /// <summary>The delay applied to each notification.</summary>
    private readonly TimeSpan _dueTime;

    /// <summary>The sequencer used to schedule delayed notifications.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>Initializes a new instance of the <see cref="ShiftSignal{T}"/> class on the thread-pool sequencer.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The delay applied to each notification.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public ShiftSignal(IObservable<T> source, TimeSpan dueTime)
        : this(source, dueTime, ThreadPoolSequencer.Instance)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ShiftSignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The delay applied to each notification.</param>
    /// <param name="scheduler">The sequencer used to schedule delayed notifications.</param>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> or <paramref name="scheduler"/> is <see langword="null"/>.</exception>
    public ShiftSignal(IObservable<T> source, TimeSpan dueTime, ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        _source = source;
        _dueTime = Sequencer.Normalize(dueTime);
        _scheduler = scheduler;
    }

    /// <summary>Creates a delayed notification sequence on the thread-pool sequencer, using <see cref="ShiftedRangeSignal{T}"/> for integer ranges.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The delay applied to each notification.</param>
    /// <returns>The shifted observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static IObservable<T> Create(IObservable<T> source, TimeSpan dueTime) =>
        Create(source, dueTime, ThreadPoolSequencer.Instance);

    /// <summary>Creates a delayed notification sequence on <paramref name="scheduler"/>, using <see cref="ShiftedRangeSignal{T}"/> for integer ranges.</summary>
    /// <param name="source">The source observable.</param>
    /// <param name="dueTime">The delay applied to each notification.</param>
    /// <param name="scheduler">The sequencer used to schedule delayed notifications, or <see langword="null"/> for the thread pool.</param>
    /// <returns>The shifted observable sequence.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is <see langword="null"/>.</exception>
    public static IObservable<T> Create(IObservable<T> source, TimeSpan dueTime, ISequencer? scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        scheduler ??= ThreadPoolSequencer.Instance;
        return source is RangeSignal range && typeof(T) == typeof(int)
            ? new ShiftedRangeSignal<T>(range, Sequencer.Normalize(dueTime), scheduler)
            : new ShiftSignal<T>(source, dueTime, scheduler);
    }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() => _scheduler == Sequencer.CurrentThread;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return !IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired
            ? RunCore(observer)
            : CurrentThreadSequencer.RunTrampoline(
                (Self: this, observer),
                static s => s.Self.RunCore(s.observer));
    }

    /// <summary>Subscribes to the source and schedules each notification by the delay.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The disposable that cancels the source subscription and pending timers.</returns>
    private ShiftCoordinator<T> RunCore(IObserver<T> observer)
    {
        ShiftCoordinator<T> coordinator = new(_source, _dueTime, _scheduler, observer);
        return coordinator.Run();
    }
}
