// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Emits a scheduled integer sequence.</summary>
public sealed class SequenceSignal : IRequireCurrentThread<int>
{
    /// <summary>Initializes a new instance of the <see cref="SequenceSignal"/> class.</summary>
    /// <param name="start">The first value to emit.</param>
    /// <param name="count">The number of values to emit.</param>
    /// <param name="scheduler">The scheduler used to emit values.</param>
    public SequenceSignal(int start, int count, ISequencer scheduler)
    {
        Start = start;
        Count = count;
        Scheduler = scheduler;
    }

    /// <summary>Gets the first value to emit.</summary>
    private int Start { get; }

    /// <summary>Gets the number of values to emit.</summary>
    private int Count { get; }

    /// <summary>Gets the scheduler used to emit values.</summary>
    private ISequencer Scheduler { get; }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() => Scheduler == Sequencer.CurrentThread;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<int> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (!IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired)
        {
            return Scheduler.Schedule((this, observer), static (_, state) => state.Item1.Emit(state.observer));
        }

        SingleDisposable subscription = new();
        _ = Sequencer.CurrentThread.Schedule(() => subscription.Create(Scheduler.Schedule((this, observer), static (_, state) => state.Item1.Emit(state.observer))));
        return subscription;
    }

    /// <summary>Emits the sequence on the scheduled callback.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>An empty disposable.</returns>
    private EmptyDisposable Emit(IObserver<int> observer)
    {
        for (var i = 0; i < Count; i++)
        {
            observer.OnNext(Start + i);
        }

        observer.OnCompleted();
        return EmptyDisposable.Instance;
    }
}
