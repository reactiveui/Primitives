// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Completes without emitting a value, delivering completion on the supplied sequencer.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("EmptySignal: Scheduler = {_scheduler}")]
public sealed class EmptySignal<T> : IRequireCurrentThread<T>
{
    /// <summary>The sequencer that delivers completion.</summary>
    private readonly ISequencer _scheduler;

    /// <summary>Initializes a new instance of the <see cref="EmptySignal{T}"/> class.</summary>
    /// <param name="scheduler">The sequencer that delivers completion.</param>
    public EmptySignal(ISequencer scheduler) => _scheduler = scheduler;

    /// <summary>Reports that subscription needs no current-thread dispatch.</summary>
    /// <returns>Always <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsRequiredSubscribeOnCurrentThread() => false;

    /// <summary>Subscribes the observer and arranges its completion.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The disposable that cancels a completion not yet delivered.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IDisposable Subscribe(IObserver<T> observer) =>
        SignalSubscription.Subscribe(observer, false, SubscribeCore);

    /// <summary>Completes the observer inline on the immediate sequencer, otherwise schedules the completion.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <param name="cancel">The outer subscription handle.</param>
    /// <returns>The disposable that cancels a scheduled completion.</returns>
    private IDisposable SubscribeCore(IObserver<T> observer, IDisposable cancel)
    {
        observer = new GuardedWitness<T>(observer, cancel);

        if (_scheduler.IsImmediate)
        {
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        return _scheduler.Schedule(observer.OnCompleted);
    }
}
