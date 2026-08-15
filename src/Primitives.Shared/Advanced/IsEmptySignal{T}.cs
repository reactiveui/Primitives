// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Signal that emits whether the source completed without values.</summary>
/// <typeparam name="T">The source value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("Source = {Source}")]
public sealed class IsEmptySignal<T> : IRequireCurrentThread<bool>
{
    /// <summary>Initializes a new instance of the <see cref="IsEmptySignal{T}"/> class.</summary>
    /// <param name="source">The source observable.</param>
    public IsEmptySignal(IObservable<T> source)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);

        Source = source;
    }

    /// <summary>Gets the source observable.</summary>
    private IObservable<T> Source { get; }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() =>
        Source is IRequireCurrentThread<T> currentThread && currentThread.IsRequiredSubscribeOnCurrentThread();

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<bool> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (Source is ImmutableEmptySignal<T>)
        {
            observer.OnNext(true);
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        if (Source is RangeSignal { Count: > 0 })
        {
            observer.OnNext(false);
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        }

        // The first value settles this operator, so it must be able to dispose the source as soon as one arrives.
        // A current-thread source runs its work on the trampoline of whichever call enters it first; if that call is
        // the source's own Subscribe, the source drains the trampoline before the sink is handed the subscription,
        // and an endless source therefore never stops. Entering the trampoline here means the source only queues its
        // first tick and returns, so the sink owns the subscription before that tick is delivered.
        if (!IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired)
        {
            return SubscribeCore(observer);
        }

        SingleDisposable subscription = new();
        _ = Sequencer.CurrentThread.Schedule(
            (Self: this, subscription, observer),
            static (_, s) =>
            {
                s.subscription.Create(s.Self.SubscribeCore(s.observer));
                return EmptyDisposable.Instance;
            });
        return subscription;
    }

    /// <summary>Subscribes the emptiness sink to the source.</summary>
    /// <param name="observer">The downstream observer.</param>
    /// <returns>The sink that owns the upstream subscription.</returns>
    private IsEmptyWitness<T> SubscribeCore(IObserver<bool> observer)
    {
        IsEmptyWitness<T> sink = new(observer);
        sink.SetSubscription(Source.Subscribe(sink));
        return sink;
    }
}
