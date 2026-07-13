// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Runs a function on a scheduler and emits its result.</summary>
/// <typeparam name="T">The result type.</typeparam>
public sealed class StartSignal<T> : IRequireCurrentThread<T>
{
    /// <summary>Initializes a new instance of the <see cref="StartSignal{T}"/> class.</summary>
    /// <param name="function">The function to run.</param>
    /// <param name="scheduler">The scheduler used to run the function.</param>
    public StartSignal(Func<T> function, ISequencer scheduler)
    {
        Function = function;
        Scheduler = scheduler;
    }

    /// <summary>Gets the function to run.</summary>
    private Func<T> Function { get; }

    /// <summary>Gets the scheduler used to run the function.</summary>
    private ISequencer Scheduler { get; }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() => Scheduler == Sequencer.CurrentThread;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        if (Scheduler == Sequencer.Immediate)
        {
            Run(observer);
            return EmptyDisposable.Instance;
        }

        if (!IsRequiredSubscribeOnCurrentThread() || !CurrentThreadSequencer.IsScheduleRequired)
        {
            return Scheduler.Schedule(
                (self: this, observer),
                static (_, s) =>
                {
                    s.self.Run(s.observer);
                    return EmptyDisposable.Instance;
                });
        }

        SingleDisposable subscription = new();
        _ = Sequencer.CurrentThread.Schedule(
            (self: this, subscription, observer),
            static (_, s) =>
            {
                s.subscription.Create(
                    s.self.Scheduler.Schedule(
                        (s.self, s.observer),
                        static (_, inner) =>
                        {
                            inner.self.Run(inner.observer);
                            return EmptyDisposable.Instance;
                        }));
                return EmptyDisposable.Instance;
            });
        return subscription;
    }

    /// <summary>Runs the function and forwards its terminal notification.</summary>
    /// <param name="observer">The downstream observer.</param>
    private void Run(IObserver<T> observer)
    {
        try
        {
            observer.OnNext(Function());
            observer.OnCompleted();
        }
        catch (Exception error)
        {
            observer.OnError(error);
        }
    }
}
