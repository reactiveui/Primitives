// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Runs an action on a scheduler and emits an <see cref="RxVoid"/> value when it completes.</summary>
[System.Diagnostics.DebuggerDisplay("StartSignal: Action = {Action}, Scheduler = {Scheduler}")]
public sealed class StartSignal : IRequireCurrentThread<RxVoid>
{
    /// <summary>Initializes a new instance of the <see cref="StartSignal"/> class.</summary>
    /// <param name="action">The action to run.</param>
    /// <param name="scheduler">The scheduler used to run the action.</param>
    public StartSignal(Action action, ISequencer scheduler)
    {
        Action = action;
        Scheduler = scheduler;
    }

    /// <summary>Gets the action to run.</summary>
    private Action Action { get; }

    /// <summary>Gets the scheduler used to run the action.</summary>
    private ISequencer Scheduler { get; }

    /// <inheritdoc/>
    public bool IsRequiredSubscribeOnCurrentThread() => Scheduler == Sequencer.CurrentThread;

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<RxVoid> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        return SubscriptionScheduling.RunOn(
            Scheduler,
            (Self: this, observer),
            static s => s.Self.Run(s.observer));
    }

    /// <summary>Runs the action and forwards its terminal notification.</summary>
    /// <param name="observer">The downstream observer.</param>
    private void Run(IObserver<RxVoid> observer)
    {
        try
        {
            Action();
            observer.OnNext(RxVoid.Default);
            observer.OnCompleted();
        }
        catch (Exception error)
        {
            observer.OnError(error);
        }
    }
}
