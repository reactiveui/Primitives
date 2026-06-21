// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Shared subscription routing for signal implementations.</summary>
public static class SignalSubscription
{
    /// <summary>Subscribes an observer, dispatching through the current-thread sequencer when required.</summary>
    /// <typeparam name="T">The signal value type.</typeparam>
    /// <param name="observer">The observer value.</param>
    /// <param name="currentThreadRequired">Whether subscription must be dispatched through the current-thread sequencer.</param>
    /// <param name="subscribeCore">The core subscription callback.</param>
    /// <returns>The subscription.</returns>
    public static IDisposable Subscribe<T>(IObserver<T> observer, bool currentThreadRequired, Func<IObserver<T>, IDisposable, IDisposable> subscribeCore)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        SingleDisposable subscription = new();

        if (currentThreadRequired && CurrentThreadSequencer.IsScheduleRequired)
        {
            _ = Sequencer.CurrentThread.Schedule(() => subscription.Create(subscribeCore(observer, subscription)));
        }
        else
        {
            subscription.Create(subscribeCore(observer, subscription));
        }

        return subscription;
    }
}
