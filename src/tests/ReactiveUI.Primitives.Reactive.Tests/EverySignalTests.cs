// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Reactive.Signals;

namespace ReactiveUI.Primitives.Reactive.Tests;

/// <summary>Tests recurring ticks whose scheduler has already accepted a callback at cancellation.</summary>
public class EverySignalTests
{
    /// <summary>A callback delivered after disposal emits no tick and cannot schedule a successor.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Every_DisposedBeforeAcceptedCallback_DoesNotEmitOrReschedule()
    {
        AcceptedCallbackScheduler scheduler = new();
        List<long> values = [];
        var subscription = Signal.Every(TimeSpan.Zero, scheduler).Subscribe(values.Add);

        subscription.Dispose();
        scheduler.RunAccepted();

        await Assert.That(values.Count).IsEqualTo(0);
        await Assert.That(scheduler.ScheduleCount).IsEqualTo(1);
        await Assert.That(scheduler.HandleDisposed).IsTrue();
    }

    /// <summary>Retains an accepted callback independently of the scheduling handle's cancellation state.</summary>
    private sealed class AcceptedCallbackScheduler : IScheduler
    {
        /// <summary>The callback accepted by the scheduler.</summary>
        private Action? _pending;

        /// <inheritdoc/>
        public DateTimeOffset Now => DateTimeOffset.UnixEpoch;

        /// <summary>Gets the number of accepted schedules.</summary>
        internal int ScheduleCount { get; private set; }

        /// <summary>Gets whether cancellation reached the scheduler's handle.</summary>
        internal bool HandleDisposed { get; private set; }

        /// <inheritdoc/>
        public IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action)
        {
            ScheduleCount++;
            _pending = () => action(this, state).Dispose();
            return Disposable.Create(this, static scheduler => scheduler.HandleDisposed = true);
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<IScheduler, TState, IDisposable> action) =>
            Schedule(state, action);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable Schedule<TState>(TState state, DateTimeOffset dueTime, Func<IScheduler, TState, IDisposable> action) =>
            Schedule(state, dueTime - Now, action);

        /// <summary>Delivers the already accepted callback even if its handle was subsequently cancelled.</summary>
        internal void RunAccepted()
        {
            var callback = _pending;
            _pending = null;
            callback!.Invoke();
        }
    }
}
