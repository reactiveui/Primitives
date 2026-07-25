// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Windows.Threading;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Wpf.Tests;

/// <summary>
/// Tests for <see cref="DispatcherSequencer"/>, exercised against a real WPF <see cref="Dispatcher"/>
/// pumped on a dedicated STA thread so both the immediate and timer-based dispatch paths run end to end.
/// Compiled only on Windows builds (see the csproj).
/// </summary>
public sealed class DispatcherSequencerTests
{
    /// <summary>Maximum time to wait for work to be marshalled onto the dispatcher thread before failing.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Stopwatch ticks until the delayed work falls due: one twentieth of a second, or 50 ms.</summary>
    private static readonly long ScheduleDelayTicks = Stopwatch.Frequency / 20;

    /// <summary>Verifies the constructor rejects a null dispatcher.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullDispatcher() =>
        await Assert.That(static () => new DispatcherSequencer(null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>Verifies immediate work is posted to and executed on the dispatcher thread.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateScheduleExecutesOnDispatcherThread()
    {
        using var harness = new DispatcherHarness();
        var sequencer = new DispatcherSequencer(harness.Dispatcher);
        var completion = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

        sequencer.Schedule(new DelegateWorkItem(() => completion.TrySetResult(Environment.CurrentManagedThreadId)));

        var ranOnThreadId = await completion.Task.WaitAsync(WaitTimeout);
        await Assert.That(ranOnThreadId).IsEqualTo(harness.ThreadId);
    }

    /// <summary>Verifies work due in the future is executed on the dispatcher thread via the dispatcher timer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedScheduleExecutesOnDispatcherThread()
    {
        using var harness = new DispatcherHarness();
        var sequencer = new DispatcherSequencer(harness.Dispatcher);
        var completion = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

        var due = sequencer.Timestamp + ScheduleDelayTicks;
        sequencer.Schedule(new DelegateWorkItem(() => completion.TrySetResult(Environment.CurrentManagedThreadId)), due);

        var ranOnThreadId = await completion.Task.WaitAsync(WaitTimeout);
        await Assert.That(ranOnThreadId).IsEqualTo(harness.ThreadId);
    }

    /// <summary>Work item that invokes a delegate when executed.</summary>
    private sealed class DelegateWorkItem : IWorkItem
    {
        /// <summary>The action to run on execution.</summary>
        private readonly Action _action;

        /// <summary>Initializes a new instance of the <see cref="DelegateWorkItem"/> class.</summary>
        /// <param name="action">The action to run on execution.</param>
        public DelegateWorkItem(Action action) => _action = action;

        /// <inheritdoc/>
        public void Execute() => _action();
    }

    /// <summary>Hosts a WPF <see cref="Dispatcher"/> on a dedicated STA thread and pumps its message loop, shutting it down and joining the thread on disposal.</summary>
    private sealed class DispatcherHarness : IDisposable
    {
        /// <summary>The thread running the dispatcher message loop.</summary>
        private readonly Thread _thread;

        /// <summary>Initializes a new instance of the <see cref="DispatcherHarness"/> class and waits until the dispatcher is running.</summary>
        public DispatcherHarness()
        {
            using var ready = new ManualResetEventSlim(false);
            _thread = new(() =>
            {
                Dispatcher = Dispatcher.CurrentDispatcher;
                ThreadId = Environment.CurrentManagedThreadId;
                ready.Set();
                Dispatcher.Run();
            }) { IsBackground = true, Name = "WpfDispatcherHarness" };

            _thread.SetApartmentState(ApartmentState.STA);
            _thread.Start();
            ready.Wait();
        }

        /// <summary>Gets the hosted dispatcher.</summary>
        public Dispatcher Dispatcher { get; private set; } = null!;

        /// <summary>Gets the managed thread id the dispatcher runs on.</summary>
        public int ThreadId { get; private set; }

        /// <inheritdoc/>
        public void Dispose()
        {
            Dispatcher.InvokeShutdown();
            _ = _thread.Join(WaitTimeout);
        }
    }
}
