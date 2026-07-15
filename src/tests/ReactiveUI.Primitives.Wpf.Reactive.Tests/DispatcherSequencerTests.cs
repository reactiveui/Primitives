// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Windows.Threading;
using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Wpf.Reactive.Tests;

/// <summary>
/// Tests for <see cref="DispatcherSequencer"/> as an <see cref="IScheduler"/>, exercised against a real WPF
/// <see cref="Dispatcher"/> pumped on a dedicated STA thread so both the immediate and timer-based dispatch
/// paths run end to end. Compiled only on Windows builds (see the csproj).
/// </summary>
public sealed class DispatcherSequencerTests
{
    /// <summary>Maximum time to wait for work to be marshalled onto the dispatcher thread before failing.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>How far into the future the delayed work is scheduled.</summary>
    private static readonly TimeSpan ScheduleDelay = TimeSpan.FromMilliseconds(50);

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
        var scheduler = new DispatcherSequencer(harness.Dispatcher);
        var completion = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = scheduler.Schedule(() => completion.TrySetResult(Environment.CurrentManagedThreadId));

        var ranOnThreadId = await completion.Task.WaitAsync(WaitTimeout);
        await Assert.That(ranOnThreadId).IsEqualTo(harness.ThreadId);
    }

    /// <summary>Verifies work due in the future is executed on the dispatcher thread via the dispatcher timer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedScheduleExecutesOnDispatcherThread()
    {
        using var harness = new DispatcherHarness();
        var scheduler = new DispatcherSequencer(harness.Dispatcher);
        var completion = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = scheduler.Schedule(ScheduleDelay, () => completion.TrySetResult(Environment.CurrentManagedThreadId));

        var ranOnThreadId = await completion.Task.WaitAsync(WaitTimeout);
        await Assert.That(ranOnThreadId).IsEqualTo(harness.ThreadId);
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
            })
            {
                IsBackground = true,
                Name = "WpfDispatcherHarness",
            };

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
