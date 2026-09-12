// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Windows.Threading;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Wpf.Tests;

/// <summary>Tests dispatcher execution on a dedicated WPF STA thread.</summary>
public sealed class DispatcherSequencerTests
{
    /// <summary>Verifies the constructor rejects a null dispatcher.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullDispatcher() =>
        await Assert.That(static () => new DispatcherSequencer(null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>Verifies the clock uses UTC and debugger text identifies the sequencer.</summary>
    /// <returns>A task representing the asynchronous test operation.</returns>
    [Test]
    public async Task ClockUsesUtcAndDebuggerTextIdentifiesSequencer()
    {
        using var harness = new DispatcherHarness();
        DispatcherSequencer sequencer = new(harness.Dispatcher);
        await Assert.That(sequencer.Now.Offset).IsEqualTo(TimeSpan.Zero);
        await Assert.That(sequencer.DebuggerDisplay).IsEqualTo(typeof(DispatcherSequencer).FullName);
    }

    /// <summary>Verifies immediate work is posted to and executed on the dispatcher thread.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateScheduleExecutesOnDispatcherThread()
    {
        using var harness = new DispatcherHarness();
        var sequencer = new DispatcherSequencer(harness.Dispatcher);
        var completion = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

        sequencer.Schedule(new DelegateWorkItem(() => completion.TrySetResult(Environment.CurrentManagedThreadId)));

        var ranOnThreadId = await completion.Task;
        await Assert.That(ranOnThreadId).IsEqualTo(harness.ThreadId);
    }

    /// <summary>Verifies due work executes on the dispatcher thread.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task DueScheduleExecutesOnDispatcherThread()
    {
        using var harness = new DispatcherHarness();
        var sequencer = new DispatcherSequencer(harness.Dispatcher);
        var completion = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

        var due = sequencer.Timestamp;
        sequencer.Schedule(new DelegateWorkItem(() => completion.TrySetResult(Environment.CurrentManagedThreadId)), due);

        var ranOnThreadId = await completion.Task;
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute() => _action();
    }

    /// <summary>Owns a WPF dispatcher and its STA message loop.</summary>
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
            _thread.Join();
        }
    }
}
