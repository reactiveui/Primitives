// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.WinForms.Tests;

/// <summary>
/// Tests for <see cref="ControlSequencer"/>, exercised against a real Windows Forms <see cref="Control"/>
/// whose handle is created on a dedicated STA thread running a message loop, so posted work runs end to end.
/// Compiled only on Windows builds (see the csproj).
/// </summary>
public sealed class ControlSequencerTests
{
    /// <summary>Maximum time to wait for work to be marshalled onto the control thread before failing.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies the constructor rejects a null control.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullControl() =>
        await Assert.That(() => new ControlSequencer(null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>Verifies immediate work is posted to and executed on the control's UI thread.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateScheduleExecutesOnControlThread()
    {
        using var harness = new ControlHarness();
        var sequencer = new ControlSequencer(harness.Control);
        var completion = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

        sequencer.Schedule(new DelegateWorkItem(() => completion.TrySetResult(Environment.CurrentManagedThreadId)));

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

    /// <summary>Hosts a Windows Forms <see cref="Control"/> on a dedicated STA thread running a message loop, exiting it and joining the thread on disposal.</summary>
    private sealed class ControlHarness : IDisposable
    {
        /// <summary>The thread running the Windows Forms message loop.</summary>
        private readonly Thread _thread;

        /// <summary>Initializes a new instance of the <see cref="ControlHarness"/> class and waits until the control handle and message loop are running.</summary>
        public ControlHarness()
        {
            using var ready = new ManualResetEventSlim(false);
            _thread = new Thread(() =>
            {
                Control = new Control();
                _ = Control.Handle; // Force handle creation so BeginInvoke can marshal work.
                ThreadId = Environment.CurrentManagedThreadId;
                ready.Set();
                Application.Run();
            })
            {
                IsBackground = true,
                Name = "WinFormsControlHarness",
            };

            _thread.SetApartmentState(ApartmentState.STA);
            _thread.Start();
            ready.Wait();
        }

        /// <summary>Gets the hosted control.</summary>
        public Control Control { get; private set; } = null!;

        /// <summary>Gets the managed thread id the control runs on.</summary>
        public int ThreadId { get; private set; }

        /// <inheritdoc/>
        public void Dispose()
        {
            _ = Control.BeginInvoke(Application.ExitThread);
            _ = _thread.Join(WaitTimeout);
            Control.Dispose();
        }
    }
}
