// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.WinForms.Reactive.Tests;

/// <summary>Tests scheduler dispatch on a dedicated Windows Forms STA thread.</summary>
public sealed class ControlSequencerTests
{
    /// <summary>Verifies the constructor rejects a null control.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ConstructorRejectsNullControl() =>
        await Assert.That(static () => new ControlSequencer(null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>Verifies immediate work is posted to and executed on the control's UI thread.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task ImmediateScheduleExecutesOnControlThread()
    {
        using var harness = new ControlHarness();
        var scheduler = new ControlSequencer(harness.Control);
        var completion = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = scheduler.Schedule(() => completion.TrySetResult(Environment.CurrentManagedThreadId));

        var ranOnThreadId = await completion.Task;
        await Assert.That(ranOnThreadId).IsEqualTo(harness.ThreadId);
    }

    /// <summary>Owns the control and its STA message loop.</summary>
    private sealed class ControlHarness : IDisposable
    {
        /// <summary>The thread running the Windows Forms message loop.</summary>
        private readonly Thread _thread;

        /// <summary>Initializes a new instance of the <see cref="ControlHarness"/> class with a created control handle.</summary>
        public ControlHarness()
        {
            using var ready = new ManualResetEventSlim(false);
            _thread = new(() =>
            {
                Control = new();
                _ = Control.Handle; // Force handle creation so BeginInvoke can marshal work.
                ThreadId = Environment.CurrentManagedThreadId;
                ready.Set();
                Application.Run();
            }) { IsBackground = true, Name = "WinFormsControlHarness" };

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
            _thread.Join();
            Control.Dispose();
        }
    }
}
