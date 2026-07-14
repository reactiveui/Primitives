// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="TaskSignal{T}"/> cancellation and disposal contracts.</summary>
public class TaskSignalTests
{
    /// <summary>Covers task-signal cancellation registration and disposal branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskSignalCoversCancellationAndDisposeBranches()
    {
        List<Exception> canceled = [];
        using CancellationTokenSource cts = new();
        var taskSignal = TaskSignal<int>.Create(static _ => Signal.Silent<int>(), Sequencer.CurrentThread, cts);
        taskSignal.GetOperationCanceled(Witness.Create<Exception>(canceled.Add));
        await Assert.That(taskSignal.IsCancellationRequested).IsFalse();
        taskSignal.Dispose();
        taskSignal.Dispose();
        await Assert.That(taskSignal.IsDisposed).IsTrue();
        await Assert.That(taskSignal.IsCancellationRequested).IsTrue();
        await Assert.That(canceled.Count).IsEqualTo(1);
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            var invalid = TaskSignal<int>.Create(null!);
            GC.KeepAlive(invalid);
        });
    }

    /// <summary>The scheduler overload of the task-signal factory builds a live signal that cancels when disposed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TaskSignalCreateWithASchedulerBuildsASignalThatCancelsOnDisposal()
    {
        using var taskSignal = TaskSignal.Create<int>(static _ => Signal.Silent<int>(), Sequencer.CurrentThread);

        await Assert.That(taskSignal.IsCancellationRequested).IsFalse();
        await Assert.That(taskSignal.IsDisposed).IsFalse();

        taskSignal.Dispose();

        await Assert.That(taskSignal.IsDisposed).IsTrue();
        await Assert.That(taskSignal.IsCancellationRequested).IsTrue();
    }
}
