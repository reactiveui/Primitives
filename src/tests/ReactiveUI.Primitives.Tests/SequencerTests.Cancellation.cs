// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests cancellation across queued, waiting, and completed work states.</summary>
public partial class SequencerTests
{
    /// <summary>A canceled trampoline item is skipped before waiting or after its wait returns.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CurrentThreadTrampolineChecksCancellationAroundTheWait()
    {
        SequencerQueue<long> queue = new();
        var runs = 0;
        ScheduledItem<long> canceled = new(0, Comparer<long>.Default, _ =>
        {
            runs++;
            return EmptyDisposable.Instance;
        });
        ScheduledItem<long> waiting = new(Stopwatch.Frequency, Comparer<long>.Default, _ =>
        {
            runs++;
            return EmptyDisposable.Instance;
        });
        canceled.Cancel();
        queue.Enqueue(canceled);
        queue.Enqueue(waiting);
        List<TimeSpan> waits = [];
        CurrentThreadSequencer.Trampoline.Run(queue, static () => 0, delay =>
        {
            waits.Add(delay);
            waiting.Cancel();
        });
        await Assert.That(waits.SequenceEqual([TimeSpan.FromSeconds(1)])).IsTrue();
        await Assert.That(runs).IsEqualTo(0);
        await Assert.That(queue.Count).IsEqualTo(0);
    }

    /// <summary>Only positive remaining delays invoke the wait operation.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CurrentThreadWaitsOnlyForPositiveDelays()
    {
        List<TimeSpan> waits = [];
        CurrentThreadSequencer.WaitIfNeeded(TimeSpan.Zero, waits.Add);
        CurrentThreadSequencer.WaitIfNeeded(TimeSpan.FromTicks(-1), waits.Add);
        CurrentThreadSequencer.WaitIfNeeded(TimeSpan.FromTicks(1), waits.Add);
        await Assert.That(waits.SequenceEqual([TimeSpan.FromTicks(1)])).IsTrue();

        var runs = 0;
        CurrentThreadSequencer.ActionWorkItem item = new(() => runs++);
        item.Dispose();
        item.Execute();
        await Assert.That(runs).IsEqualTo(0);
        await Assert.That(item.IsDisposed).IsTrue();
    }

    /// <summary>Canceled immediate and delayed work never executes when the manual pool drains.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ThreadPoolDropsCanceledWorkFromBothQueues()
    {
        using ManualThreadPool pool = new();
        CancellableWorkItem immediate = new();
        CancellableWorkItem delayed = new();
        pool.Sequencer.Schedule(immediate);
        pool.Sequencer.Schedule(delayed, One);
        immediate.Dispose();
        delayed.Dispose();
        pool.RunReady();
        pool.RunDue(One);
        await Assert.That(immediate.ExecuteCount).IsEqualTo(0);
        await Assert.That(delayed.ExecuteCount).IsEqualTo(0);
    }

    /// <summary>A task canceled after dispatch is skipped when its task scheduler runs it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TaskPoolSkipsWorkCanceledAfterDispatch()
    {
        ManualTaskScheduler scheduler = new();
        TaskPoolSequencer sequencer = new(new(scheduler));
        CancellableWorkItem item = new();
        sequencer.Schedule(item);
        item.Dispose();
        scheduler.RunPending();
        await Assert.That(item.ExecuteCount).IsEqualTo(0);
    }

    /// <summary>Cancellation after result publication releases the owned resource exactly once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ThreadPoolFinalCancellationCleanupDoesNotReleaseTheResultTwice()
    {
        using ManualThreadPool pool = new();
        RecordingDisposable result = new();
        ThreadPoolSequencer.ScheduledWorkItem<int> item = new(
            pool.Sequencer,
            0,
            (_, _) => result);
        item.Execute();
        item.Dispose();
        item.ReleaseCanceledResult();
        await Assert.That(result.DisposeCount).IsEqualTo(1);
        await Assert.That(item.IsDisposed).IsTrue();
    }

    /// <summary>Cancellation after publication leaves no result for final cleanup to release again.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DelegateFinalCancellationCleanupDoesNotReleaseTheResultTwice()
    {
        RecordingDisposable result = new();
        Sequencer.DelegateWorkItem<int> item = new(Sequencer.Immediate, 0, (_, _) => result);
        item.Execute();
        item.Dispose();
        item.ReleaseCanceledResult();
        await Assert.That(result.DisposeCount).IsEqualTo(1);
    }
}
