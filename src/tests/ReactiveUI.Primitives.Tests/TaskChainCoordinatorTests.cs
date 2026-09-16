// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests that <see cref="TaskChainCoordinator{T}"/> serializes deliveries without holding a lock while user code runs.</summary>
public sealed class TaskChainCoordinatorTests
{
    /// <summary>The first task result.</summary>
    private const int One = 1;

    /// <summary>The second task result.</summary>
    private const int Two = 2;

    /// <summary>The number of tasks in the contention scenario.</summary>
    private const int ContendedTasks = 200;

    /// <summary>
    /// An observer whose error handler marshals synchronously to another thread is not deadlocked when that thread pushes
    /// another task and completes the outer source, and nothing follows the error.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TaskChainCoordinatorErrorObserverMarshallingWhileTheOtherThreadPushesATaskDoesNotDeadlock()
    {
        using MarshallingThread dispatcher = new();
        IObserver<Task<int>>? outer = null;
        Exception? received = null;
        var values = 0;
        var completions = 0;
        InvalidOperationException expected = new("task");

        using var subscription = new TaskChainSignal<int>(new ScriptedObservable<Task<int>>(observer => outer = observer)).Subscribe(
            _ => values++,
            error =>
            {
                received = error;
                dispatcher.Invoke(() =>
                {
                    outer!.OnNext(Task.FromResult(Two));
                    outer.OnCompleted();
                });
            },
            () => completions++);
        var worker = BackgroundThread.Start(() => outer!.OnNext(Task.FromException<int>(expected)));

        await Assert.That(await BackgroundThread.FinishesPromptly(worker)).IsTrue();
        dispatcher.Invoke(static () => { });
        await Assert.That(received).IsSameReferenceAs(expected);
        await Assert.That(values).IsEqualTo(0);
        await Assert.That(completions).IsEqualTo(0);
    }

    /// <summary>
    /// An observer that marshals synchronously to another thread is not deadlocked when that thread pushes the next task
    /// and completes the outer source; the result and completion follow in order.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TaskChainCoordinatorObserverMarshallingWhileTheOtherThreadCompletesTheChainDoesNotDeadlock()
    {
        using MarshallingThread dispatcher = new();
        using ManualResetEventSlim inside = new(false);
        IObserver<Task<int>>? outer = null;
        ConcurrentQueue<int> values = new();
        CallbackRecordingWitness<int> downstream = new(value =>
        {
            values.Enqueue(value);
            if (Environment.CurrentManagedThreadId == dispatcher.ManagedThreadId || inside.IsSet)
            {
                return;
            }

            inside.Set();
            dispatcher.Invoke(static () => { });
        });

        using var subscription = new TaskChainCoordinator<int>(downstream).Run(new ScriptedObservable<Task<int>>(observer => outer = observer));
        dispatcher.Post(() =>
        {
            inside.Wait();
            outer!.OnNext(Task.FromResult(Two));
            outer.OnCompleted();
        });
        var worker = BackgroundThread.Start(() => outer!.OnNext(Task.FromResult(One)));

        await Assert.That(await BackgroundThread.FinishesPromptly(worker)).IsTrue();
        dispatcher.Invoke(static () => { });
        await Assert.That(string.Join(",", values)).IsEqualTo("1,2");
        await Assert.That(downstream.Completions).IsEqualTo(1);
    }

    /// <summary>Outer completion raised from another thread is delivered after the results queued before it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TaskChainCoordinatorCompletionRaisedDuringDeliveryFollowsTheQueuedResults()
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        IObserver<Task<int>>? outer = null;
        List<int> values = [];
        var downstream = MergeDeliveryAssertions.BlockOnFirstValue(values, inside, release);

        using var subscription = new TaskChainCoordinator<int>(downstream).Run(new ScriptedObservable<Task<int>>(observer => outer = observer));
        var owner = BackgroundThread.Start(() => outer!.OnNext(Task.FromResult(One)));
        inside.Wait();
        await BackgroundThread.Start(() => outer!.OnNext(Task.FromResult(Two)));
        await BackgroundThread.Start(() => outer!.OnCompleted());
        await Assert.That(downstream.IsCompleted).IsFalse();
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo("1,2");
        await Assert.That(downstream.Completions).IsEqualTo(1);
    }

    /// <summary>
    /// An outer error raised from another thread while a result is delivered follows it, and the task queued before the
    /// error is dropped.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TaskChainCoordinatorOuterErrorRaisedDuringDeliveryStopsTheChain()
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        IObserver<Task<int>>? outer = null;
        List<int> values = [];
        var downstream = MergeDeliveryAssertions.BlockOnFirstValue(values, inside, release);
        InvalidOperationException expected = new("outer");

        using var subscription = new TaskChainCoordinator<int>(downstream).Run(new ScriptedObservable<Task<int>>(observer => outer = observer));
        var owner = BackgroundThread.Start(() => outer!.OnNext(Task.FromResult(One)));
        inside.Wait();
        await BackgroundThread.Start(() => outer!.OnNext(Task.FromResult(Two)));
        await BackgroundThread.Start(() => outer!.OnError(expected));
        await Assert.That(downstream.Error).IsNull();
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo("1");
        await Assert.That(downstream.Error).IsSameReferenceAs(expected);
        await Assert.That(downstream.Completions).IsEqualTo(0);
    }

    /// <summary>
    /// Tasks pushed from one thread and completed in reverse order from another deliver every result in source order,
    /// without overlap, followed by one completion.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TaskChainCoordinatorResultsKeepSourceOrderUnderContention()
    {
        var sources = new TaskCompletionSource<int>[ContendedTasks];
        for (var i = 0; i < sources.Length; i++)
        {
            sources[i] = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        IObserver<Task<int>>? outer = null;
        var inFlight = 0;
        var overlaps = 0;
        var outOfOrder = 0;
        var last = -1;
        var completions = 0;

        using var subscription = new TaskChainSignal<int>(new ScriptedObservable<Task<int>>(observer => outer = observer)).Subscribe(
            value =>
            {
                if (Interlocked.Increment(ref inFlight) != 1)
                {
                    overlaps++;
                }

                outOfOrder += value == last + 1 ? 0 : 1;
                last = value;
                _ = Interlocked.Decrement(ref inFlight);
            },
            static error => throw error,
            () =>
            {
                completions++;
                completed.SetResult();
            });
        var producer = BackgroundThread.Start(() =>
        {
            foreach (var source in sources)
            {
                outer!.OnNext(source.Task);
            }

            outer!.OnCompleted();
        });
        var completer = BackgroundThread.Start(() =>
        {
            for (var i = sources.Length - 1; i >= 0; i--)
            {
                sources[i].SetResult(i);
            }
        });
        await Task.WhenAll(producer, completer);

        await Assert.That(await BackgroundThread.FinishesPromptly(completed.Task)).IsTrue();
        await Assert.That(overlaps).IsEqualTo(0);
        await Assert.That(outOfOrder).IsEqualTo(0);
        await Assert.That(last).IsEqualTo(ContendedTasks - 1);
        await Assert.That(completions).IsEqualTo(1);
    }

    /// <summary>An outer error raised before disposal while another thread delivers is still delivered after the result.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TaskChainCoordinatorErrorRaisedBeforeDisposeIsStillDelivered()
    {
        using ManualResetEventSlim inside = new(false);
        using ManualResetEventSlim release = new(false);
        IObserver<Task<int>>? outer = null;
        List<int> values = [];
        var downstream = MergeDeliveryAssertions.BlockOnFirstValue(values, inside, release);
        InvalidOperationException expected = new("outer");

        var coordinator = new TaskChainCoordinator<int>(downstream).Run(new ScriptedObservable<Task<int>>(observer => outer = observer));
        var owner = BackgroundThread.Start(() => outer!.OnNext(Task.FromResult(One)));
        inside.Wait();
        await BackgroundThread.Start(() => outer!.OnError(expected));
        var disposer = BackgroundThread.Start(coordinator.Dispose);
        await Assert.That(await BackgroundThread.FinishesPromptly(disposer)).IsTrue();
        release.Set();
        await owner;

        await Assert.That(string.Join(",", values)).IsEqualTo("1");
        await Assert.That(downstream.Error).IsSameReferenceAs(expected);
        await Assert.That(downstream.Completions).IsEqualTo(0);
    }

    /// <summary>Tasks and completion pushed after disposal are dropped.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TaskChainCoordinatorDropsNotificationsRaisedAfterDispose()
    {
        IObserver<Task<int>>? outer = null;
        RecordingWitness<int> downstream = new();

        var coordinator = new TaskChainCoordinator<int>(downstream).Run(new ScriptedObservable<Task<int>>(observer => outer = observer));
        coordinator.Dispose();
        outer!.OnNext(Task.FromResult(One));
        outer.OnCompleted();
        outer.OnError(new InvalidOperationException("after dispose"));

        await Assert.That(downstream.Values.Count).IsEqualTo(0);
        await Assert.That(downstream.Errors.Count).IsEqualTo(0);
        await Assert.That(downstream.Completed).IsEqualTo(0);
    }
}
