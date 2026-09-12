// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for ReactiveExtensionsTests.</summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>Syncronizes the asynchronous runs with asynchronous tasks in subscriptions.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeSynchronus_RunsWithAsyncTasksInSubscriptions()
    {
        // Each handler completes after updating the shared counters.
        var result = 0;
        var itterations = 0;
        Subject<bool> subject = new();
        TaskCompletionSource allHandled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var disposable = subject.SubscribeSynchronous(async x =>
        {
            await Task.Yield();
            _ = x ? Interlocked.Increment(ref result) : Interlocked.Decrement(ref result);
            _ = Interlocked.Increment(ref itterations) == SampleValue6 && allHandled.TrySetResult();
        });
        subject.OnNext(true);
        subject.OnNext(false);
        subject.OnNext(true);
        subject.OnNext(false);
        subject.OnNext(true);
        subject.OnNext(false);
        await allHandled.Task;

        // Then
        await Assert.That(Volatile.Read(ref result)).IsZero();
    }

    /// <summary>Syncronizes the asynchronous runs with asynchronous tasks in subscriptions.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SynchronizeSynchronous_RunsWithAsyncTasksInSubscriptions()
    {
        // Each handler completes after updating the shared counters.
        var result = 0;
        var itterations = 0;
        Subject<bool> subject = new();
        List<Task> tasks = [];
        using var disposable = subject.SynchronizeSynchronous().Subscribe(x => tasks.Add(HandleAsync(x)));

        async Task HandleAsync((bool Value, IDisposable Sync) x)
        {
            try
            {
                await Task.Yield();
                _ = x.Value ? Interlocked.Increment(ref result) : Interlocked.Decrement(ref result);
            }
            finally
            {
                x.Sync.Dispose();
                _ = Interlocked.Increment(ref itterations);
            }
        }

        subject.OnNext(true);
        subject.OnNext(false);
        subject.OnNext(true);
        subject.OnNext(false);
        subject.OnNext(true);
        subject.OnNext(false);
        await Task.WhenAll(tasks);

        // Then
        using (Assert.Multiple())
        {
            await Assert.That(Volatile.Read(ref result)).IsZero();
            await Assert.That(Volatile.Read(ref itterations)).IsEqualTo(SampleValue6);
        }
    }

    /// <summary>Syncronizes the asynchronous runs with asynchronous tasks in subscriptions.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeAsync_RunsWithAsyncTasksInSubscriptions()
    {
        // Each handler completes after updating the shared counters.
        var result = 0;
        var itterations = 0;
        Subject<bool> subject = new();
        TaskCompletionSource allHandled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using var disposable = subject.SubscribeAsync(async x =>
        {
            await Task.Yield();
            _ = x ? Interlocked.Increment(ref result) : Interlocked.Decrement(ref result);
            _ = Interlocked.Increment(ref itterations) == SampleValue6 && allHandled.TrySetResult();
        });
        subject.OnNext(true);
        subject.OnNext(false);
        subject.OnNext(true);
        subject.OnNext(false);
        subject.OnNext(true);
        subject.OnNext(false);
        await allHandled.Task;

        // Then
        await Assert.That(Volatile.Read(ref result)).IsZero();
    }

    /// <summary>Tests WithLimitedConcurrency.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous RxVoid test.</returns>
    [Test]
    public async Task WithLimitedConcurrency_LimitsConcurrentTasks()
    {
        const int MaxConcurrency = 3;
        var inFlight = 0;
        var maxConcurrent = 0;
        var pulled = System.Threading.Channels.Channel.CreateUnbounded<TaskCompletionSource<int>>();
        List<int> results = [];
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        IEnumerable<Task<int>> CreateTasks()
        {
            for (var i = 1; i <= SampleValue10; i++)
            {
                TaskCompletionSource<int> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
                var current = Interlocked.Increment(ref inFlight);
                maxConcurrent = Math.Max(maxConcurrent, current);
                _ = pulled.Writer.TryWrite(gate);
                yield return gate.Task;
            }
        }

        using var sub = CreateTasks().WithLimitedConcurrency(MaxConcurrency)
            .Subscribe(results.Add, completed.SetResult);
        for (var next = 1; next <= SampleValue10; next++)
        {
            var gate = await pulled.Reader.ReadAsync();
            _ = Interlocked.Decrement(ref inFlight);
            gate.SetResult(next);
        }

        await completed.Task;
        await Assert.That(results).Count().IsEqualTo(SampleValue10);
        await Assert.That(maxConcurrent).IsEqualTo(MaxConcurrency);
    }

    /// <summary>Verifies an empty limited-concurrency task sequence completes immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WithLimitedConcurrency_EmptyTaskSequence_Completes()
    {
        var completed = false;
        List<int> values = [];
        using var sub = Array.Empty<Task<int>>().WithLimitedConcurrency(SampleValue3)
            .Subscribe(values.Add, () => completed = true);
        await Assert.That(values).IsEmpty();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies faulted and canceled tasks stop enumeration and report the expected errors.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WithLimitedConcurrency_TaskFaultsOrCancels_ThenErrorsAndStops()
    {
        InvalidOperationException expected = new("limited-concurrency");
        Exception? fault = null;
        Exception? canceled = null;
        var afterFaultPulled = false;

        IEnumerable<Task<int>> FaultingTasks()
        {
            yield return Task.FromException<int>(expected);
            afterFaultPulled = true;
            yield return Task.FromResult(SampleValue2);
        }

        using var faultSub = FaultingTasks().WithLimitedConcurrency(1).Subscribe(
            static _ => { },
            ex => fault = ex);
        using var canceledSub = new[] { Task.FromCanceled<int>(new(true)) }.WithLimitedConcurrency(1).Subscribe(
            static _ => { },
            ex => canceled = ex);
        await Assert.That(fault).IsSameReferenceAs(expected);
        await Assert.That(canceled).IsTypeOf<OperationCanceledException>();
        await Assert.That(afterFaultPulled).IsFalse();
    }

    /// <summary>Verifies disposing before a pending task completes drops later notifications.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WithLimitedConcurrency_DisposeBeforeTaskContinuation_DropsWork()
    {
        ConcurrencyLimiter<int> limiter = new([], 1);
        List<int> values = [];
        Exception? caught = null;
        var completed = false;
        ConcurrencyLimiter<int>.Subscription subscription = new(
            limiter,
            Observer.Create<int>(values.Add, ex => caught = ex, () => completed = true));
        subscription.Dispose();
        limiter.ProcessTaskCompletion(subscription, Task.FromResult(SampleValue10));
        await Assert.That(values).IsEmpty();
        await Assert.That(caught).IsNull();
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Exercises the internal disposed pull path used after a subscription is closed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WithLimitedConcurrency_PullAfterDisposed_ThenNoNotifications()
    {
        ConcurrencyLimiter<int> limiter = new([Task.FromResult(SampleValue10)], 1) { Disposed = true };
        List<int> values = [];
        var completed = false;
        limiter.PullNextTask(Observer.Create<int>(
            values.Add,
            static _ => { },
            () => completed = true));
        await Assert.That(values).IsEmpty();
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Exercises the null-current continuation path defensively tolerated by the limiter.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WithLimitedConcurrency_NullTaskEntry_ThenNoNotifications()
    {
        IEnumerable<Task<int>> tasks = [null!];
        List<int> values = [];
        Exception? caught = null;
        var completed = false;
        using var sub = tasks.WithLimitedConcurrency(1)
            .Subscribe(values.Add, ex => caught = ex, () => completed = true);
        await Assert.That(values).IsEmpty();
        await Assert.That(caught).IsNull();
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Tests SynchronizeSynchronous provides sync lock.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SynchronizeSynchronous_ProvidesSyncLock()
    {
        Subject<int> subject = new();
        List<int> results = [];
        IDisposable? lastSync = null;
        using var sub = subject.SynchronizeSynchronous().Subscribe(tuple =>
        {
            results.Add(tuple.Value);
            lastSync = tuple.Sync;
            tuple.Sync.Dispose(); // Must dispose sync lock to allow next item to process
        });
        subject.OnNext(1);
        using (Assert.Multiple())
        {
            await Assert.That(results).Count().IsEqualTo(1);
            await Assert.That(lastSync).IsNotNull();
        }
    }

    /// <summary>Tests SynchronizeAsync provides sync lock.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SynchronizeAsync_ProvidesSyncLock()
    {
        Subject<int> subject = new();
        List<int> results = [];
        IDisposable? lastSync = null;
        using var sub = subject.SynchronizeAsync().Subscribe(tuple =>
        {
            results.Add(tuple.Value);
            lastSync = tuple.Sync;
        });
        subject.OnNext(1);
        using (Assert.Multiple())
        {
            await Assert.That(results).Count().IsEqualTo(1);
            await Assert.That(lastSync).IsNotNull();
        }
    }

    /// <summary>Tests SubscribeAsync with onNext and onError.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous RxVoid test.</returns>
    [Test]
    public async Task SubscribeAsync_WithOnNextAndOnError_HandlesError()
    {
        List<int> results = [];
        Exception? caughtException = null;
        TaskCompletionSource<bool> errorSource = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = Observable.Create<int>(static observer =>
        {
            observer.OnNext(1);
            observer.OnError(new InvalidOperationException());
            return EmptyDisposable.Instance;
        });
        using var sub = source.SubscribeAsync(
            async x => results.Add(x),
            ex =>
        {
            caughtException = ex;
            _ = errorSource.TrySetResult(true);
        });
        await errorSource.Task;
        using (Assert.Multiple())
        {
            await Assert.That(results).IsCollectionEqualTo([1]);
            await Assert.That(caughtException).IsNotNull();
        }
    }

    /// <summary>Tests SubscribeSynchronous with full callbacks.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeSynchronous_WithFullCallbacks_ExecutesAll()
    {
        Subject<int> subject = new();
        List<int> results = [];
        var errorHandled = false;
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = subject.SubscribeSynchronous(
            async v =>
            {
                await Task.Yield();
                results.Add(v);
            },
            _ => errorHandled = true,
            () => completed.TrySetResult());
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        subject.OnCompleted();
        await completed.Task;
        using (Assert.Multiple())
        {
            await Assert.That(results).IsCollectionEqualTo([1, SampleValue2]);
            await Assert.That(errorHandled).IsFalse();
            await Assert.That(completed.Task.IsCompletedSuccessfully).IsTrue();
        }
    }

    /// <summary>Tests SubscribeSynchronous with onNext and onError.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeSynchronous_WithOnNextAndOnError_HandlesError()
    {
        Subject<int> subject = new();
        List<int> results = [];
        TaskCompletionSource onNextCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource errorHandled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = subject.SubscribeSynchronous(
            async v =>
            {
                await Task.Yield();
                results.Add(v);
                onNextCompleted.SetResult();
            },
            _ => errorHandled.TrySetResult());
        subject.OnNext(1);
        await onNextCompleted.Task;
        subject.OnError(new InvalidOperationException());
        await errorHandled.Task;
        using (Assert.Multiple())
        {
            await Assert.That(results).IsCollectionEqualTo([1]);
            await Assert.That(errorHandled.Task.IsCompletedSuccessfully).IsTrue();
        }
    }

    /// <summary>Tests SubscribeSynchronous with onNext and onCompleted.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeSynchronous_WithOnNextAndOnCompleted_CompletesCorrectly()
    {
        Subject<int> subject = new();
        List<int> results = [];
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = subject.SubscribeSynchronous(
            async v =>
            {
                await Task.Yield();
                results.Add(v);
            },
            () => completed.TrySetResult());
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        subject.OnCompleted();
        await completed.Task;
        using (Assert.Multiple())
        {
            await Assert.That(results).IsCollectionEqualTo([1, SampleValue2]);
            await Assert.That(completed.Task.IsCompletedSuccessfully).IsTrue();
        }
    }

    /// <summary>Tests SubscribeSynchronous with only onNext.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeSynchronous_WithOnlyOnNext_ProcessesValues()
    {
        const int ExpectedCount = 3;
        Subject<int> subject = new();
        List<int> results = [];
        TaskCompletionSource allReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = subject.SubscribeSynchronous(async v =>
        {
            await Task.Yield();
            results.Add(v);
            _ = results.Count == ExpectedCount && allReceived.TrySetResult();
        });
        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        subject.OnNext(SampleValue3);
        await allReceived.Task;
        await Assert.That(results).IsCollectionEqualTo([1, SampleValue2, SampleValue3]);
    }

    /// <summary>Tests SubscribeAsync with onNext and onCompleted.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeAsync_WithOnNextAndOnCompleted_CompletesCorrectly()
    {
        List<int> results = [];
        var completed = false;
        TaskCompletionSource<bool> completionSource = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = Observable.Create<int>(static observer =>
        {
            observer.OnNext(1);
            observer.OnNext(SampleValue2);
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        });
        using var subscription = source.SubscribeAsync(
            async v =>
            {
                await Task.Yield();
                results.Add(v);
            },
            () =>
            {
                completed = true;
                _ = completionSource.TrySetResult(true);
            });
        await completionSource.Task;
        using (Assert.Multiple())
        {
            await Assert.That(results).IsCollectionEqualTo([1, SampleValue2]);
            await Assert.That(completed).IsTrue();
        }
    }
}
