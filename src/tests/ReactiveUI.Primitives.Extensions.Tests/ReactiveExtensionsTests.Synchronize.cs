// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using ReactiveUI.Primitives.Extensions.Internal;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for ReactiveExtensionsTests.</summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>
    /// Syncronizes the asynchronous runs with asynchronous tasks in subscriptions.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeSynchronus_RunsWithAsyncTasksInSubscriptions()
    {
        // Given, When. SubscribeSynchronous dispatches each OnNext concurrently on the thread
        // pool, so result / itterations need Interlocked for the read-modify-write to be safe.
        var result = 0;
        var itterations = 0;
        var subject = new Subject<bool>();
        using var disposable = subject
            .SubscribeSynchronous(async x =>
            {
                if (x)
                {
                    await Task.Delay(1000);
                    Interlocked.Increment(ref result);
                }
                else
                {
                    await Task.Delay(500);
                    Interlocked.Decrement(ref result);
                }

                Interlocked.Increment(ref itterations);
            });

        subject.OnNext(true);
        subject.OnNext(false);
        subject.OnNext(true);
        subject.OnNext(false);
        subject.OnNext(true);
        subject.OnNext(false);

        while (Volatile.Read(ref itterations) < SampleValue6)
        {
            Thread.Yield();
        }

        // Then
        await Assert.That(Volatile.Read(ref result)).IsZero();
    }

    /// <summary>
    /// Syncronizes the asynchronous runs with asynchronous tasks in subscriptions.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SynchronizeSynchronous_RunsWithAsyncTasksInSubscriptions()
    {
        // Given, When. SynchronizeSynchronous dispatches each OnNext through an independent
        // Continuation so the six HandleAsync invocations run concurrently on the thread pool —
        // the int read-modify-write therefore needs Interlocked. The test asserts pair-wise
        // (+1, -1) sums to zero after WhenAll completes.
        var result = 0;
        var itterations = 0;
        var subject = new Subject<bool>();
        var tasks = new List<Task>();
        using var disposable = subject
            .SynchronizeSynchronous()
            .Subscribe(x => tasks.Add(HandleAsync(x)));

        async Task HandleAsync((bool Value, IDisposable Sync) x)
        {
            try
            {
                if (x.Value)
                {
                    await Task.Delay(LongDelayMilliseconds);
                    Interlocked.Increment(ref result);
                }
                else
                {
                    await Task.Delay(ShortDelayMilliseconds);
                    Interlocked.Decrement(ref result);
                }
            }
            finally
            {
                x.Sync.Dispose();
                Interlocked.Increment(ref itterations);
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
        await Assert.That(Volatile.Read(ref result)).IsZero();
    }

    /// <summary>
    /// Syncronizes the asynchronous runs with asynchronous tasks in subscriptions.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeAsync_RunsWithAsyncTasksInSubscriptions()
    {
        // Given, When. SubscribeAsync dispatches each OnNext concurrently, so the integer
        // read-modify-write needs Interlocked and the polling read needs Volatile.
        var result = 0;
        var itterations = 0;
        var subject = new Subject<bool>();
        using var disposable = subject
            .SubscribeAsync(async x =>
            {
                if (x)
                {
                    await Task.Delay(1000);
                    Interlocked.Increment(ref result);
                }
                else
                {
                    await Task.Delay(500);
                    Interlocked.Decrement(ref result);
                }

                Interlocked.Increment(ref itterations);
            });

        subject.OnNext(true);
        subject.OnNext(false);
        subject.OnNext(true);
        subject.OnNext(false);
        subject.OnNext(true);
        subject.OnNext(false);

        while (Volatile.Read(ref itterations) < SampleValue6)
        {
            Thread.Yield();
        }

        // Then
        await Assert.That(Volatile.Read(ref result)).IsZero();
    }

    /// <summary>
    /// Tests WithLimitedConcurrency.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous RxVoid test.</returns>
    [Test]
    public async Task WithLimitedConcurrency_LimitsConcurrentTasks()
    {
        var maxConcurrent = 0;
        var currentConcurrent = 0;

        IEnumerable<Task<int>> CreateTasks()
        {
            for (int i = 1; i <= SampleValue10; i++)
            {
                var value = i;
                yield return Task.Run(async () =>
                {
                    lock (_gate)
                    {
                        currentConcurrent++;
                        maxConcurrent = Math.Max(maxConcurrent, currentConcurrent);
                    }

                    await Task.Delay(SampleValue10);

                    lock (_gate)
                    {
                        currentConcurrent--;
                    }

                    return value;
                });
            }
        }

        var results = await CreateTasks().WithLimitedConcurrency(3).ToList();

        using (Assert.Multiple())
        {
            await Assert.That(results).Count().IsEqualTo(SampleValue10);
            await Assert.That(maxConcurrent).IsLessThanOrEqualTo(SampleValue3);
        }
    }

    /// <summary>Verifies an empty limited-concurrency task sequence completes immediately.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WithLimitedConcurrency_EmptyTaskSequence_Completes()
    {
        var completed = false;
        var values = new List<int>();

        using var sub = Array.Empty<Task<int>>()
            .WithLimitedConcurrency(SampleValue3)
            .Subscribe(values.Add, () => completed = true);

        await Assert.That(values).IsEmpty();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies faulted and canceled tasks stop enumeration and report the expected errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WithLimitedConcurrency_TaskFaultsOrCancels_ThenErrorsAndStops()
    {
        var expected = new InvalidOperationException("limited-concurrency");
        Exception? fault = null;
        Exception? canceled = null;
        var afterFaultPulled = false;

        IEnumerable<Task<int>> FaultingTasks()
        {
            yield return Task.FromException<int>(expected);
            afterFaultPulled = true;
            yield return Task.FromResult(SampleValue2);
        }

        using var faultSub = FaultingTasks()
            .WithLimitedConcurrency(1)
            .Subscribe(static _ => { }, ex => fault = ex);

        using var canceledSub = new[] { Task.FromCanceled<int>(new CancellationToken(canceled: true)) }
            .WithLimitedConcurrency(1)
            .Subscribe(static _ => { }, ex => canceled = ex);

        await Assert.That(fault).IsSameReferenceAs(expected);
        await Assert.That(canceled).IsTypeOf<OperationCanceledException>();
        await Assert.That(afterFaultPulled).IsFalse();
    }

    /// <summary>Verifies disposing before a pending task completes drops later notifications.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WithLimitedConcurrency_DisposeBeforeTaskContinuation_DropsWork()
    {
        var task = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var values = new List<int>();
        Exception? caught = null;
        var completed = false;

        var sub = new[] { task.Task }
            .WithLimitedConcurrency(1)
            .Subscribe(values.Add, ex => caught = ex, () => completed = true);

        sub.Dispose();
        task.SetResult(SampleValue10);
        await Task.Delay(SettleDelayMilliseconds).ConfigureAwait(false);

        await Assert.That(values).IsEmpty();
        await Assert.That(caught).IsNull();
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Exercises the internal disposed pull path used after a subscription is closed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WithLimitedConcurrency_PullAfterDisposed_ThenNoNotifications()
    {
        var limiter = new ConcurrencyLimiter<int>([Task.FromResult(SampleValue10)], 1)
        {
            Disposed = true
        };
        var values = new List<int>();
        var completed = false;

        limiter.PullNextTask(Observer.Create<int>(values.Add, static _ => { }, () => completed = true));

        await Assert.That(values).IsEmpty();
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Exercises the null-current continuation path defensively tolerated by the limiter.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WithLimitedConcurrency_NullTaskEntry_ThenNoNotifications()
    {
        IEnumerable<Task<int>> tasks = [null!];
        var values = new List<int>();
        Exception? caught = null;
        var completed = false;

        using var sub = tasks
            .WithLimitedConcurrency(1)
            .Subscribe(values.Add, ex => caught = ex, () => completed = true);

        await Task.Delay(SettleDelayMilliseconds).ConfigureAwait(false);

        await Assert.That(values).IsEmpty();
        await Assert.That(caught).IsNull();
        await Assert.That(completed).IsFalse();
    }

    /// <summary>
    /// Tests SynchronizeSynchronous provides sync lock.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SynchronizeSynchronous_ProvidesSyncLock()
    {
        var subject = new Subject<int>();
        var results = new List<int>();
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

    /// <summary>
    /// Tests SynchronizeAsync provides sync lock.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SynchronizeAsync_ProvidesSyncLock()
    {
        var subject = new Subject<int>();
        var results = new List<int>();
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

    /// <summary>
    /// Tests SubscribeAsync with onNext and onError.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous RxVoid test.</returns>
    [Test]
    public async Task SubscribeAsync_WithOnNextAndOnError_HandlesError()
    {
        var results = new List<int>();
        Exception? caughtException = null;
        var errorSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = Observable.Create<int>(observer =>
        {
            observer.OnNext(1);
            observer.OnError(new InvalidOperationException());
            return Disposable.Empty;
        });

        using var sub = source.SubscribeAsync(
            async x => results.Add(x),
            ex =>
            {
                caughtException = ex;
                errorSource.TrySetResult(true);
            });

        await errorSource.Task.WaitAsync(TimeSpan.FromSeconds(5));

        using (Assert.Multiple())
        {
            await Assert.That(results).IsCollectionEqualTo([1]);
            await Assert.That(caughtException).IsNotNull();
        }
    }

    /// <summary>
    /// Tests SubscribeSynchronous with full callbacks.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeSynchronous_WithFullCallbacks_ExecutesAll()
    {
        var subject = new Subject<int>();
        var results = new List<int>();
        var errorHandled = false;
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        subject.SubscribeSynchronous(
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

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        using (Assert.Multiple())
        {
            await Assert.That(results).IsCollectionEqualTo([1, SampleValue2]);
            await Assert.That(errorHandled).IsFalse();
            await Assert.That(completed.Task.IsCompletedSuccessfully).IsTrue();
        }
    }

    /// <summary>
    /// Tests SubscribeSynchronous with onNext and onError.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeSynchronous_WithOnNextAndOnError_HandlesError()
    {
        var subject = new Subject<int>();
        var results = new List<int>();
        var onNextCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var errorHandled = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        subject.SubscribeSynchronous(
            async v =>
            {
                await Task.Yield();
                results.Add(v);
                onNextCompleted.TrySetResult();
            },
            _ => errorHandled.TrySetResult());

        subject.OnNext(1);
        await onNextCompleted.Task.WaitAsync(TimeSpan.FromSeconds(5));

        subject.OnError(new InvalidOperationException());
        await errorHandled.Task.WaitAsync(TimeSpan.FromSeconds(5));

        using (Assert.Multiple())
        {
            await Assert.That(results).IsCollectionEqualTo([1]);
            await Assert.That(errorHandled.Task.IsCompletedSuccessfully).IsTrue();
        }
    }

    /// <summary>
    /// Tests SubscribeSynchronous with onNext and onCompleted.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeSynchronous_WithOnNextAndOnCompleted_CompletesCorrectly()
    {
        var subject = new Subject<int>();
        var results = new List<int>();
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        subject.SubscribeSynchronous(
            async v =>
            {
                await Task.Yield();
                results.Add(v);
            },
            () => completed.TrySetResult());

        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        subject.OnCompleted();

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        using (Assert.Multiple())
        {
            await Assert.That(results).IsCollectionEqualTo([1, SampleValue2]);
            await Assert.That(completed.Task.IsCompletedSuccessfully).IsTrue();
        }
    }

    /// <summary>
    /// Tests SubscribeSynchronous with only onNext.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeSynchronous_WithOnlyOnNext_ProcessesValues()
    {
        const int ExpectedCount = 3;
        var subject = new Subject<int>();
        var results = new List<int>();
        var allReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        subject.SubscribeSynchronous(
            async v =>
            {
                await Task.Yield();
                results.Add(v);
                _ = results.Count == ExpectedCount && allReceived.TrySetResult();
            });

        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        subject.OnNext(SampleValue3);

        await allReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(results).IsCollectionEqualTo([1, SampleValue2, SampleValue3]);
    }

    /// <summary>
    /// Tests SubscribeAsync with onNext and onCompleted.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SubscribeAsync_WithOnNextAndOnCompleted_CompletesCorrectly()
    {
        var results = new List<int>();
        var completed = false;
        var completionSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = Observable.Create<int>(observer =>
        {
            observer.OnNext(1);
            observer.OnNext(2);
            observer.OnCompleted();
            return Disposable.Empty;
        });

        using var subscription = source.SubscribeAsync(
            async v =>
            {
                await Task.Delay(1);
                results.Add(v);
            },
            () =>
            {
                completed = true;
                completionSource.TrySetResult(true);
            });

        await completionSource.Task.WaitAsync(TimeSpan.FromSeconds(5));

        using (Assert.Multiple())
        {
            await Assert.That(results).IsCollectionEqualTo([1, SampleValue2]);
            await Assert.That(completed).IsTrue();
        }
    }
}
