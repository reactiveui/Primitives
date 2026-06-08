// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using System.Reactive.Subjects;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for ReactiveExtensionsTests.</summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>String literal "changed" used by multiple tests.</summary>
    private const string ChangedValueLiteral = "changed";

    /// <summary>String literal "initial" used by multiple tests.</summary>
    private const string InitialValueLiteral = "initial";

    /// <summary>Stabilization window for scheduler-driven assertions.</summary>
    private const int SchedulerStabilizeMilliseconds = 100;

    /// <summary>Hoisted source array used by tests (was inline literal).</summary>
    private static readonly string[] SequenceTest123HelloTest456World = ["test123", "hello", "test456", "world"];

    /// <summary>Syncronizes the asynchronous runs with asynchronous tasks in subscriptions.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SyncronizeAsync_RunsWithAsyncTasksInSubscriptions()
    {
        // Given, When
        var result = 0;
        var itterations = 0;
        var subject = new Subject<bool>();
        var tasks = new List<Task>();
        using var disposable = subject
            .SynchronizeAsync()
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

        while (itterations < SampleValue6)
        {
            Thread.Yield();
        }

        // Then
        await Assert.That(result).IsZero();
    }

    /// <summary>Tests OnNext with params.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task OnNext_WithMultipleValues_PushesAll()
    {
        var results = new List<int>();
        var subject = new Subject<int>();
        using var sub = subject.Subscribe(results.Add);

        subject.OnNext(1, SampleValue2, SampleValue3, SampleValue4, SampleValue5);

        await Assert.That(results).IsCollectionEqualTo([1, SampleValue2, SampleValue3, SampleValue4, SampleValue5]);
    }

    /// <summary>Tests FromArray with scheduler.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task FromArray_WithScheduler_EmitsElements()
    {
        var source = new[] { 1, 2, 3, 4, 5 };
        var results = new List<int>();
        using var sub = source.FromArray(Sequencer.Immediate).Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo(source);
    }

    /// <summary>Tests Filter with regex.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Filter_WithRegex_FiltersStrings()
    {
        var source = SequenceTest123HelloTest456World.ToObservable();
        var results = new List<string>();
        using var sub = source.Filter(@"^test\d+$").Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo(["test123", "test456"]);
    }

    /// <summary>Tests Shuffle randomizes array.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Shuffle_RandomizesArray()
    {
        var original = Enumerable.Range(1, 100).ToArray();
        var source = Observable.Return(original.ToArray());
        int[]? result = null;
        using var sub = source.Shuffle().Subscribe(x => result = x);

        using (Assert.Multiple())
        {
            await Assert.That(result).IsNotNull();
            await Assert.That(result).Count().IsEqualTo(SchedulerWindowTicks);
            var sorted = result!.ToArray();
            Array.Sort(sorted);
            await Assert.That(sorted).IsCollectionEqualTo(original);
        }
    }

    /// <summary>Tests TakeUntil with predicate.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task TakeUntil_WithPredicate_CompletesWhenPredicateTrue()
    {
        var subject = new Subject<int>();
        var results = new List<int>();
        using var sub = subject.TakeUntil(x => x >= 5).Subscribe(results.Add);

        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        subject.OnNext(SampleValue5);
        subject.OnNext(SampleValue6);

        await Assert.That(results).IsCollectionEqualTo([1, SampleValue2, SampleValue5]);
    }

    /// <summary>Tests Partition splits sequence.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Partition_SplitsSequence()
    {
        var subject = new Subject<int>();
        var trueResults = new List<int>();
        var falseResults = new List<int>();

        var (trueObs, falseObs) = subject.Partition(x => x % SampleValue2 == 0);

        using var trueSub = trueObs.Subscribe(trueResults.Add);
        using var falseSub = falseObs.Subscribe(falseResults.Add);

        for (int i = 1; i <= SampleValue10; i++)
        {
            subject.OnNext(i);
        }

        subject.OnCompleted();

        using (Assert.Multiple())
        {
            await Assert.That(trueResults).IsCollectionEqualTo([SampleValue2, SampleValue4, SampleValue6, SampleValue8, SampleValue10]);
            await Assert.That(falseResults).IsCollectionEqualTo([1, SampleValue3, SampleValue5, SampleValue7, SampleValue9]);
        }
    }

    /// <summary>Tests WaitUntil takes first matching.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WaitUntil_TakesFirstMatching()
    {
        var subject = new Subject<int>();
        var results = new List<int>();
        using var sub = subject.WaitUntil(x => x > 5).Subscribe(results.Add);

        subject.OnNext(1);
        subject.OnNext(SampleValue3);
        subject.OnNext(SampleValue7);
        subject.OnNext(SampleValue9);

        await Assert.That(results).IsCollectionEqualTo([SampleValue7]);
    }

    /// <summary>Tests DoOnSubscribe executes on subscribe.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task DoOnSubscribe_ExecutesOnSubscribe()
    {
        var executed = false;
        var source = Observable.Return(1);
        using var sub = source.DoOnSubscribe(() => executed = true).Subscribe();

        await Assert.That(executed).IsTrue();
    }

    /// <summary>Tests DoOnDispose executes on dispose.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task DoOnDispose_ExecutesOnDispose()
    {
        var executed = false;
        var source = Observable.Never<int>();
        var sub = source.DoOnDispose(() => executed = true).Subscribe();

        sub.Dispose();

        await Assert.That(executed).IsTrue();
    }

    /// <summary>Tests Stale class with update.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Stale_WithUpdate_IsNotStale()
    {
        var stale = new Stale<int>(42);

        using (Assert.Multiple())
        {
            await Assert.That(stale.IsStale).IsFalse();
            await Assert.That(stale.Update).IsEqualTo(SampleValue42);
        }
    }

    /// <summary>Tests Stale class without update.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Stale_WithoutUpdate_IsStale()
    {
        var stale = new Stale<int>();

        await Assert.That(stale.IsStale).IsTrue();
    }

    /// <summary>Tests Stale throws on Update access when stale.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Stale_WithoutUpdate_ThrowsOnUpdateAccess()
    {
        var stale = new Stale<int>();

        var ex = Assert.Throws<InvalidOperationException>(() => _ = stale.Update);

        await Assert.That(ex).IsNotNull();
    }

    /// <summary>Tests Continuation can be disposed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Continuation_CanBeDisposed()
    {
        var continuation = new Continuation();

        continuation.Dispose();
    }

    /// <summary>Tests Continuation tracks completed phases.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Continuation_TracksCompletedPhases()
    {
        using var continuation = new Continuation();

        var phases = continuation.CompletedPhases;

        await Assert.That(phases).IsGreaterThanOrEqualTo(0);
    }

    /// <summary>Tests Pairwise emits previous and current pairs.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task Pairwise_EmitsPairs()
    {
        var subject = new Subject<int>();
        var results = new List<(int Previous, int Current)>();

        subject.Pairwise().Subscribe(results.Add);

        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        subject.OnNext(SampleValue3);

        using (Assert.Multiple())
        {
            await Assert.That(results).Count().IsEqualTo(SampleValue2);
            await Assert.That(results[0]).IsEqualTo((1, SampleValue2));
            await Assert.That(results[1]).IsEqualTo((SampleValue2, SampleValue3));
        }
    }

    /// <summary>Tests ScanWithInitial starts with initial value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ScanWithInitial_StartsWithInitial()
    {
        var subject = new Subject<int>();
        var results = new List<int>();

        subject.ScanWithInitial(SampleValue10, (acc, x) => acc + x).Subscribe(results.Add);

        subject.OnNext(1);
        subject.OnNext(SampleValue2);

        await Assert.That(results).IsCollectionEqualTo([SampleValue10, SampleValue11, SampleValue13]);
    }

    /// <summary>Tests SampleLatest samples latest on trigger.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SampleLatest_SamplesLatestOnTrigger()
    {
        var subject = new Subject<int>();
        var trigger = new Subject<object>();
        var results = new List<int>();

        subject.SampleLatest(trigger).Subscribe(results.Add);

        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        trigger.OnNext(new object()); // Should emit 2
        subject.OnNext(SampleValue3);
        trigger.OnNext(new object()); // Should emit 3

        await Assert.That(results).IsCollectionEqualTo([SampleValue2, SampleValue3]);
    }

    /// <summary>Tests SwitchIfEmpty switches to fallback when empty.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SwitchIfEmpty_SwitchesWhenEmpty()
    {
        var emptySubject = new Subject<int>();
        var fallbackSubject = new Subject<int>();
        var results = new List<int>();

        emptySubject.SwitchIfEmpty(fallbackSubject).Subscribe(results.Add);

        emptySubject.OnCompleted(); // Empty completes
        fallbackSubject.OnNext(SampleValue42);
        fallbackSubject.OnCompleted();

        await Assert.That(results).IsCollectionEqualTo([SampleValue42]);
    }

    /// <summary>Tests ToReadOnlyBehavior creates read-only behavior.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ToReadOnlyBehavior_CreatesReadOnly()
    {
        var (observable, observer) = ReactiveExtensions.ToReadOnlyBehavior(SampleValue10);
        var results = new List<int>();

        observable.Subscribe(results.Add);

        observer.OnNext(SampleValue20);
        observer.OnNext(SampleValue30);

        await Assert.That(results).IsCollectionEqualTo([SampleValue10, SampleValue20, SampleValue30]);
    }

    /// <summary>Tests ToHotTask converts to hot task.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ToHotTask_ConvertsToTask()
    {
        var subject = new Subject<int>();
        var task = subject.ToHotTask();

        subject.OnNext(SampleValue42);

        await Assert.That(await task).IsEqualTo(SampleValue42);
    }

    /// <summary>Tests ToHotValueTask converts to a hot value task that completes with the first value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ToHotValueTask_ConvertsToValueTask()
    {
        var subject = new Subject<int>();
        var task = subject.ToHotValueTask();

        subject.OnNext(SampleValue42);

        await Assert.That(await task).IsEqualTo(SampleValue42);
    }

    /// <summary>Tests ToPropertyObservable observes property changes.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ToPropertyObservable_ObservesProperty()
    {
        var obj = new TestNotifyPropertyChanged { TestProperty = InitialValueLiteral };
        var results = new List<string>();

        obj.ToPropertyObservable(x => x.TestProperty).Subscribe(results.Add);

        obj.TestProperty = ChangedValueLiteral;

        await Assert.That(results).IsCollectionEqualTo([InitialValueLiteral, ChangedValueLiteral]);
    }

    /// <summary>Tests SkipWhileNull emits values after the first non-null value, including later nulls.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task SkipWhileNull_WhenFirstValueArrives_EmitsRemainingValues()
    {
        IObservable<string> source = Observable.Create<string>(observer =>
        {
            observer.OnNext(null!);
            observer.OnNext(null!);
            observer.OnNext("first");
            observer.OnNext(null!);
            observer.OnNext("second");
            observer.OnCompleted();
            return EmptyDisposable.Instance;
        });
        var results = new List<string>();

        using var sub = source.SkipWhileNull().Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo(["first", null, "second"]);
    }

    /// <summary>Tests ReplayLastOnSubscribe replays last value to new subscribers.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task ReplayLastOnSubscribe_ReplaysLastValueToNewSubscribers()
    {
        var subject = new Subject<int>();
        var replayed = subject.ReplayLastOnSubscribe(SampleValue99);

        var results1 = new List<int>();
        using var sub1 = replayed.Subscribe(results1.Add);

        // First subscriber gets the per-subscription initial value.
        await Assert.That(results1).IsCollectionEqualTo([SampleValue99]);

        subject.OnNext(1);
        await Assert.That(results1).IsCollectionEqualTo([SampleValue99, 1]);

        var results2 = new List<int>();
        using var sub2 = replayed.Subscribe(results2.Add);

        // ReplayLastOnSubscribe creates a fresh BehaviorSubject per subscriber seeded with the initial value;
        // late subscribers therefore receive the initial value, not values emitted to earlier subscribers.
        await Assert.That(results2).IsCollectionEqualTo([SampleValue99]);

        subject.OnNext(SampleValue2);
        using (Assert.Multiple())
        {
            await Assert.That(results1).IsCollectionEqualTo([SampleValue99, 1, SampleValue2]);
            await Assert.That(results2).IsCollectionEqualTo([SampleValue99, SampleValue2]);
        }
    }

    /// <summary>Tests LatestOrDefault emits default first then distinct values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLatestOrDefault_ThenEmitsDefaultThenDistinctValues()
    {
        var subject = new Subject<int>();
        var results = new List<int>();

        subject.LatestOrDefault(SampleValue42).Subscribe(results.Add);

        subject.OnNext(SampleValue42); // Same as default, should be suppressed by DistinctUntilChanged
        subject.OnNext(1);
        subject.OnNext(1); // Duplicate, suppressed
        subject.OnNext(SampleValue2);

        await Assert.That(results).IsCollectionEqualTo([SampleValue42, 1, SampleValue2]);
    }

    /// <summary>Tests OnNext with null observer throws ArgumentNullException.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnNextWithNullObserver_ThenThrowsArgumentNullException()
    {
        IObserver<int>? observer = null;

        var ex = Assert.Throws<ArgumentNullException>(() => observer!.OnNext(1, 2, 3));

        await Assert.That(ex).IsNotNull();
    }

    /// <summary>Tests OnNext with null events array throws ArgumentNullException.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnNextWithNullEvents_ThenThrowsArgumentNullException()
    {
        var subject = new Subject<int>();

        var ex = Assert.Throws<ArgumentNullException>(() => subject.OnNext(null!));

        await Assert.That(ex).IsNotNull();
    }

    /// <summary>Tests SubscribeAsync with all three handlers (onNext, onError, onCompleted).</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncWithAllHandlers_ThenInvokesAll()
    {
        var subject = new Subject<int>();
        var results = new List<int>();
        var completed = false;
        Exception? caughtError = null;
        var completionSource = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        const int ExpectedCount = 2;
        var allReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var sub = subject.SubscribeAsync(
            async x =>
            {
                await Task.Yield();
                results.Add(x);
                _ = results.Count == ExpectedCount && allReceived.TrySetResult();
            },
            ex => caughtError = ex,
            () =>
            {
                completed = true;
                completionSource.TrySetResult(true);
            });

        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        await allReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));
        subject.OnCompleted();

        await completionSource.Task.WaitAsync(TimeSpan.FromSeconds(5));

        using (Assert.Multiple())
        {
            await Assert.That(results).IsCollectionEqualTo([1, SampleValue2]);
            await Assert.That(completed).IsTrue();
            await Assert.That(caughtError).IsNull();
        }
    }

    /// <summary>
    /// Tests ToPropertyObservable unsubscribes from PropertyChanged when disposed,
    /// exercising line 1428 of ReactiveExtensions.cs (the -= handler).
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenToPropertyObservableDisposed_ThenUnsubscribesFromPropertyChanged()
    {
        var obj = new TestNotifyPropertyChanged { TestProperty = InitialValueLiteral };
        var results = new List<string>();

        var sub = obj.ToPropertyObservable(x => x.TestProperty).Subscribe(results.Add);

        obj.TestProperty = ChangedValueLiteral;

        // Dispose the subscription, triggering the -= handler
        sub.Dispose();

        // Changes after dispose should not be observed
        obj.TestProperty = "afterDispose";

        await Assert.That(results).IsCollectionEqualTo([InitialValueLiteral, ChangedValueLiteral]);
    }

    /// <summary>Verifies <c>ScheduleSafe(action)</c> uses the scheduler when one is supplied.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScheduleSafeImmediateWithScheduler_ThenSchedulerIsUsed()
    {
        var scheduler = new VirtualClock();
        var ran = false;

        scheduler.ScheduleSafe(() => ran = true);

        await Assert.That(ran).IsFalse();
        scheduler.AdvanceBy(1);
        await Assert.That(ran).IsTrue();
    }

    /// <summary>Verifies <c>ScheduleSafe(dueTime, action)</c> uses the scheduler when one is supplied.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScheduleSafeDelayedWithScheduler_ThenSchedulerIsUsed()
    {
        const int DelayTicks = 50;
        var scheduler = new VirtualClock();
        var ran = false;

        scheduler.ScheduleSafe(TimeSpan.FromTicks(DelayTicks), () => ran = true);

        await Assert.That(ran).IsFalse();
        scheduler.AdvanceBy(DelayTicks + 1);
        await Assert.That(ran).IsTrue();
    }

    /// <summary>Verifies the two-argument <c>OnErrorRetry&lt;TSource,TException&gt;</c> overload retries indefinitely.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorRetryTypedTwoArgOverload_ThenRetriesAndCallsErrorHandler()
    {
        const int SuccessAttempt = 2;
        var attempts = 0;
        var values = new List<int>();
        var caught = new List<InvalidOperationException>();
        var source = Observable.Create<int>(observer =>
        {
            var attempt = Interlocked.Increment(ref attempts);
            observer.OnNext(attempt);
            if (attempt < SuccessAttempt)
            {
                observer.OnError(new InvalidOperationException("retry"));
            }
            else
            {
                observer.OnCompleted();
            }

            return EmptyDisposable.Instance;
        });

        using var sub = source.OnErrorRetry<int, InvalidOperationException>(caught.Add).Subscribe(values.Add);

        await Assert.That(values).IsCollectionEqualTo([1, SuccessAttempt]);
        await Assert.That(caught).Count().IsEqualTo(1);
    }

    /// <summary>Verifies the typed <c>OnErrorRetry&lt;TSource,TException&gt;</c> skips the error callback when the exception type does not match.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorRetryNonMatchingExceptionType_ThenOnErrorCallbackSkipped()
    {
        var caught = new List<NotSupportedException>();
        var values = new List<int>();
        var failure = new InvalidOperationException("wrong type");

        var source = Observable.Create<int>(observer =>
        {
            observer.OnNext(1);
            observer.OnError(failure);
            return EmptyDisposable.Instance;
        });

        using var sub = source.OnErrorRetry<int, NotSupportedException>(caught.Add, retryCount: 1, TimeSpan.Zero, Sequencer.Default)
            .Subscribe(values.Add, static _ => { });

        await Task.Delay(TimeSpan.FromMilliseconds(SchedulerStabilizeMilliseconds));

        await Assert.That(caught).IsEmpty();
        await Assert.That(values.Count).IsGreaterThanOrEqualTo(1);
    }

    /// <summary>Verifies the two-argument <c>RetryWithBackoff</c> overload retries until success.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithBackoffTwoArgOverload_ThenRetriesUntilSuccess()
    {
        const int SuccessAttempt = 2;
        var attempts = 0;
        var done = new TaskCompletionSource<List<int>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var values = new List<int>();
        var source = Observable.Create<int>(observer =>
        {
            var attempt = Interlocked.Increment(ref attempts);
            if (attempt < SuccessAttempt)
            {
                observer.OnError(new InvalidOperationException("retry"));
            }
            else
            {
                observer.OnNext(attempt);
                observer.OnCompleted();
            }

            return EmptyDisposable.Instance;
        });

        using var sub = source.RetryWithBackoff(maxRetries: 3, TimeSpan.FromMilliseconds(1))
            .Subscribe(values.Add, () => done.TrySetResult(values));

        var captured = await done.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(captured).IsCollectionEqualTo([SuccessAttempt]);
    }

    /// <summary>Verifies <c>ReplayLastOnSubscribe</c> throws when the source is null.</summary>
    [Test]
    public void WhenReplayLastOnSubscribeSourceNull_ThenThrows() =>
        Assert.Throws<ArgumentNullException>(static () => ReactiveExtensions.ReplayLastOnSubscribe(null!, 0));

    /// <summary>Verifies the two-argument <c>BufferUntilInactive</c> overload flushes a buffer on completion using the default scheduler.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBufferUntilInactiveTwoArgOverload_ThenFlushesBufferOnCompletion()
    {
        var subject = new Subject<int>();
        var results = new List<IList<int>>();
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        using var sub = subject.BufferUntilInactive(TimeSpan.FromSeconds(5))
            .Subscribe(results.Add, () => completed.TrySetResult());

        subject.OnNext(1);
        subject.OnNext(SampleValue2);
        subject.OnCompleted();

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(results.Count).IsGreaterThanOrEqualTo(1);
        await Assert.That(results[^1]).IsCollectionEqualTo([1, SampleValue2]);
    }

    /// <summary>Verifies <c>CatchReturn</c> substitutes the fallback value when the source errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchReturnSourceErrors_ThenEmitsFallbackAndCompletes()
    {
        const int Fallback = 99;
        var subject = new Subject<int>();
        var results = new List<int>();
        var completed = false;

        using var sub = subject.CatchReturn(Fallback).Subscribe(results.Add, () => completed = true);

        subject.OnNext(1);
        subject.OnError(new InvalidOperationException("boom"));

        await Assert.That(results).IsCollectionEqualTo([1, Fallback]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies <c>CatchReturnUnit</c> substitutes <see cref="RxVoid.Default"/> when the source errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchReturnUnitSourceErrors_ThenEmitsUnitAndCompletes()
    {
        var subject = new Subject<RxVoid>();
        var results = new List<RxVoid>();
        var completed = false;

        using var sub = subject.CatchReturnUnit().Subscribe(results.Add, () => completed = true);

        subject.OnError(new InvalidOperationException("boom"));

        await Assert.That(results).IsCollectionEqualTo([RxVoid.Default]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Exercises <c>CatchReturn</c>'s <c>OnCompleted</c> forwarder — when the source
    /// completes normally without erroring, completion passes through to the downstream.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchReturnSourceCompletesNormally_ThenForwardsCompletion()
    {
        const int Fallback = 99;
        var subject = new Subject<int>();
        var results = new List<int>();
        var completed = false;

        using var sub = subject.CatchReturn(Fallback).Subscribe(results.Add, () => completed = true);

        subject.OnNext(1);
        subject.OnCompleted();

        await Assert.That(results).IsCollectionEqualTo([1]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Exercises <c>CatchIgnore&lt;T,TException&gt;</c>'s <c>OnCompleted</c> forwarder —
    /// when the source completes normally, the observer passes completion straight through.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchIgnoreWithErrorActionSourceCompletesNormally_ThenForwardsCompletion()
    {
        var subject = new Subject<int>();
        var results = new List<int>();
        var completed = false;

        using var sub = subject.CatchIgnore<int, InvalidOperationException>(static _ => { })
            .Subscribe(results.Add, () => completed = true);

        subject.OnNext(1);
        subject.OnCompleted();

        await Assert.That(results).IsCollectionEqualTo([1]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Exercises the empty-on-error <c>CatchIgnore&lt;T&gt;</c> overload's <c>OnCompleted</c>
    /// forwarder — when the source completes normally, the observer forwards completion.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchIgnoreEmptyOverloadSourceCompletesNormally_ThenForwardsCompletion()
    {
        var subject = new Subject<int?>();
        var completed = false;

        using var sub = subject.CatchIgnore().Subscribe(static _ => { }, () => completed = true);

        subject.OnCompleted();

        await Assert.That(completed).IsTrue();
    }

    /// <summary>Exercises <c>ToPropertyObservable</c>'s <c>as MemberExpression ?? throw</c> branch
    /// — passing an expression whose body is not a member access raises ArgumentException.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenToPropertyObservableNonMemberExpression_ThenThrowsArgumentException()
    {
        var owner = new ToPropertyNonMemberOwner();

        Action call = () => owner.ToPropertyObservable(static _ => 1 + 1);
        var ex = Assert.Throws<ArgumentException>(call);

        await Assert.That(ex).IsNotNull();
    }

    /// <summary>Exercises <c>AsSignalObservable</c>'s <c>OnError</c> forwarder — the synthesized
    /// RxVoid-stream propagates the source's error verbatim.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsSignalSourceErrors_ThenForwardsError()
    {
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException("as-signal-error");

        using var sub = subject.AsSignal().Subscribe(static _ => { }, ex => caught = ex);

        subject.OnError(expected);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Test class for INotifyPropertyChanged.</summary>
    private sealed class TestNotifyPropertyChanged : INotifyPropertyChanged
    {
        /// <summary>Property changed event.</summary>
        public event PropertyChangedEventHandler? PropertyChanged;

        /// <summary>Gets or sets the test property.</summary>
        /// <returns>The test property value.</returns>
        public string TestProperty
        {
            get;
            set
            {
                if (field == value)
                {
                    return;
                }

                field = value;
                PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(nameof(TestProperty)));
            }
        }

        = string.Empty;
    }

    /// <summary>INPC owner whose property type lets us pass a non-member expression body
    /// (e.g. a literal arithmetic expression) into <c>ToPropertyObservable</c> so the
    /// <c>as MemberExpression ?? throw</c> guard fires. The <c>PropertyChanged</c> event is
    /// required by the interface but never raised — the guard short-circuits before
    /// subscription wiring runs.</summary>
    private sealed class ToPropertyNonMemberOwner : INotifyPropertyChanged
    {
        /// <inheritdoc/>
        public event PropertyChangedEventHandler? PropertyChanged
        {
            add => _ = value;
            remove => _ = value;
        }
    }
}
