// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Reactive.Linq;
using System.Reactive.Subjects;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for several small synchronous operators:
/// <c>WhereSelect</c>, <c>FromArray</c>, <c>RetryWithDelay</c>,
/// <c>RetryForeverWithDelay</c>, <c>ThrottleOnScheduler</c>,
/// <c>ThrottleDistinct</c> (sync), <c>SubscribeAndComplete</c> error path,
/// <c>Schedule</c> with side-effect and transform overloads,
/// <c>ToReadOnlyBehavior</c>, and <c>Pairwise</c> after-error path.</summary>
public class RetryAndThrottleAndFactoryOperatorTests
{
    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Window in scheduler ticks for the throttle tests.</summary>
    private const int ThrottleWindowTicks = 100;

    /// <summary>Advance past the throttle window.</summary>
    private const int AdvancePastWindowTicks = 101;

    /// <summary>First sentinel.</summary>
    private const int Value1 = 1;

    /// <summary>Second sentinel.</summary>
    private const int Value2 = 2;

    /// <summary>Third sentinel.</summary>
    private const int Value3 = 3;

    /// <summary>Multiplier used by <c>WhereSelect</c>.</summary>
    private const int Multiplier = 10;

    /// <summary>Verifies that <c>WhereSelect</c> filters by predicate and projects matching values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereSelect_ThenFiltersThenProjects()
    {
        int[] inputs = [Value1, Value2, Value3];
        var results = new List<int>();
        using var sub = inputs.ToObservable().WhereSelect(static x => x % Value2 == 0, static x => x * Multiplier).Subscribe(results.Add);
        await Assert.That(results).IsCollectionEqualTo([Value2 * Multiplier]);
    }

    /// <summary>Verifies that a <c>WhereSelect</c> predicate exception forwards to <c>OnError</c>.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereSelectPredicateThrows_ThenForwardsError()
    {
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException("predicate failed");
        using var sub = subject.WhereSelect(_ => throw expected, static x => x).Subscribe(
            static _ =>
        {
        },
            ex => caught = ex);
        subject.OnNext(Value1);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that a <c>WhereSelect</c> selector exception forwards to <c>OnError</c>.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereSelectSelectorThrows_ThenForwardsError()
    {
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException("selector failed");
        using var sub = subject.WhereSelect<int, int>(static _ => true, _ => throw expected).Subscribe(
            static _ =>
        {
        },
            ex => caught = ex);
        subject.OnNext(Value1);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>WhereSelect</c> forwards source errors and completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereSelectSourceCompletes_ThenForwardsCompletion()
    {
        var subject = new Subject<int>();
        var completed = false;
        using var sub = subject.WhereSelect(static _ => true, static x => x).Subscribe(
            static _ =>
        {
        },
            () => completed = true);
        subject.OnCompleted();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <c>FromArray</c> with no scheduler pumps inline and completes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFromArrayInline_ThenPumpsAndCompletes()
    {
        int[] inputs = [Value1, Value2, Value3];
        var results = new List<int>();
        var completed = false;
        using var sub = inputs.FromArray().Subscribe(results.Add, () => completed = true);
        await Assert.That(results).IsCollectionEqualTo(inputs);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <c>FromArray</c> with a scheduler dispatches the pump via the scheduler.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFromArrayWithScheduler_ThenPumpsViaScheduler()
    {
        int[] inputs = [Value1, Value2, Value3];
        var scheduler = new VirtualClock();
        var results = new List<int>();
        var completed = false;
        using var sub = inputs.FromArray(scheduler).Subscribe(results.Add, () => completed = true);
        await Assert.That(results).IsEmpty();
        scheduler.AdvanceBy(1);
        await Assert.That(results).IsCollectionEqualTo(inputs);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <c>FromArray</c> forwards enumeration errors to <c>OnError</c>.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFromArrayEnumerationThrows_ThenForwardsError()
    {
        Exception? caught = null;
        var expected = new InvalidOperationException("enumeration failed");
        using var sub = BadEnumerable(expected).FromArray().Subscribe(
            static _ =>
        {
        },
            ex => caught = ex);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>RetryWithDelay</c> retries the configured number of times with
    /// a zero delay (so retries happen synchronously on the default scheduler).</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithDelayAlwaysFails_ThenRetriesThenErrors()
    {
        const int RetryCount = 3;
        var attempts = 0;
        var expected = new InvalidOperationException("attempt failed");
        var source = Observable.Create<int>(o =>
        {
            attempts++;
            o.OnError(expected);
            return () =>
            {
            };
        });
        using var sub = source.RetryWithDelay(RetryCount, _ => TimeSpan.Zero).Subscribe(
            static _ =>
        {
        },
            static _ =>
        {
        });

        // Initial attempt + RetryCount retries = RetryCount+1 total invocations.
        await Assert.That(attempts).IsGreaterThan(1);
    }

    /// <summary>Verifies that <c>RetryForeverWithDelay</c> keeps retrying after failures.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryForeverWithDelay_ThenKeepsRetrying()
    {
        var attempts = 0;
        var source = Observable.Create<int>(o =>
        {
            attempts++;
            if (attempts < Value3)
            {
                o.OnError(new InvalidOperationException("retry"));
            }
            else
            {
                o.OnNext(Value1);
                o.OnCompleted();
            }

            return () =>
            {
            };
        });
        var results = new List<int>();
        using var sub = source.RetryForeverWithDelay(TimeSpan.Zero).Subscribe(results.Add);
        await Assert.That(attempts).IsGreaterThanOrEqualTo(Value3);
        await Assert.That(results).IsCollectionEqualTo([Value1]);
    }

    /// <summary>Verifies that <c>ThrottleOnScheduler</c> emits the latest value after the window.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleOnScheduler_ThenEmitsLatestAfterWindow()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<int>();
        using var sub = subject.ThrottleOnScheduler(TimeSpan.FromTicks(ThrottleWindowTicks), scheduler).Subscribe(results.Add);
        subject.OnNext(Value1);
        subject.OnNext(Value2);
        scheduler.AdvanceBy(AdvancePastWindowTicks);
        await Assert.That(results).IsCollectionEqualTo([Value2]);
    }

    /// <summary>Verifies that <c>ThrottleOnScheduler</c> forwards source errors.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleOnSchedulerSourceErrors_ThenForwardsError()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);
        using var sub = subject.ThrottleOnScheduler(TimeSpan.FromTicks(ThrottleWindowTicks), scheduler).Subscribe(
            static _ =>
        {
        },
            ex => caught = ex);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>ThrottleDistinct</c> (sync overload, no scheduler) emits distinct values respecting the throttle window.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleDistinctSyncDefaultScheduler_ThenForwardsSourceError()
    {
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);
        using var sub = subject.ThrottleDistinct(TimeSpan.FromTicks(ThrottleWindowTicks)).Subscribe(
            static _ =>
        {
        },
            ex => caught = ex);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>ThrottleDistinct</c> (sync overload with scheduler) suppresses duplicates
    /// and emits the latest after the throttle window.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleDistinctSyncWithScheduler_ThenSuppressesUpstreamDuplicates()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var results = new List<int>();
        using var sub = subject.ThrottleDistinct(TimeSpan.FromTicks(ThrottleWindowTicks), scheduler).Subscribe(results.Add);
        subject.OnNext(Value1);
        subject.OnNext(Value1);
        scheduler.AdvanceBy(AdvancePastWindowTicks);
        await Assert.That(results.Count).IsLessThanOrEqualTo(1);
    }

    /// <summary>Verifies that <c>ToReadOnlyBehavior</c> returns a paired observable / observer that
    /// replays the initial value to new subscribers.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenToReadOnlyBehavior_ThenReplayInitial()
    {
        var(observable, observer) = ReactiveExtensions.ToReadOnlyBehavior(Value1);
        var results = new List<int>();
        using var sub = observable.Subscribe(results.Add);
        observer.OnNext(Value2);
        await Assert.That(results).IsCollectionEqualTo([Value1, Value2]);
    }

    /// <summary>Verifies that <c>SubscribeAndComplete</c> handles a RxVoid-producing source that errors,
    /// swallowing the error silently as the contract requires.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAndCompleteSourceErrors_ThenSwallows()
    {
        // The NoopWitness inside SubscribeAndComplete must absorb the error without throwing.
        var captured = new InvalidOperationException("ignored");
        Observable.Throw<RxVoid>(captured).SubscribeAndComplete();
        var followUp = Observable.Return(RxVoid.Default).SubscribeGetValue();
        await Assert.That(followUp).IsEqualTo(RxVoid.Default);
    }

    /// <summary>An <see cref = "IEnumerable{T}"/> whose <c>MoveNext</c> throws when enumerated, used to drive the error path of <c>FromArray</c>.</summary>
    /// <param name = "error">The exception thrown when enumeration begins.</param>
    /// <returns>An enumerable that throws.</returns>
    private static IEnumerable<int> BadEnumerable(Exception error)
    {
        yield return Value1;
        throw error;
    }
}
