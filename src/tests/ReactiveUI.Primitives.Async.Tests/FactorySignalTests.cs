// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Internals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Tests for factory signals: Return, Empty, Throw, Never, Range, FromAsync, Defer, Create, Timer, Interval, ToAsyncSignal.
/// </summary>
public class FactorySignalTests
{
    /// <summary>Sentinel value (42) used by tests.</summary>
    private const int SentinelValue = 42;

    /// <summary>Hoisted source array used by tests (was inline literal).</summary>
    private static readonly int[] Sequence123 = [1, 2, 3];

    /// <summary>
    /// Tests Return emits single value.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReturnSingleValue_ThenEmitsValueAndCompletes()
    {
        var result = await SignalAsync.Return(SentinelValue).ToListAsync();

        await Assert.That(result).Count().IsEqualTo(1);
        await Assert.That(result[0]).IsEqualTo(SentinelValue);
    }

    /// <summary>
    /// Tests Return emits string.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReturnString_ThenEmitsStringAndCompletes()
    {
        var result = await SignalAsync.Return("hello").ToListAsync();

        await Assert.That(result).IsCollectionEqualTo(["hello"]);
    }

    /// <summary>
    /// Tests Empty completes with no items.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenEmpty_ThenCompletesWithNoItems()
    {
        var result = await SignalAsync.Empty<int>().ToListAsync();

        await Assert.That(result).IsEmpty();
    }

    /// <summary>
    /// Tests Throw completes with exception.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrow_ThenCompletesWithException()
    {
        var ex = new InvalidOperationException("test error");
        var source = SignalAsync.Throw<int>(ex);

        InvalidOperationException? thrown = null;
        try
        {
            await source.ToListAsync();
        }
        catch (InvalidOperationException caught)
        {
            thrown = caught;
        }

        await Assert.That(thrown).IsNotNull();
        await Assert.That(thrown!.Message).IsEqualTo("test error");
    }

    /// <summary>
    /// Tests Throw rejects null exception.
    /// </summary>
    [Test]
    public void WhenThrowNullException_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(() => SignalAsync.Throw<int>(null!));

    /// <summary>
    /// Tests Never does not complete within timeout.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenNever_ThenDoesNotCompleteWithinTimeout()
    {
        const int ObservationWindowMs = 250;
        using var cts = new CancellationTokenSource(200);
        var items = new List<int>();
        var completed = false;

        await using var sub = await SignalAsync.Never<int>().SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null,
            _ =>
            {
                completed = true;
                return default;
            },
            cts.Token);

        await Task.Delay(ObservationWindowMs);

        await Assert.That(items).IsEmpty();
        await Assert.That(completed).IsFalse();
    }

    /// <summary>
    /// Tests Range from zero emits sequential integers.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRangeFromZero_ThenEmitsSequentialIntegers()
    {
        const int ExpectedThird = 2;
        const int ExpectedFourth = 3;
        const int ExpectedFifth = 4;
        var result = await SignalAsync.Range(0, 5).ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([0, 1, ExpectedThird, ExpectedFourth, ExpectedFifth]);
    }

    /// <summary>
    /// Tests Range from non-zero emits correct range.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRangeFromNonZero_ThenEmitsCorrectRange()
    {
        const int ExpectedFirst = 10;
        const int ExpectedSecond = 11;
        const int ExpectedThird = 12;
        var result = await SignalAsync.Range(10, 3).ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([ExpectedFirst, ExpectedSecond, ExpectedThird]);
    }

    /// <summary>
    /// Tests Range with count zero emits nothing.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRangeCountZero_ThenEmitsNothing()
    {
        var result = await SignalAsync.Range(0, 0).ToListAsync();

        await Assert.That(result).IsEmpty();
    }

    /// <summary>
    /// Tests FromAsync with value emits single value.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFromAsyncWithValue_ThenEmitsSingleValue()
    {
        const int ExpectedValue = 99;
        var source = SignalAsync.FromAsync(async _ =>
        {
            await Task.Yield();
            return ExpectedValue;
        });

        var result = await source.ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([ExpectedValue]);
    }

    /// <summary>
    /// Tests FromAsync void executes action.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFromAsyncVoid_ThenEmitsUnit()
    {
        var executed = false;
        var source = SignalAsync.FromAsync(async _ =>
        {
            await Task.Yield();
            executed = true;
        });

        await source.WaitCompletionAsync();

        await Assert.That(executed).IsTrue();
    }

    /// <summary>
    /// Tests Defer creates new sequence per subscription.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDefer_ThenCreatesNewSequencePerSubscription()
    {
        const int ExpectedSecondValue = 2;
        var counter = 0;
        var source = SignalAsync.Defer(() =>
        {
            counter++;
            return SignalAsync.Return(counter);
        });

        var first = await source.FirstAsync();
        var second = await source.FirstAsync();

        await Assert.That(first).IsEqualTo(1);
        await Assert.That(second).IsEqualTo(ExpectedSecondValue);
    }

    /// <summary>
    /// Tests async Defer creates new sequence per subscription.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDeferAsync_ThenCreatesNewSequencePerSubscription()
    {
        const int ExpectedSecondValue = 2;
        var counter = 0;
        var source = SignalAsync.Defer(async _ =>
        {
            await Task.Yield();
            counter++;
            return SignalAsync.Return(counter);
        });

        var first = await source.FirstAsync();
        var second = await source.FirstAsync();

        await Assert.That(first).IsEqualTo(1);
        await Assert.That(second).IsEqualTo(ExpectedSecondValue);
    }

    /// <summary>
    /// Tests Create with custom subscription logic.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCreate_ThenCustomSubscriptionLogicRuns()
    {
        const int SecondItem = 2;
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnNextAsync(SecondItem, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        var result = await source.ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([1, SecondItem]);
    }

    /// <summary>
    /// Tests Create with null subscribe function.
    /// </summary>
    [Test]
    public void WhenCreateWithNullSubscribeFunc_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(() =>
            SignalAsync.Create<int>(null!));

    /// <summary>
    /// Tests CreateAsBackgroundJob runs on background.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCreateAsBackgroundJob_ThenRunsOnBackground()
    {
        var source = SignalAsync.CreateAsBackgroundJob<int>(
            async (observer, ct) =>
        {
            await Task.Yield();
            await observer.OnNextAsync(SentinelValue, ct);
            await observer.OnCompletedAsync(Result.Success);
        },
            NewThreadTaskScheduler.Instance);

        var result = await source.ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([SentinelValue]);
    }

    /// <summary>
    /// Tests Timer single shot emits one value.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTimerSingleShot_ThenEmitsSingleValueAfterDelay()
    {
        var source = SignalAsync.Timer(TimeSpan.FromMilliseconds(50));

        var result = await source.ToListAsync();

        await Assert.That(result).Count().IsEqualTo(1);
        await Assert.That(result[0]).IsEqualTo(0L);
    }

    /// <summary>
    /// Tests Timer periodic emits multiple values.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTimerPeriodic_ThenEmitsMultipleValues()
    {
        const int MinimumEmissions = 2;
        var source = SignalAsync.Timer(
            TimeSpan.FromMilliseconds(10),
            TimeSpan.FromMilliseconds(50));

        var items = new List<long>();
        await using var sub = await source.SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => items.Count >= MinimumEmissions,
            TimeSpan.FromSeconds(5));

        await Assert.That(items.Count).IsGreaterThanOrEqualTo(MinimumEmissions);
        await Assert.That(items[0]).IsEqualTo(0L);
    }

    /// <summary>
    /// Tests Timer negative due time.
    /// </summary>
    [Test]
    public void WhenTimerNegativeDueTime_ThenThrowsArgumentOutOfRange() =>
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            SignalAsync.Timer(TimeSpan.FromMilliseconds(-1)));

    /// <summary>
    /// Tests Timer periodic with non-positive period.
    /// </summary>
    [Test]
    public void WhenTimerPeriodicNonPositivePeriod_ThenThrowsArgumentOutOfRange() =>
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            SignalAsync.Timer(TimeSpan.Zero, TimeSpan.Zero));

    /// <summary>
    /// Tests IEnumerable to SignalAsync.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenEnumerableToAsyncSignal_ThenEmitsAllItems()
    {
        const int ExpectedSecond = 2;
        const int ExpectedThird = 3;
        var source = Sequence123.ToAsyncSignal();

        var result = await source.ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([1, ExpectedSecond, ExpectedThird]);
    }

    /// <summary>
    /// Tests IAsyncEnumerable to SignalAsync.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncEnumerableToAsyncSignal_ThenEmitsAllItems()
    {
        const int FirstYield = 10;
        const int SecondYield = 20;
        const int ThirdYield = 30;
        var source = AsyncEnumerable().ToAsyncSignal();

        var result = await source.ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([FirstYield, SecondYield, ThirdYield]);

        static async IAsyncEnumerable<int> AsyncEnumerable()
        {
            const int FirstYield = 10;
            const int SecondYield = 20;
            const int ThirdYield = 30;
            yield return FirstYield;
            await Task.Yield();
            yield return SecondYield;
            await Task.Yield();
            yield return ThirdYield;
        }
    }

    /// <summary>
    /// Tests Task to SignalAsync.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTaskToAsyncSignal_ThenEmitsTaskResult()
    {
        const int ExpectedResult = 7;
        var task = Task.FromResult(7);
        var source = task.ToAsyncSignal();

        var result = await source.FirstAsync();

        await Assert.That(result).IsEqualTo(ExpectedResult);
    }

    /// <summary>
    /// Tests void Task to SignalAsync.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenVoidTaskToAsyncSignal_ThenEmitsUnit()
    {
        var task = Task.CompletedTask;
        var source = task.ToAsyncSignal();

        await source.WaitCompletionAsync();
    }

    /// <summary>
    /// Tests Interval emits periodic values.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenIntervalWithCancellation_ThenEmitsPeriodicValues()
    {
        const int MinimumEmissions = 2;
        using var cts = new CancellationTokenSource();
        var source = SignalAsync.Interval(TimeSpan.FromMilliseconds(50));

        var items = new List<long>();
        var received = false;
        try
        {
            await using var sub = await source.SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null,
                null,
                cts.Token);
            received = await AsyncTestHelpers.WaitForConditionAsync(
                () => items.Count >= 2,
                TimeSpan.FromSeconds(5));
        }
        catch (OperationCanceledException)
        {
            // Expected — the timer is being cancelled to end the test.
        }
        finally
        {
            if (!cts.IsCancellationRequested)
            {
                await cts.CancelAsync();
            }
        }

        await Assert.That(received).IsTrue();
        await Assert.That(items.Count).IsGreaterThanOrEqualTo(MinimumEmissions);
        await Assert.That(items[0]).IsEqualTo(1L);
    }

    /// <summary>
    /// Tests that EmitEnumerableAsync returns early when the cancellation token is already cancelled.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenEmitEnumerableAsyncWithCancelledToken_ThenReturnsEarly()
    {
        var items = new List<int>();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var observer = new DelegateAsyncWitness<int>((x, _) =>
        {
            items.Add(x);
            return default;
        });

        const int RangeCount = 100;
        await SignalAsync.EmitEnumerableAsync(Enumerable.Range(0, RangeCount), observer, cts.Token);

        await Assert.That(items).IsEmpty();
    }

    /// <summary>
    /// Tests that the parameterless SubscribeAsync overload subscribes and disposes without error.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncParameterless_ThenSubscribesAndDisposes()
    {
        await using var sub = await SignalAsync.Return(1).SubscribeAsync();

        await Assert.That(sub).IsNotNull();
    }

    /// <summary>
    /// Tests that SubscribeAsync with only an async onNext delegate and no cancellation token subscribes correctly.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncWithOnNextAsyncOnly_ThenReceivesItems()
    {
        var items = new List<int>();
        var received = new TaskCompletionSource();

        const int EmittedValue = 77;
        await using var sub = await SignalAsync.Return(EmittedValue).SubscribeAsync((x, _) =>
        {
            items.Add(x);
            received.TrySetResult();
            return default;
        });

        await received.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).IsCollectionEqualTo([EmittedValue]);
    }

    /// <summary>
    /// Tests that SubscribeAsync with an async onNext delegate and explicit cancellation token subscribes correctly.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncWithOnNextAsyncAndCancellationToken_ThenReceivesItems()
    {
        var items = new List<int>();
        var received = new TaskCompletionSource();
        using var cts = new CancellationTokenSource();

        const int EmittedValue = 55;
        await using var sub = await SignalAsync.Return(EmittedValue).SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                received.TrySetResult();
                return default;
            },
            cts.Token);

        await received.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).IsCollectionEqualTo([EmittedValue]);
    }

    /// <summary>
    /// Tests that the sync SubscribeAsync overload invokes the onErrorResume action when an error occurs.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncSyncOverloadWithError_ThenInvokesOnErrorResume()
    {
        var errorReceived = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException("sync error"), ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        await using var sub = await source.SubscribeAsync(
            (Action<int>)(_ => { }),
            onErrorResume: ex => errorReceived.TrySetResult(ex),
            onCompleted: null,
            CancellationToken.None);

        var error = await errorReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(error).IsTypeOf<InvalidOperationException>();
        await Assert.That(error.Message).IsEqualTo("sync error");
    }

    /// <summary>
    /// Tests that the sync SubscribeAsync overload invokes the onCompleted action when the sequence completes.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncSyncOverloadWithCompletion_ThenInvokesOnCompleted()
    {
        var completedResult = new TaskCompletionSource<Result>();

        await using var sub = await SignalAsync.Return(1).SubscribeAsync(
            (Action<int>)(_ => { }),
            onErrorResume: null,
            onCompleted: r => completedResult.TrySetResult(r),
            CancellationToken.None);

        var result = await completedResult.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(result.IsSuccess).IsTrue();
    }

    /// <summary>
    /// Tests that SubscribeAsync with null onErrorResume completes normally.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncWithNullOnErrorResume_ThenCompletesNormally()
    {
        var items = new List<int>();
        var completed = new TaskCompletionSource();

        await using var sub = await SignalAsync.Return(SentinelValue).SubscribeAsync(
            (Action<int>)items.Add,
            onErrorResume: null,
            onCompleted: _ => completed.TrySetResult(),
            CancellationToken.None);

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).IsCollectionEqualTo([SentinelValue]);
    }

    /// <summary>
    /// Tests that SubscribeAsync with null onCompleted completes normally.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncWithNullOnCompleted_ThenCompletesNormally()
    {
        var items = new List<int>();
        var received = new TaskCompletionSource();

        await using var sub = await SignalAsync.Return(SentinelValue).SubscribeAsync(
            (Action<int>)(x =>
            {
                items.Add(x);
                received.TrySetResult();
            }),
            onErrorResume: _ => { },
            onCompleted: null,
            CancellationToken.None);

        await received.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(items).IsCollectionEqualTo([SentinelValue]);
    }

    /// <summary>
    /// Verifies that FromAsync(Func of CancellationToken, ValueTask) throws ArgumentNullException when the factory is null.
    /// Covers the null guard in SignalAsync.FromAsync.
    /// </summary>
    [Test]
    public void WhenFromAsyncWithNullFactory_ThenThrowsArgumentNull()
    {
        const Func<CancellationToken, ValueTask> Factory = null!;
        Assert.Throws<ArgumentNullException>(() => SignalAsync.FromAsync(Factory));
    }
}
