// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;

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

    /// <summary>Delay before the single-shot timer fires.</summary>
    private static readonly TimeSpan SingleShotDelay = TimeSpan.FromMilliseconds(50);

    /// <summary>Delay before the first tick of a periodic timer.</summary>
    private static readonly TimeSpan PeriodicDueTime = TimeSpan.FromMilliseconds(10);

    /// <summary>Interval between the ticks of a periodic timer.</summary>
    private static readonly TimeSpan PeriodicInterval = TimeSpan.FromMilliseconds(50);

    /// <summary>Tests Return emits single value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReturnSingleValue_ThenEmitsValueAndCompletes()
    {
        var result = await SignalAsync.Return(SentinelValue).ToListAsync();
        await Assert.That(result).Count().IsEqualTo(1);
        await Assert.That(result[0]).IsEqualTo(SentinelValue);
    }

    /// <summary>Tests Return emits string.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReturnString_ThenEmitsStringAndCompletes()
    {
        var result = await SignalAsync.Return("hello").ToListAsync();
        await Assert.That(result).IsCollectionEqualTo(["hello"]);
    }

    /// <summary>Tests Empty completes with no items.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenEmpty_ThenCompletesWithNoItems()
    {
        var result = await SignalAsync.Empty<int>().ToListAsync();
        await Assert.That(result).IsEmpty();
    }

    /// <summary>Tests Throw completes with exception.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrow_ThenCompletesWithException()
    {
        InvalidOperationException ex = new("test error");
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

    /// <summary>Tests Throw rejects null exception.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void WhenThrowNullException_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(static () => SignalAsync.Throw<int>(null!));

    /// <summary>Tests Never neither emits nor completes, including once its subscription token is cancelled.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenNever_ThenNeitherEmitsNorCompletes()
    {
        using CancellationTokenSource cts = new();
        List<int> items = [];
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
        await cts.CancelAsync();
        await Assert.That(items).IsEmpty();
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Tests Range from zero emits sequential integers.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRangeFromZero_ThenEmitsSequentialIntegers()
    {
        const int ExpectedThird = 2;
        const int ExpectedFourth = 3;
        const int ExpectedFifth = 4;
        const int SourceValueCount = 5;

        var result = await SignalAsync.Range(0, SourceValueCount).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([0, 1, ExpectedThird, ExpectedFourth, ExpectedFifth]);
    }

    /// <summary>Tests Range from non-zero emits correct range.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRangeFromNonZero_ThenEmitsCorrectRange()
    {
        const int ExpectedFirst = 10;
        const int ExpectedSecond = 11;
        const int ExpectedThird = 12;
        const int SourceValueCount = 3;

        var result = await SignalAsync.Range(ExpectedFirst, SourceValueCount).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([ExpectedFirst, ExpectedSecond, ExpectedThird]);
    }

    /// <summary>Tests Range with count zero emits nothing.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRangeCountZero_ThenEmitsNothing()
    {
        var result = await SignalAsync.Range(0, 0).ToListAsync();
        await Assert.That(result).IsEmpty();
    }

    /// <summary>Tests FromAsync with value emits single value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFromAsyncWithValue_ThenEmitsSingleValue()
    {
        const int ExpectedValue = 99;
        var source = SignalAsync.FromAsync(static async _ =>
        {
            await Task.Yield();
            return ExpectedValue;
        });
        var result = await source.ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([ExpectedValue]);
    }

    /// <summary>Tests FromAsync void executes action.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFromAsyncVoid_ThenEmitsUnit()
    {
        var executed = false;
        var source = SignalAsyncReactiveExtensions.FromAsync(async _ =>
        {
            await Task.Yield();
            executed = true;
        });
        await source.WaitCompletionAsync();
        await Assert.That(executed).IsTrue();
    }

    /// <summary>Tests Defer creates new sequence per subscription.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
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

    /// <summary>Tests async Defer creates new sequence per subscription.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
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

    /// <summary>Tests Create with custom subscription logic.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCreate_ThenCustomSubscriptionLogicRuns()
    {
        const int SecondItem = 2;
        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnNextAsync(SecondItem, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        var result = await source.ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([1, SecondItem]);
    }

    /// <summary>Tests Create with null subscribe function.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void WhenCreateWithNullSubscribeFunc_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(static () => SignalAsync.Create<int>(null!));

    /// <summary>Tests CreateAsBackgroundJob runs on background.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCreateAsBackgroundJob_ThenRunsOnBackground()
    {
        var source = SignalAsync.CreateAsBackgroundJob<int>(
            static async (observer, ct) =>
            {
                await Task.Yield();
                await observer.OnNextAsync(SentinelValue, ct);
                await observer.OnCompletedAsync(Result.Success);
            },
            new CustomTaskScheduler());
        var result = await source.ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([SentinelValue]);
    }

    /// <summary>Tests Timer single shot emits one value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTimerSingleShot_ThenEmitsSingleValueAfterDelay()
    {
        ManualTimeProvider time = new();
        var pending = SignalAsync.Timer(SingleShotDelay, time).ToListAsync().AsTask();
        var timer = await time.NextTimerAsync();
        await Assert.That(pending.IsCompleted).IsFalse();
        await Assert.That(timer.DueTime).IsEqualTo(SingleShotDelay);
        timer.Fire();
        await Assert.That(await pending).IsCollectionEqualTo([0L]);
    }

    /// <summary>Tests Timer periodic emits multiple values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTimerPeriodic_ThenEmitsMultipleValues()
    {
        const int SecondValue = 2;
        ManualTimeProvider time = new();
        var pending = SignalAsync.Timer(PeriodicDueTime, PeriodicInterval, time).Take(SecondValue).ToListAsync().AsTask();
        var first = await time.NextTimerAsync();
        await Assert.That(first.DueTime).IsEqualTo(PeriodicDueTime);
        first.Fire();
        var second = await time.NextTimerAsync();
        await Assert.That(second.DueTime).IsEqualTo(PeriodicInterval);
        second.Fire();
        await Assert.That(await pending).IsCollectionEqualTo([0L, 1L]);
    }

    /// <summary>Tests Timer negative due time.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void WhenTimerNegativeDueTime_ThenThrowsArgumentOutOfRange() =>
        Assert.Throws<ArgumentOutOfRangeException>(static () => SignalAsync.Timer(TimeSpan.FromMilliseconds(-1)));

    /// <summary>Tests Timer periodic with non-positive period.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void WhenTimerPeriodicNonPositivePeriod_ThenThrowsArgumentOutOfRange() =>
        Assert.Throws<ArgumentOutOfRangeException>(static () => SignalAsync.Timer(TimeSpan.Zero, TimeSpan.Zero));

    /// <summary>Tests IEnumerable to SignalAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenEnumerableToAsyncSignal_ThenEmitsAllItems()
    {
        const int ExpectedSecond = 2;
        const int ExpectedThird = 3;
        var source = Sequence123.ToAsyncSignal();
        var result = await source.ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([1, ExpectedSecond, ExpectedThird]);
    }

    /// <summary>Tests async observable ToAsyncSignal null source validation.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void WhenObservableToAsyncSignalWithNullSource_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(static () => ((IObservableAsync<int>)null!).ToAsyncSignal());

    /// <summary>Tests IAsyncEnumerable to SignalAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
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

    /// <summary>Tests Task to SignalAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTaskToAsyncSignal_ThenEmitsTaskResult()
    {
        const int ExpectedResult = 7;
        var task = Task.FromResult(ExpectedResult);
        var source = task.ToAsyncSignal();
        var result = await source.FirstAsync();
        await Assert.That(result).IsEqualTo(ExpectedResult);
    }

    /// <summary>Tests void Task to SignalAsync.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenVoidTaskToAsyncSignal_ThenEmitsUnit()
    {
        var source = Task.CompletedTask.ToAsyncSignal();
        await source.WaitCompletionAsync();
    }

    /// <summary>Tests Interval emits periodic values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenIntervalWithCancellation_ThenEmitsPeriodicValues()
    {
        const int SecondValue = 2;
        const long SecondTick = 2L;
        ManualTimeProvider time = new();
        var values = await time.RunAsync(SignalAsync.Interval(PeriodicInterval, time).Take(SecondValue).ToListAsync().AsTask());
        await Assert.That(values).IsCollectionEqualTo([1L, SecondTick]);
    }

    /// <summary>Tests that enumerable subscription emission returns early when the cancellation token is already cancelled.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenEnumerableSubscriptionWithCancelledToken_ThenReturnsEarly()
    {
        List<int> items = [];
        using CancellationTokenSource cts = new();
        await cts.CancelAsync();
        CallbackWitnessAsync<int> observer = new((x, _) =>
        {
            items.Add(x);
            return default;
        });
        const int RangeCount = 100;
        EnumerableSubscription<int> subscription = new(observer, Enumerable.Range(0, RangeCount));
        await TaskSignalState.ExecuteAsync(subscription, observer, cts.Token);
        await subscription.DisposeAsync();
        await Assert.That(items).IsEmpty();
    }

    /// <summary>Tests that the parameterless SubscribeAsync overload subscribes and disposes without error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncParameterless_ThenSubscribesAndDisposes()
    {
        await using var sub = await SignalAsync.Return(1).SubscribeAsync();
        await Assert.That(sub).IsNotNull();
    }

    /// <summary>Tests that SubscribeAsync with only an async onNext delegate and no cancellation token subscribes correctly.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncWithOnNextAsyncOnly_ThenReceivesItems()
    {
        List<int> items = [];
        TaskCompletionSource received = new(TaskCreationOptions.RunContinuationsAsynchronously);
        const int EmittedValue = 77;
        await using var sub = await SignalAsync.Return(EmittedValue).SubscribeAsync((x, _) =>
        {
            items.Add(x);
            IgnoredResult.Of(received.TrySetResult());
            return default;
        });
        await received.Task;
        await Assert.That(items).IsCollectionEqualTo([EmittedValue]);
    }

    /// <summary>Tests that SubscribeAsync with an async onNext delegate and explicit cancellation token subscribes correctly.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncWithOnNextAsyncAndCancellationToken_ThenReceivesItems()
    {
        List<int> items = [];
        TaskCompletionSource received = new(TaskCreationOptions.RunContinuationsAsynchronously);
        using CancellationTokenSource cts = new();
        const int EmittedValue = 55;
        await using var sub = await SignalAsync.Return(EmittedValue).SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                IgnoredResult.Of(received.TrySetResult());
                return default;
            },
            cts.Token);
        await received.Task;
        await Assert.That(items).IsCollectionEqualTo([EmittedValue]);
    }

    /// <summary>Tests that the sync SubscribeAsync overload invokes the onErrorResume action when an error occurs.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncSyncOverloadWithError_ThenInvokesOnErrorResume()
    {
        TaskCompletionSource<Exception> errorReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException("sync error"), ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        await using var sub = await source.SubscribeAsync(
            (Action<int>)(static _ => { }),
            ex => errorReceived.TrySetResult(ex),
            null,
            CancellationToken.None);
        var error = await errorReceived.Task;
        await Assert.That(error).IsTypeOf<InvalidOperationException>();
        await Assert.That(error.Message).IsEqualTo("sync error");
    }

    /// <summary>Tests that the sync SubscribeAsync overload invokes the onCompleted action when the sequence completes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncSyncOverloadWithCompletion_ThenInvokesOnCompleted()
    {
        TaskCompletionSource<Result> completedResult = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await SignalAsync.Return(1).SubscribeAsync(
            (Action<int>)(static _ => { }),
            null,
            r => completedResult.TrySetResult(r),
            CancellationToken.None);
        var result = await completedResult.Task;
        await Assert.That(result.IsSuccess).IsTrue();
    }

    /// <summary>Tests that SubscribeAsync with null onErrorResume completes normally.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncWithNullOnErrorResume_ThenCompletesNormally()
    {
        List<int> items = [];
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await SignalAsync.Return(SentinelValue).SubscribeAsync(
            (Action<int>)items.Add,
            null,
            _ => completed.TrySetResult(),
            CancellationToken.None);
        await completed.Task;
        await Assert.That(items).IsCollectionEqualTo([SentinelValue]);
    }

    /// <summary>Tests that SubscribeAsync with null onCompleted completes normally.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscribeAsyncWithNullOnCompleted_ThenCompletesNormally()
    {
        List<int> items = [];
        TaskCompletionSource received = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await SignalAsync.Return(SentinelValue).SubscribeAsync(
            (Action<int>)(x =>
            {
                items.Add(x);
                _ = received.TrySetResult();
            }),
            static _ => { },
            null,
            CancellationToken.None);
        await received.Task;
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
        _ = Assert.Throws<ArgumentNullException>(static () => SignalAsyncReactiveExtensions.FromAsync(Factory));
    }
}
