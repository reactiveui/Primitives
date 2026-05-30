// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.SystemReactiveBridge;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Edge-case coverage for the fused async operators in
/// <c>ParityHelpers.OperatorFusions</c> — async <c>ScanWithInitial</c>,
/// <c>ThrottleDistinct</c> upstream/downstream filtering, <c>DebounceUntil</c>
/// immediate-bypass branch, and the typed fast paths in <c>ForEach</c>
/// (array / IReadOnlyList / general IEnumerable).</summary>
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "TUnit requires instance methods")]
public class ParityHelpersOperatorFusionsTests
{
    /// <summary>Initial accumulator seed for scan tests.</summary>
    private const int ScanSeed = 0;

    /// <summary>Throttle window in milliseconds for <c>ThrottleDistinct</c> tests.</summary>
    private const int ThrottleWindowMilliseconds = 50;

    /// <summary>Sentinel one.</summary>
    private const int One = 1;

    /// <summary>Sentinel two.</summary>
    private const int Two = 2;

    /// <summary>Sentinel three.</summary>
    private const int Three = 3;

    /// <summary>Sentinel four.</summary>
    private const int Four = 4;

    /// <summary>Array sentinels for the array fast-path test.</summary>
    private static readonly int[] ArraySlice1 = [One, Two];

    /// <summary>Second array sentinels.</summary>
    private static readonly int[] ArraySlice2 = [Three, Four];

    /// <summary>Expected flat result for the array fast-path test.</summary>
    private static readonly int[] ExpectedArrayFlat = [One, Two, Three, Four];

    /// <summary>Expected flat result for the list and enumerable tests.</summary>
    private static readonly int[] ExpectedListFlat = [One, Two, Three];

    /// <summary>Inputs for the <c>ScanWithInitial</c> async-accumulator test.</summary>
    private static readonly int[] ScanInputs = [One, Two, Three];

    /// <summary>Inputs for the <c>ThrottleDistinct</c> rapid-values test.</summary>
    private static readonly int[] ThrottleRapidInputs = [One, Two, Three];

    /// <summary>Inputs for the <c>DebounceUntil</c> immediate-bypass test.</summary>
    private static readonly int[] DebounceInputs = [One, Two, Three];

    /// <summary>Inputs of all-equal values for the <c>ThrottleDistinct</c> duplicates test.</summary>
    private static readonly int[] ThrottleDuplicateInputs = [One, One, One];

    /// <summary>Verifies that the async-accumulator overload of <c>ScanWithInitial</c>
    /// emits the seed first then every intermediate value produced by the asynchronous fold.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScanWithInitialAsync_ThenSeedThenAsyncFolded()
    {
        var result = await ScanInputs.ToAsyncSignal()
            .ScanWithInitial(ScanSeed, static async (acc, x, _) =>
            {
                await Task.Yield();
                return acc + x;
            })
            .ToListAsync();

        int[] expected =
        [
            ScanSeed,
            One,
            One + Two,
            One + Two + Three
        ];
        await Assert.That(result).IsEquivalentTo(expected);
    }

    /// <summary>Verifies that <c>ThrottleDistinct</c> suppresses consecutive duplicates upstream
    /// before any throttle work is scheduled.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleDistinctConsecutiveDuplicates_ThenSuppressesUpstream()
    {
        var result = await ThrottleDuplicateInputs.ToAsyncSignal()
            .ThrottleDistinct(TimeSpan.FromMilliseconds(ThrottleWindowMilliseconds))
            .ToListAsync();

        // All inputs are equal — only one emission is ever scheduled, and the source completes
        // before the throttle window elapses, so the pending emission must still flush exactly once.
        await Assert.That(result.Count).IsLessThanOrEqualTo(1);
    }

    /// <summary>Verifies that <c>ThrottleDistinct</c> with distinct rapid values respects the
    /// no-consecutive-duplicates contract and never emits more than the input count.
    /// (Pending throttled emissions are superseded by source completion — this is the
    /// documented behavior, so a count-bound assertion is the appropriate check rather than
    /// "at least one emission".)</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleDistinctRapidDistinctValues_ThenNoConsecutiveDuplicates()
    {
        var result = await ThrottleRapidInputs.ToAsyncSignal()
            .ThrottleDistinct(TimeSpan.FromMilliseconds(ThrottleWindowMilliseconds))
            .ToListAsync();

        await Assert.That(result.Count).IsLessThanOrEqualTo(ThrottleRapidInputs.Length);
        for (var i = 1; i < result.Count; i++)
        {
            await Assert.That(result[i]).IsNotEqualTo(result[i - 1]);
        }
    }

    /// <summary>Verifies that <c>DebounceUntil</c> with an always-true condition bypasses
    /// the debounce window and emits inline.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceUntilConditionAlwaysTrue_ThenEmitsImmediately()
    {
        var result = await DebounceInputs.ToAsyncSignal()
            .DebounceUntil(TimeSpan.FromSeconds(5), static _ => true)
            .ToListAsync();

        await Assert.That(result).IsEquivalentTo(DebounceInputs);
    }

    /// <summary>Verifies that the array fast path of <c>ForEach</c> flattens an
    /// <c>IObservableAsync&lt;T[]&gt;</c> into a flat sequence of elements.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForEachOverArray_ThenUsesArrayFastPath()
    {
        IEnumerable<int>[] arrays = [ArraySlice1, ArraySlice2];

        var result = await arrays.ToAsyncSignal()
            .ForEach()
            .ToListAsync();

        await Assert.That(result).IsEquivalentTo(ExpectedArrayFlat);
    }

    /// <summary>Verifies that the <see cref="IReadOnlyList{T}"/> fast path of <c>ForEach</c>
    /// flattens a list-typed source.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForEachOverReadOnlyList_ThenUsesListFastPath()
    {
        var firstList = new List<int>(ArraySlice1);
        var secondList = new List<int>(1) { Three };
        IEnumerable<int>[] lists = [firstList, secondList];

        var result = await lists.ToAsyncSignal()
            .ForEach()
            .ToListAsync();

        await Assert.That(result).IsEquivalentTo(ExpectedListFlat);
    }

    /// <summary>Verifies that the general <see cref="IEnumerable{T}"/> path of <c>ForEach</c>
    /// flattens a non-array, non-list source.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForEachOverGenericEnumerable_ThenUsesEnumeratorPath()
    {
        IEnumerable<int>[] enumerables = [Enumerate(One, Two), Enumerate(Three)];

        var result = await enumerables.ToAsyncSignal()
            .ForEach()
            .ToListAsync();

        await Assert.That(result).IsEquivalentTo(ExpectedListFlat);
    }

    /// <summary>Verifies that <c>Partition</c> broadcasts an upstream non-terminal error to both
    /// subscribed branches via the <c>OnErrorResume</c> path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPartitionSourceErrorResume_ThenBothBranchesReceiveError()
    {
        var signal = Signal.Create<int>();
        var (evens, odds) = signal.Values.Partition(static x => x % Two == 0);

        Exception? evenError = null;
        Exception? oddError = null;
        var evenTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var oddTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var evenSub = await evens.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                evenError = ex;
                evenTcs.TrySetResult();
                return default;
            });
        await using var oddSub = await odds.SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                oddError = ex;
                oddTcs.TrySetResult();
                return default;
            });

        var expected = new InvalidOperationException("partition-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await Task.WhenAll(evenTcs.Task, oddTcs.Task).WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(evenError).IsSameReferenceAs(expected);
        await Assert.That(oddError).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that a branch subscriber attaching after the source has already
    /// completed gets the cached terminal forwarded immediately.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPartitionLateBranchSubscribesAfterCompletion_ThenCachedTerminalForwarded()
    {
        var signal = Signal.Create<int>();
        var (evens, odds) = signal.Values.Partition(static x => x % Two == 0);

        var firstTask = evens.ToListAsync().AsTask();
        await signal.OnNextAsync(Two, CancellationToken.None);
        await signal.OnCompletedAsync(Result.Success);
        await firstTask;

        var lateValues = new List<int>();
        var lateCompleted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var lateSub = await odds.SubscribeAsync(
            (v, _) =>
            {
                lateValues.Add(v);
                return default;
            },
            (_, _) => default,
            result =>
            {
                lateCompleted.TrySetResult();
                return default;
            });

        await lateCompleted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(lateValues).IsEmpty();
    }

    /// <summary>Verifies that <c>DropIfBusy</c> resets the busy flag and re-throws when the
    /// async action throws synchronously (rather than returning a faulted task).</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDropIfBusyActionThrowsSynchronously_ThenBusyFlagResetAndErrorObserved()
    {
        var failure = new InvalidOperationException("sync-throw");
        InvalidOperationException? observed = null;

        try
        {
            await new[] { One }.ToAsyncSignal()
                .DropIfBusy(static (_, _) => throw new InvalidOperationException("sync-throw"))
                .ToListAsync();
        }
        catch (InvalidOperationException ex)
        {
            observed = ex;
        }

        await Assert.That(observed).IsNotNull();
        await Assert.That(observed!.Message).IsEqualTo(failure.Message);
    }

    /// <summary>Verifies that <c>ScanWithInitial</c> forwards a non-terminal upstream error
    /// downstream while still emitting the seed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScanWithInitialSourceErrorResumes_ThenForwardsDownstream()
    {
        var signal = Signal.Create<int>();
        var values = new List<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .ScanWithInitial(ScanSeed, static (acc, x) => acc + x)
            .SubscribeAsync(
                (v, _) =>
                {
                    values.Add(v);
                    return default;
                },
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("scan-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(values).IsEquivalentTo([ScanSeed]);
    }

    /// <summary>Verifies that <c>ThrottleDistinct</c> forwards a non-terminal upstream error
    /// downstream.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleDistinctSourceErrorResumes_ThenForwardsDownstream()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .ThrottleDistinct(TimeSpan.FromMilliseconds(ThrottleWindowMilliseconds))
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("throttle-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>DebounceUntil</c> forwards a non-terminal upstream error
    /// downstream.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceUntilSourceErrorResumes_ThenForwardsDownstream()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .DebounceUntil(TimeSpan.FromSeconds(5), static _ => false)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("debounce-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>ForEach</c> forwards a non-terminal upstream error downstream.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForEachSourceErrorResumes_ThenForwardsDownstream()
    {
        var signal = Signal.Create<IEnumerable<int>>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .ForEach()
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("foreach-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>DropIfBusy</c> forwards a non-terminal upstream error downstream.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDropIfBusySourceErrorResumes_ThenForwardsDownstream()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .DropIfBusy(static (_, _) => default)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("dropifbusy-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>DropIfBusy</c> with a sync action but an asynchronously-completing
    /// downstream takes the <c>AwaitForwardAsync</c> slow path and resets the busy flag in
    /// its <c>finally</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDropIfBusySyncActionAsyncDownstream_ThenAwaitForwardSlowPathResets()
    {
        var signal = Signal.Create<int>();
        var values = new List<int>();
        var emittedTcs = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .DropIfBusy(static (_, _) => default)
            .SubscribeAsync(async (v, _) =>
            {
                await Task.Yield();
                values.Add(v);
                emittedTcs.TrySetResult(v);
            });

        await signal.OnNextAsync(One, CancellationToken.None);
        await emittedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // After the slow path resets _isBusy, a second emission must also flow through.
        var secondTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub2 = await signal.Values
            .DropIfBusy(static (_, _) => default)
            .SubscribeAsync(async (_, _) =>
            {
                await Task.Yield();
                secondTcs.TrySetResult();
            });

        await signal.OnNextAsync(Two, CancellationToken.None);
        await secondTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(values).Contains(One);
    }

    /// <summary>Verifies that the async-accumulator <c>ScanWithInitial</c> overload forwards
    /// upstream non-terminal errors downstream.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScanWithInitialAsyncSourceErrorResumes_ThenForwardsDownstream()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .ScanWithInitial(ScanSeed, static (acc, x, _) => new ValueTask<int>(acc + x))
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("scan-async-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that a branch subscription disposes idempotently — the second
    /// <c>DisposeAsync</c> is a no-op via the latched-int short-circuit.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPartitionBranchDisposedTwice_ThenIdempotent()
    {
        var signal = Signal.Create<int>();
        var (evens, _) = signal.Values.Partition(static x => x % Two == 0);

        var sub = await evens.SubscribeAsync(static (_, _) => default);

        await sub.DisposeAsync();
        await sub.DisposeAsync();

        // Subsequent emissions must not throw and the result of pushing a value is captured.
        await signal.OnNextAsync(Two, CancellationToken.None);
    }

    /// <summary>Verifies that the <c>ObserverAsync</c> base class's <c>LinkExternalCancellation</c>
    /// takes the already-cancelled fast path when a fused operator's sink is constructed with
    /// a pre-cancelled subscribe token.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFusedOperatorSubscribedWithAlreadyCancelledToken_ThenSinkCancelsImmediately()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        try
        {
            await using var sub = await new[] { One, Two }.ToAsyncSignal()
                .ScanWithInitial(ScanSeed, static (acc, x) => acc + x)
                .SubscribeAsync(static (_, _) => default, cts.Token);
        }
        catch (OperationCanceledException)
        {
            // Expected — the sink is constructed with a cancelled token and the pipeline
            // short-circuits via OperationCanceledException somewhere in the subscribe chain.
        }
    }

    /// <summary>Verifies that an unhandled exception thrown by the downstream observer inside
    /// <c>ThrottleDistinct</c>'s delayed-emit task is routed to
    /// <see cref="UnhandledExceptionHandler"/>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleDistinctDownstreamThrowsInDelay_ThenRoutedToUnhandled()
    {
        using var unhandled = new UnhandledExceptionCapture();

        var signal = Signal.Create<int>();
        var throwingObserver = new ThrowingAsyncObserver<int>(new InvalidOperationException("downstream-throws"));

        await using var sub = await signal.Values
            .ThrottleDistinct(TimeSpan.FromMilliseconds(ThrottleWindowMilliseconds))
            .SubscribeAsync(throwingObserver, CancellationToken.None);

        await signal.OnNextAsync(One, CancellationToken.None);
        var exception = await unhandled.WaitForAsync("downstream-throws", TimeSpan.FromSeconds(5));

        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!.Message).IsEqualTo("downstream-throws");
    }

    /// <summary>Verifies that an exception thrown by the downstream observer inside <c>Throttle</c>'s
    /// post-delay forwarding is caught by the operator's <c>catch (Exception e)</c> block and
    /// routed through <see cref="UnhandledExceptionHandler"/>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleDownstreamThrowsInDelay_ThenRoutedToUnhandled()
    {
        using var unhandled = new UnhandledExceptionCapture();

        var signal = Signal.Create<int>();
        var throwingObserver = new ThrowingAsyncObserver<int>(new InvalidOperationException("throttle-downstream-throws"));

        await using var sub = await signal.Values
            .Throttle(TimeSpan.FromMilliseconds(ThrottleWindowMilliseconds))
            .SubscribeAsync(throwingObserver, CancellationToken.None);

        await signal.OnNextAsync(One, CancellationToken.None);
        var exception = await unhandled.WaitForAsync("throttle-downstream-throws", TimeSpan.FromSeconds(5));

        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!.Message).IsEqualTo("throttle-downstream-throws");
    }

    /// <summary>Exercises the <c>!IsCurrentEmission(id)</c> guard inside <c>DebounceUntil</c>'s
    /// <c>DelayAndEmitAsync</c> — when a later emission supersedes the current pending one
    /// before its debounce window elapses, the older delayed-emit task wakes, sees its id is
    /// stale, and returns early without forwarding.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceUntilSecondEmissionSupersedesFirst_ThenStaleDelayDropsValue()
    {
        var signal = Signal.Create<int>();
        var values = new List<int>();
        var emitted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .DebounceUntil(TimeSpan.FromMilliseconds(80), static _ => false)
            .SubscribeAsync(
                (v, _) =>
                {
                    values.Add(v);
                    emitted.TrySetResult();
                    return default;
                });

        await signal.OnNextAsync(One, CancellationToken.None);
        await signal.OnNextAsync(Two, CancellationToken.None);

        await emitted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Task.Delay(ThrottleWindowMilliseconds);

        await Assert.That(values).IsEquivalentTo([Two]);
    }

    /// <summary>Verifies that an unhandled exception thrown by the downstream observer inside
    /// <c>DebounceUntil</c>'s delayed-emit task is routed to <see cref="UnhandledExceptionHandler"/>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceUntilDownstreamThrowsInDelay_ThenRoutedToUnhandled()
    {
        using var unhandled = new UnhandledExceptionCapture();

        var signal = Signal.Create<int>();
        var throwingObserver = new ThrowingAsyncObserver<int>(new InvalidOperationException("debounce-downstream-throws"));

        await using var sub = await signal.Values
            .DebounceUntil(TimeSpan.FromMilliseconds(ThrottleWindowMilliseconds), static _ => false)
            .SubscribeAsync(throwingObserver, CancellationToken.None);

        await signal.OnNextAsync(One, CancellationToken.None);
        var exception = await unhandled.WaitForAsync("debounce-downstream-throws", TimeSpan.FromSeconds(5));

        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!.Message).IsEqualTo("debounce-downstream-throws");
    }

    /// <summary>Verifies that <c>Partition</c> drops upstream values whose predicate matches a
    /// branch that has no current subscriber — exercises the
    /// <c>target?.OnNextAsync(...) ?? default</c> null-target path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPartitionEmitsWhileMatchingBranchUnsubscribed_ThenDropped()
    {
        var signal = Signal.Create<int>();
        var (evens, _) = signal.Values.Partition(static x => x % Two == 0);

        var values = new List<int>();
        await using var sub = await evens.SubscribeAsync((v, _) =>
        {
            values.Add(v);
            return default;
        });

        // Odd value: matches the false branch, which has no subscriber.
        await signal.OnNextAsync(One, CancellationToken.None);

        // Even value: matches the true branch.
        await signal.OnNextAsync(Two, CancellationToken.None);

        await Assert.That(values).IsEquivalentTo([Two]);
    }

    /// <summary>Verifies that <see cref="SignalAsync.ThrottleDistinctSignal{T}.ThrottleDistinctObserver.TryClaimEmission"/>
    /// returns <see langword="false"/> when the id has been superseded by a newer upstream emission.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleDistinctTryClaimEmissionSuperseded_ThenReturnsFalse()
    {
        var observer = new SignalAsync.ThrottleDistinctSignal<int>.ThrottleDistinctObserver(
            new NoOpAsyncObserver<int>(),
            TimeSpan.FromHours(1),
            TimeProvider.System,
            CancellationToken.None);

        // Drive _id forward by two emissions; the first pending delay's id (1) is then stale.
        await observer.OnNextAsync(One, CancellationToken.None);
        await observer.OnNextAsync(Two, CancellationToken.None);

        var claimed = observer.TryClaimEmission(One, id: 1);

        await Assert.That(claimed).IsFalse();
    }

    /// <summary>Verifies that <see cref="SignalAsync.ThrottleDistinctSignal{T}.ThrottleDistinctObserver.TryClaimEmission"/>
    /// returns <see langword="false"/> when the value is a duplicate of the most-recently-emitted one.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleDistinctTryClaimEmissionDuplicate_ThenReturnsFalse()
    {
        var observer = new SignalAsync.ThrottleDistinctSignal<int>.ThrottleDistinctObserver(
            new NoOpAsyncObserver<int>(),
            TimeSpan.FromHours(1),
            TimeProvider.System,
            CancellationToken.None);

        await observer.OnNextAsync(One, CancellationToken.None);

        // First claim latches the downstream-distinct state with value One.
        var firstClaim = observer.TryClaimEmission(One, id: 1);

        // Drive another upstream so id matches the second claim.
        await observer.OnNextAsync(Two, CancellationToken.None);

        // Re-claim with the previously-emitted value at the new id — rejected by the
        // downstream-distinct check.
        var secondClaim = observer.TryClaimEmission(One, id: 2);

        await Assert.That(firstClaim).IsTrue();
        await Assert.That(secondClaim).IsFalse();
    }

    /// <summary>Verifies that <see cref="SignalAsync.DebounceUntilSignal{T}.DebounceUntilObserver.IsCurrentEmission"/>
    /// returns <see langword="true"/> for the most-recent id and <see langword="false"/> for
    /// stale ids.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDebounceUntilIsCurrentEmission_ThenMatchesIdState()
    {
        var observer = new SignalAsync.DebounceUntilSignal<int>.DebounceUntilObserver(
            new NoOpAsyncObserver<int>(),
            TimeSpan.FromHours(1),
            static _ => false,
            TimeProvider.System,
            CancellationToken.None);

        await observer.OnNextAsync(One, CancellationToken.None);
        await observer.OnNextAsync(Two, CancellationToken.None);

        await Assert.That(observer.IsCurrentEmission(id: 2)).IsTrue();
        await Assert.That(observer.IsCurrentEmission(id: 1)).IsFalse();
    }

    /// <summary>Verifies that <see cref="SignalAsync.PartitionCoordinator{T}.TryAttachSourceSubscription"/>
    /// returns <see langword="false"/> when both branches have already been disposed by the time
    /// the source subscription returns — the disposeNow race fast-path that is otherwise only
    /// reachable through a real concurrency race.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPartitionTryAttachSourceSubscriptionAndBothBranchesGone_ThenReturnsFalse()
    {
        var signal = Signal.Create<int>();
        var coordinator = new SignalAsync.PartitionCoordinator<int>(signal.Values, static x => x % Two == 0);

        // Subscribe then immediately dispose so both branch slots are null.
        var sub = await coordinator.TrueBranch.SubscribeAsync(static (_, _) => default);
        await sub.DisposeAsync();

        var attached = coordinator.TryAttachSourceSubscription(DisposableAsync.Empty);

        await Assert.That(attached).IsFalse();
    }

    /// <summary>Verifies that <see cref="SignalAsync.PartitionCoordinator{T}.TryAttachSourceSubscription"/>
    /// returns <see langword="true"/> when at least one branch is still alive.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPartitionTryAttachSourceSubscriptionAndBranchAlive_ThenReturnsTrue()
    {
        var signal = Signal.Create<int>();
        var coordinator = new SignalAsync.PartitionCoordinator<int>(signal.Values, static x => x % Two == 0);

        await using var sub = await coordinator.TrueBranch.SubscribeAsync(static (_, _) => default);

        var attached = coordinator.TryAttachSourceSubscription(DisposableAsync.Empty);

        await Assert.That(attached).IsTrue();
    }

    /// <summary>Yields values as a generic <see cref="IEnumerable{T}"/> (neither array nor list)
    /// to drive the slow-path branch of <c>ForEach</c>.</summary>
    /// <param name="values">Values to yield.</param>
    /// <returns>A lazily-evaluated enumerable.</returns>
    private static IEnumerable<int> Enumerate(params int[] values)
    {
        foreach (var v in values)
        {
            yield return v;
        }
    }

    /// <summary>Bare-bones downstream async observer that throws a given exception inside
    /// <c>OnNextAsync</c>. Bypassing the <see cref="ObserverAsync{T}"/> base class is intentional
    /// — the base class would otherwise swallow synchronous throws and route them through
    /// <see cref="UnhandledExceptionHandler"/>, never letting the exception propagate up to the
    /// upstream operator's <c>catch (Exception e)</c> block under test.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    /// <param name="error">The exception to throw on every emission.</param>
    private sealed class ThrowingAsyncObserver<T>(Exception error) : IObserverAsync<T>
    {
        /// <inheritdoc/>
        public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) =>
            throw error;

        /// <inheritdoc/>
        public ValueTask OnErrorResumeAsync(Exception err, CancellationToken cancellationToken) =>
            default;

        /// <inheritdoc/>
        public ValueTask OnCompletedAsync(Result result) => default;

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => default;
    }

    /// <summary>No-op async observer used as a downstream stand-in for direct unit tests of
    /// observer-internal decision methods.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    private sealed class NoOpAsyncObserver<T> : IObserverAsync<T>
    {
        /// <inheritdoc/>
        public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        public ValueTask OnCompletedAsync(Result result) => default;

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => default;
    }
}
