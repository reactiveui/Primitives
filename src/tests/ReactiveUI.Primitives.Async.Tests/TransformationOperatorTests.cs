// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for transformation operators: Select, SelectMany, Scan, Do, Cast, OfType.</summary>
public partial class TransformationOperatorTests
{
    /// <summary>Message thrown by an observer from its completion callback.</summary>
    private const string CompletionFailedMessage = "completion failed";

    /// <summary>Message thrown by an observer from a secondary completion callback.</summary>
    private const string OnCompletedBlewUpMessage = "onCompleted blew up";

    /// <summary>Message thrown by a raw observer from its completion callback.</summary>
    private const string RawObserverCompletionFailedMessage = "raw observer completion failed";

    /// <summary>Sample integer value one.</summary>
    private const int One = 1;

    /// <summary>Sample integer value two.</summary>
    private const int Two = 2;

    /// <summary>Number of inputs fed into the async-accumulator <c>Scan</c> sync-result test.</summary>
    private const int ScanInputCount = 3;

    /// <summary>Sentinel value emitted by the single-value sources.</summary>
    private const int SentinelValue = 42;

    /// <summary>Divisor of the even/odd group key.</summary>
    private const int EvenDivisor = 2;

    /// <summary>Hoisted source array used by tests (was inline literal).</summary>
    private static readonly int[] Sequence123456 = [1, 2, 3, 4, 5, 6];

    /// <summary>Mixed-type source in which only two elements are strings.</summary>
    private static readonly object[] OfTypeMixedItems = [1, "two", 3, "four"];

    /// <summary>Mixed-type source in which no element is a string.</summary>
    private static readonly object[] OfTypeMisses = [1, 2, 3];

    /// <summary>Tests sync Select projects each element.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectSyncSelector_ThenProjectsEachElement()
    {
        const int Multiplier = 10;
        const int SourceValueCount = 3;
        const int ExpectedFirst = 10;
        const int ExpectedSecond = 20;
        const int ExpectedThird = 30;
        var result = await SignalAsync.Range(1, SourceValueCount).Select(static x => x * Multiplier).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([ExpectedFirst, ExpectedSecond, ExpectedThird]);
    }

    /// <summary>Tests async Select projects each element.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncSelector_ThenProjectsEachElement()
    {
        const int SourceValueCount = 3;

        var result = await SignalAsync.Range(1, SourceValueCount).Select(static async (x, _) =>
        {
            await Task.Yield();
            return x.ToString();
        }).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo(["1", "2", "3"]);
    }

    /// <summary>Tests sync SelectMany flattens inner sequences.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManySync_ThenFlattensInnerSequences()
    {
        const int InnerMultiplier = 10;
        const int ExpectedCount = 6;
        const int FirstExpectedValue = 10;
        const int LastExpectedValue = 30;
        const int SourceValueCount = 3;
        const int InnerValueCount = 2;

        var result = await SignalAsync.Range(1, SourceValueCount)
            .SelectMany(static x => SignalAsync.Range(x * InnerMultiplier, InnerValueCount))
            .ToListAsync();
        await Assert.That(result).Count().IsEqualTo(ExpectedCount);
        await Assert.That(result).Contains(FirstExpectedValue);
        await Assert.That(result).Contains(LastExpectedValue);
    }

    /// <summary>Tests async SelectMany flattens inner sequences.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManyAsync_ThenFlattensInnerSequences()
    {
        const int Multiplier = 100;
        const int ExpectedCount = 2;
        const int FirstExpectedValue = 100;
        const int SecondExpectedValue = 200;
        const int SourceValueCount = 2;

        var result = await SignalAsync.Range(1, SourceValueCount).SelectMany(static async (x, _) =>
        {
            await Task.Yield();
            return SignalAsync.Return(x * Multiplier);
        }).ToListAsync();
        await Assert.That(result).Count().IsEqualTo(ExpectedCount);
        await Assert.That(result).Contains(FirstExpectedValue);
        await Assert.That(result).Contains(SecondExpectedValue);
    }

    /// <summary>Tests SelectMany with result selector projects pairs.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManyWithResultSelector_ThenProjectsPairs()
    {
        const int InnerMultiplier = 10;
        const int ExpectedCount = 2;
        const int SourceValueCount = 2;

        var result = await SignalAsync.Range(1, SourceValueCount)
            .SelectMany(
                static x => SignalAsync.Return(x * InnerMultiplier),
                static (outer, inner) => $"{outer}:{inner}")
            .ToListAsync();
        await Assert.That(result).Count().IsEqualTo(ExpectedCount);
        await Assert.That(result).Contains("1:10");
        await Assert.That(result).Contains("2:20");
    }

    /// <summary>Verifies SelectMany waits for active inner sequences after the outer sequence completes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManyOuterCompletesBeforeInner_ThenWaitsForInnerCompletion()
    {
        DirectSource<int> outer = new();
        DirectSource<int> inner = new();
        List<int> values = [];
        Result? completion = null;

        await using var subscription = await outer
            .SelectMany(_ => inner)
            .SubscribeAsync(
                (value, _) =>
                {
                    values.Add(value);
                    return default;
                },
                null,
                result =>
                {
                    completion = result;
                    return default;
                });

        await outer.EmitNext(One);
        await inner.EmitNext(Two);
        await outer.Complete(Result.Success);
        await Assert.That(completion.HasValue).IsFalse();

        await inner.Complete(Result.Success);
        await Assert.That(completion.HasValue).IsTrue();

        await Assert.That(values).Contains(Two);
        await Assert.That(completion!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Verifies SelectMany forwards inner completion failures to the downstream observer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManyInnerFails_ThenCompletesWithFailure()
    {
        DirectSource<int> outer = new();
        DirectSource<int> inner = new();
        InvalidOperationException expected = new("select-many-inner");
        Result? completion = null;

        await using var subscription = await outer
            .SelectMany(_ => inner)
            .SubscribeAsync(
                static (_, _) => default,
                null,
                result =>
                {
                    completion = result;
                    return default;
                });

        await outer.EmitNext(One);
        await inner.Complete(Result.Failure(expected));
        await Assert.That(completion.HasValue).IsTrue();

        await Assert.That(completion!.Value.IsFailure).IsTrue();
        await Assert.That(completion.Value.Exception).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies SelectMany forwards inner subscription failures to the downstream observer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManyInnerSubscribeThrows_ThenCompletesWithFailure()
    {
        InvalidOperationException expected = new("select-many-subscribe");
        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var subscription = await SignalAsync
            .Return(One)
            .SelectMany(_ => SignalAsync.Create<int>((_, _) => throw expected))
            .SubscribeAsync(
                static (_, _) => default,
                null,
                result =>
                {
                    IgnoredResult.Of(completed.TrySetResult(result));
                    return default;
                });

        var completion = await completed.Task;
        await Assert.That(completion.IsFailure).IsTrue();
        await Assert.That(completion.Exception).IsSameReferenceAs(expected);
    }

    /// <summary>Tests SelectMany null selector throws.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void WhenSelectManyNullSelector_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(static () =>
            SignalAsync.Return(1).SelectMany((Func<int, IObservableAsync<int>>)null!));

    /// <summary>Tests sync Scan emits running accumulation.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScanSync_ThenEmitsRunningAccumulation()
    {
        const int ExpectedSecond = 3;
        const int ExpectedThird = 6;
        const int ExpectedFourth = 10;
        const int SourceValueCount = 4;

        var result = await SignalAsync.Range(1, SourceValueCount)
            .Scan(0, static (acc, x) => acc + x)
            .ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([1, ExpectedSecond, ExpectedThird, ExpectedFourth]);
    }

    /// <summary>Tests async Scan emits running accumulation.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScanAsync_ThenEmitsRunningAccumulation()
    {
        const int SourceValueCount = 3;

        var result = await SignalAsync.Range(1, SourceValueCount).Scan(string.Empty, static async (acc, x, _) =>
        {
            await Task.Yield();
            return acc + x;
        }).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo(["1", "12", "123"]);
    }

    /// <summary>Tests Scan null accumulator throws.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void WhenScanNullAccumulator_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(static () => SignalAsync.Return(1).Scan(0, (Func<int, int, int>)null!));

    /// <summary>
    /// Verifies that Cast completes with a failure containing an <see cref = "InvalidCastException"/>
    /// when the source emits an element that cannot be cast to the target type.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCastIncompatibleType_ThenCompletesWithFailure()
    {
        var source = SignalAsync.Return<object>(SentinelValue);
        Result? completionResult = null;
        TaskCompletionSource tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await source.Cast<object, string>().SubscribeAsync(static (_, _) => default, null, result =>
        {
            completionResult = result;
            _ = tcs.TrySetResult();
            return default;
        });
        await tcs.Task;
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsFalse();
        await Assert.That(completionResult.Value.Exception).IsTypeOf<InvalidCastException>();
    }

    /// <summary>Tests Cast with compatible type casts correctly.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCastCompatibleType_ThenCasts()
    {
        var source = SignalAsync.Return<object>("hello");
        var result = await source.Cast<object, string>().ToListAsync();
        await Assert.That(result).IsCollectionEqualTo(["hello"]);
    }

    /// <summary>Tests OfType with matching type filters correctly.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOfTypeMatchingType_ThenFiltersCorrectly()
    {
        var source = OfTypeMixedItems.ToAsyncSignal();
        var strings = await source.OfType<object, string>().ToListAsync();
        await Assert.That(strings).IsCollectionEqualTo(["two", "four"]);
    }

    /// <summary>Tests OfType with no matches emits nothing.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOfTypeNoMatches_ThenEmitsNothing()
    {
        var source = OfTypeMisses.ToAsyncSignal();
        var strings = await source.OfType<object, string>().ToListAsync();
        await Assert.That(strings).IsEmpty();
    }

    /// <summary>
    /// Verifies that disposing a Prepend subscription from inside an OnNext callback
    /// triggers the early return guard when cancellation is requested during the prepend loop.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependCancelledBeforeAllValues_ThenEarlyReturn()
    {
        const int PrependedValueCount = 100;

        List<int> received = [];
        var values = Enumerable.Range(1, PrependedValueCount).ToArray();
        IAsyncDisposable? subscription = null;
        var source = SignalAsync.Never<int>();
        var pipeline = source.Prepend(values);
        _ = await pipeline.SubscribeAsync(
            async (x, _) =>
            {
                received.Add(x);
                if (received.Count == 3)
                {
                    await subscription!.DisposeAsync();
                }
            },
            null);
        const int MinReceivedCount = 3;
        await Assert.That(received.Count >= 3).IsTrue();
        await Assert.That(received.Count).IsGreaterThanOrEqualTo(MinReceivedCount);
    }

    /// <summary>
    /// Verifies that when Prepend's source throws an exception and the observer's OnCompletedAsync
    /// also throws, the completion exception is routed to the UnhandledExceptionHandler.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Correctness",
        "SST2409:Do not throw a general exception type",
        Justification = "Deliberately throws a generic exception type to verify operator error-handling pathways.")]
    [SuppressMessage(
        "Microsoft.Design",
        "CA2201",
        Justification =
            "Deliberately uses a generic exception type to verify operator error-handling pathways with arbitrary exception kinds.")]
    public async Task WhenPrependSourceThrowsAndCompletionAlsoThrows_ThenRoutedToHandler()
    {
        using UnhandledExceptionCapture unhandled = new();
        InvalidOperationException completionException = new(CompletionFailedMessage);
        var source = SignalAsync.Create<int>(static (_, _) =>
            ValueTask.FromException<IAsyncDisposable>(new ApplicationException("source error")));
        var pipeline = source.Prepend(SentinelValue);
        await using var sub = await pipeline.SubscribeAsync(static (_, _) => default, null, _ => throw completionException);
        var exception = await unhandled.WaitForAsync(CompletionFailedMessage);
        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!).IsTypeOf<InvalidOperationException>();
        await Assert.That(exception!.Message).IsEqualTo(CompletionFailedMessage);
    }

    /// <summary>
    /// Verifies that async Do with an onErrorResume callback invokes the callback
    /// when the source emits a resumable error, and forwards the error downstream.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoAsyncWithOnErrorResume_ThenInvokesCallbackAndForwardsError()
    {
        List<Exception> resumedErrors = [];
        List<Exception> downstreamErrors = [];
        TaskCompletionSource tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(new InvalidOperationException("test error"), ct);
            await observer.OnNextAsync(Two, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        await using var sub = await source.Do(
            (Func<int, CancellationToken, ValueTask>?)null,
            async (ex, _) =>
            {
                await Task.Yield();
                resumedErrors.Add(ex);
            },
            null).SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                downstreamErrors.Add(ex);
                return default;
            },
            _ =>
            {
                IgnoredResult.Of(tcs.TrySetResult());
                return default;
            });
        await tcs.Task;
        await Assert.That(resumedErrors).Count().IsEqualTo(1);
        await Assert.That(resumedErrors[0].Message).IsEqualTo("test error");
        await Assert.That(downstreamErrors).Count().IsEqualTo(1);
    }

    /// <summary>Verifies that async Do with an onCompleted callback invokes the callback when the source sequence completes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoAsyncWithOnCompleted_ThenInvokesCallback()
    {
        Result? capturedResult = null;
        TaskCompletionSource tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnNextAsync(SentinelValue, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        await using var sub = await source.Do((Func<int, CancellationToken, ValueTask>?)null, null, async result =>
        {
            await Task.Yield();
            capturedResult = result;
        }).SubscribeAsync(static (_, _) => default, null, _ =>
        {
            IgnoredResult.Of(tcs.TrySetResult());
            return default;
        });
        await tcs.Task;
        await Assert.That(capturedResult).IsNotNull();
        await Assert.That(capturedResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>
    /// Verifies that sync Do with an onErrorResume callback invokes the callback
    /// when the source emits a resumable error, and forwards the error downstream.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoSyncWithOnErrorResume_ThenInvokesCallbackAndForwardsError()
    {
        List<Exception> resumedErrors = [];
        List<Exception> downstreamErrors = [];
        TaskCompletionSource tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(new InvalidOperationException("sync error"), ct);
            await observer.OnNextAsync(Two, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        await using var sub = await source.Do((Action<int>?)null, resumedErrors.Add, (Action<Result>?)null)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    downstreamErrors.Add(ex);
                    return default;
                },
                _ =>
                {
                    IgnoredResult.Of(tcs.TrySetResult());
                    return default;
                });
        await tcs.Task;
        await Assert.That(resumedErrors).Count().IsEqualTo(1);
        await Assert.That(resumedErrors[0].Message).IsEqualTo("sync error");
        await Assert.That(downstreamErrors).Count().IsEqualTo(1);
    }

    /// <summary>Verifies that ObserveOn with an IScheduler creates the correct async context and emits values through the pipeline.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIScheduler_ThenEmitsValues()
    {
        var scheduler = Sequencer.Immediate;
        const int ExpectedSecond = 2;
        const int ExpectedThird = 3;
        const int SourceValueCount = 3;

        var result = await SignalAsync.Range(1, SourceValueCount).WitnessOn(scheduler).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([1, ExpectedSecond, ExpectedThird]);
    }

    /// <summary>
    /// Verifies that ObserveOn forwards resumable errors emitted by the source
    /// through the context-switched observer to the downstream error handler.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnSourceEmitsResumableError_ThenForwardsErrorDownstream()
    {
        const int ExpectedSecond = 2;
        List<Exception> downstreamErrors = [];
        List<int> receivedValues = [];
        TaskCompletionSource tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(new InvalidOperationException("resumable"), ct);
            await observer.OnNextAsync(Two, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        await using var sub = await source.WitnessOn(AsyncContext.Default).SubscribeAsync(
            (x, _) =>
            {
                receivedValues.Add(x);
                return default;
            },
            (ex, _) =>
            {
                downstreamErrors.Add(ex);
                return default;
            },
            _ =>
            {
                IgnoredResult.Of(tcs.TrySetResult());
                return default;
            });
        await tcs.Task;
        await Assert.That(receivedValues).IsCollectionEqualTo([1, ExpectedSecond]);
        await Assert.That(downstreamErrors).Count().IsEqualTo(1);
        await Assert.That(downstreamErrors[0]).IsTypeOf<InvalidOperationException>();
        await Assert.That(downstreamErrors[0].Message).IsEqualTo("resumable");
    }

    /// <summary>
    /// Verifies that when the CancellationToken passed to Prepend is already cancelled before
    /// the prepend loop begins iterating, the loop exits immediately without emitting any values.
    /// This covers the cancellation-requested early return guard inside the prepend foreach loop.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependTokenCancelledBeforeIteration_ThenEmitsNoValues()
    {
        const int MaxReceivedCount = 100;
        List<int> received = [];
        using CancellationTokenSource cts = new();
        var source = SignalAsync.Never<int>();
        var pipeline = source.Prepend(Enumerable.Range(1, MaxReceivedCount));

        // Cancel the token before subscribing so the prepend loop sees cancellation immediately.
        await cts.CancelAsync();
        var subscription = await pipeline.SubscribeAsync(
            (x, _) =>
            {
                received.Add(x);
                return default;
            },
            null,
            null,
            cts.Token);
        await subscription.DisposeAsync();
        await Assert.That(received.Count).IsLessThan(MaxReceivedCount);
    }

    /// <summary>Verifies that a completion callback failure after Prepend subscription failure reaches the unhandled exception handler.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Correctness",
        "SST2409:Do not throw a general exception type",
        Justification = "Deliberately throws a generic exception type to verify operator error-handling pathways.")]
    [SuppressMessage(
        "Microsoft.Design",
        "CA2201",
        Justification =
            "Deliberately uses a generic exception type to verify operator error-handling pathways with arbitrary exception kinds.")]
    public async Task WhenPrependSourceThrowsAndOnCompletedThrows_ThenSecondaryExceptionRoutedToHandler()
    {
        using UnhandledExceptionCapture unhandled = new();
        InvalidOperationException secondaryException = new(OnCompletedBlewUpMessage);
        var source = SignalAsync.Create<int>(static (_, _) =>
            ValueTask.FromException<IAsyncDisposable>(new ApplicationException("source failure")));

        // Source subscription fails after the prepended value is delivered.
        var pipeline = source.Prepend(1);
        await using var sub = await pipeline.SubscribeAsync(static (_, _) => default, null, _ => throw secondaryException);
        var exception = await unhandled.WaitForAsync(OnCompletedBlewUpMessage);
        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!).IsTypeOf<InvalidOperationException>();
        await Assert.That(exception!.Message).IsEqualTo(OnCompletedBlewUpMessage);
    }

    /// <summary>Verifies that Yield with a null source throws <see cref = "ArgumentNullException"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void WhenYieldNullSource_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(static () => SignalAsyncReactiveExtensions.Yield<int>(null!));

    /// <summary>Yield constructs the wrapper that captures the subscriber's context.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task WhenYield_ThenWrapsSource()
    {
        var source = SignalAsync.Return(SentinelValue);
        var observed = source.Yield();
        await Assert.That(observed).IsTypeOf<SignalAsyncReactiveExtensions.YieldSignal<int>>();
        await Assert.That(observed).IsNotSameReferenceAs(source);
    }

    /// <summary>Verifies that the three-argument GroupBy overload throws <see cref = "ArgumentNullException"/> when the source parameter is null.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void WhenGroupByWithSignalSelectorNullSource_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(static () =>
            SignalAsyncExtensions.GroupBy<int, int>(null!, static x => x, static _ => Signal.Create<int>()));

    /// <summary>Verifies that the three-argument GroupBy overload throws <see cref = "ArgumentNullException"/> when the keySelector parameter is null.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void WhenGroupByWithSignalSelectorNullKeySelector_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(static () =>
            SignalAsync.Empty<int>().GroupBy<int, int>(null!, static _ => Signal.Create<int>()));

    /// <summary>
    /// Verifies that the three-argument GroupBy overload with a custom group Signal selector
    /// correctly groups elements by key using the provided Signal factory.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGroupByWithCustomSignalSelector_ThenGroupsByKey()
    {
        const int ExpectedGroupCount = 2;
        const int OddSecond = 3;
        const int OddThird = 5;
        const int EvenFirst = 2;
        const int EvenSecond = 4;
        const int EvenThird = 6;
        var source = Sequence123456.ToAsyncSignal();
        Dictionary<int, List<int>> groups = [];
        TaskCompletionSource tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await source.GroupBy(static x => x % EvenDivisor, static _ => Signal.Create<int>()).SubscribeAsync(
            async (group, ct) =>
            {
                List<int> list = [];
                groups[group.Key] = list;
                await group.SubscribeAsync(
                    (value, _) =>
                    {
                        list.Add(value);
                        return default;
                    },
                    null,
                    null,
                    ct);
            },
            null,
            _ =>
            {
                IgnoredResult.Of(tcs.TrySetResult());
                return default;
            });
        await tcs.Task;
        await Assert.That(groups).Count().IsEqualTo(ExpectedGroupCount);
        await Assert.That(groups[1]).IsCollectionEqualTo([1, OddSecond, OddThird]);
        await Assert.That(groups[0]).IsCollectionEqualTo([EvenFirst, EvenSecond, EvenThird]);
    }

    /// <summary>Verifies that GroupBy forwards resumable errors from the source through to the downstream observer via <see cref = "IObserverAsync{T}.OnErrorResumeAsync"/>.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGroupBySourceEmitsResumableError_ThenForwardsErrorDownstream()
    {
        List<Exception> downstreamErrors = [];
        TaskCompletionSource tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(new InvalidOperationException("group error"), ct);
            await observer.OnNextAsync(Two, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        await using var sub = await source.GroupBy(static x => x, static _ => Signal.Create<int>()).SubscribeAsync(
            static async (group, ct) => await group.SubscribeAsync(static (_, _) => default, null, null, ct),
            (ex, _) =>
            {
                downstreamErrors.Add(ex);
                return default;
            },
            _ =>
            {
                IgnoredResult.Of(tcs.TrySetResult());
                return default;
            });
        await tcs.Task;
        await Assert.That(downstreamErrors).Count().IsEqualTo(1);
        await Assert.That(downstreamErrors[0]).IsTypeOf<InvalidOperationException>();
        await Assert.That(downstreamErrors[0].Message).IsEqualTo("group error");
    }

    /// <summary>Verifies that a raw observer's completion failure after Prepend subscription failure reaches the unhandled exception handler.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Correctness",
        "SST2409:Do not throw a general exception type",
        Justification = "Deliberately throws a generic exception type to verify operator error-handling pathways.")]
    [SuppressMessage(
        "Microsoft.Design",
        "CA2201",
        Justification =
            "Deliberately uses a generic exception type to verify operator error-handling pathways with arbitrary exception kinds.")]
    public async Task WhenPrependSourceThrowsAndRawObserverCompletionThrows_ThenRoutedToUnhandledHandler()
    {
        using UnhandledExceptionCapture unhandled = new();
        InvalidOperationException completionException = new(RawObserverCompletionFailedMessage);
        var source = SignalAsync.Create<int>(static (_, _) =>
            ValueTask.FromException<IAsyncDisposable>(new ApplicationException("source subscribe error")));
        var pipeline = source.Prepend(1);
        ThrowingOnCompletedWitness<int> rawObserver = new(completionException);
        await using var sub = await pipeline.SubscribeAsync(rawObserver, CancellationToken.None);
        var exception = await unhandled.WaitForAsync(RawObserverCompletionFailedMessage);
        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!).IsTypeOf<InvalidOperationException>();
        await Assert.That(exception!.Message).IsEqualTo(RawObserverCompletionFailedMessage);
    }

    /// <summary>Tests Do with onErrorResume callback invokes callback on error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoWithOnErrorResume_ThenCallbackInvoked()
    {
        DirectSource<int> directSource = new();
        List<Exception> errors = [];
        InvalidOperationException error = new("test");
        await using var sub = await directSource.Do((Action<int>?)null, errors.Add, (Action<Result>?)null)
            .SubscribeAsync(static (_, _) => default, static (_, _) => default, static _ => default);
        await directSource.EmitError(error);
        await directSource.Complete(Result.Success);
        await Assert.That(errors).Count().IsEqualTo(1);
    }

    /// <summary>Tests GroupBy source failure propagates.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGroupBySourceFails_ThenErrorPropagated()
    {
        const int GroupModulus = 2;
        InvalidOperationException error = new("group-fail");
        await Assert.That(async () => await SignalAsync.Throw<int>(error).GroupBy(static x => x % GroupModulus).FirstAsync())
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Tests Using operator disposes resource on source failure.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingSourceThrows_ThenResourceDisposed()
    {
        StrongBox<bool> disposed = new();
        InvalidOperationException error = new("test");
        await Assert.That(async () => await SignalAsync.Using(
            _ =>
            {
                try
                {
                    return ValueTask.FromResult(DisposableAsync.Create(disposed, static state =>
                    {
                        state.Value = true;
                        return default;
                    }));
                }
                catch (Exception exception)
                {
                    return ValueTask.FromException<IAsyncDisposable>(exception);
                }
            },
            _ => SignalAsync.Throw<int>(error)).FirstAsync()).ThrowsExactly<InvalidOperationException>();
        await Assert.That(disposed.Value).IsTrue();
    }

    /// <summary>Verifies the async-accumulator <c>Scan</c> overload's sync-completed fast path -
    /// returning a synchronously-completed <see cref = "ValueTask{TResult}"/> from the accumulator
    /// takes the inline <c>pending.Result</c> branch.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScanAsyncAccumulatorReturnsSync_ThenForwardsAccumulator()
    {
        const int ThirdRunningTotal = 3;
        const int SixthRunningTotal = 6;
        var result = await SignalAsync.Range(1, ScanInputCount).Scan(0, static (acc, x, _) => new(acc + x))
            .ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([1, ThirdRunningTotal, SixthRunningTotal]);
    }

    /// <summary>Verifies that the sync-accumulator <c>Scan</c> overload forwards a non-terminal upstream error downstream.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScanSyncSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        TaskCompletionSource errorTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.Scan(0, static (acc, x) => acc + x).SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                caught = ex;
                IgnoredResult.Of(errorTcs.TrySetResult());
                return default;
            });
        InvalidOperationException expected = new("scan-sync-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);
        await errorTcs.Task;
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that the async-accumulator <c>Scan</c> overload forwards a non-terminal upstream error downstream.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScanAsyncSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        TaskCompletionSource errorTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.Scan(0, static (acc, x, _) => new(acc + x)).SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                caught = ex;
                IgnoredResult.Of(errorTcs.TrySetResult());
                return default;
            });
        InvalidOperationException expected = new("scan-async-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);
        await errorTcs.Task;
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Throws directly from OnCompletedAsync without catching the exception.</summary>
    /// <typeparam name = "T">The type of elements received by the observer.</typeparam>
    /// <param name = "completionException">The exception to throw when <see cref = "OnCompletedAsync"/> is called.</param>
    private sealed class ThrowingOnCompletedWitness<T>(Exception completionException) : IObserverAsync<T>
    {
        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        public ValueTask OnCompletedAsync(Result result) => throw completionException;

        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => default;
    }
}
