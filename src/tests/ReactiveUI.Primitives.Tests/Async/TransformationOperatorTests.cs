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
using ReactiveUI.Primitives.Async.Subjects;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Tests for transformation operators: Select, SelectMany, Scan, Do, Cast, OfType.
/// </summary>
public class TransformationOperatorTests
{
    /// <summary>Number of inputs fed into the async-accumulator <c>Scan</c> sync-result test.</summary>
    private const int ScanInputCount = 3;

    /// <summary>Hoisted source array used by tests (was inline literal).</summary>
    private static readonly int[] Sequence123456 = [1, 2, 3, 4, 5, 6];

    /// <summary>Tests sync Select projects each element.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectSyncSelector_ThenProjectsEachElement()
    {
        const int Multiplier = 10;
        const int ExpectedFirst = 10;
        const int ExpectedSecond = 20;
        const int ExpectedThird = 30;

        var result = await ObservableAsync.Range(1, 3)
            .Select(x => x * Multiplier)
            .ToListAsync();

        await Assert.That(result).IsEquivalentTo([ExpectedFirst, ExpectedSecond, ExpectedThird]);
    }

    /// <summary>Tests async Select projects each element.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncSelector_ThenProjectsEachElement()
    {
        var result = await ObservableAsync.Range(1, 3)
            .Select(async (x, _) =>
            {
                await Task.Yield();
                return x.ToString();
            })
            .ToListAsync();

        await Assert.That(result).IsEquivalentTo(["1", "2", "3"]);
    }

    /// <summary>Tests sync SelectMany flattens inner sequences.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManySync_ThenFlattensInnerSequences()
    {
        const int InnerMultiplier = 10;
        const int ExpectedCount = 6;
        const int FirstExpectedValue = 10;
        const int LastExpectedValue = 30;

        var result = await ObservableAsync.Range(1, 3)
            .SelectMany(x => ObservableAsync.Range(x * InnerMultiplier, 2))
            .ToListAsync();

        await Assert.That(result).Count().IsEqualTo(ExpectedCount);
        await Assert.That(result).Contains(FirstExpectedValue);
        await Assert.That(result).Contains(LastExpectedValue);
    }

    /// <summary>Tests async SelectMany flattens inner sequences.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManyAsync_ThenFlattensInnerSequences()
    {
        const int Multiplier = 100;
        const int ExpectedCount = 2;
        const int FirstExpectedValue = 100;
        const int SecondExpectedValue = 200;

        var result = await ObservableAsync.Range(1, 2)
            .SelectMany(async (x, _) =>
            {
                await Task.Yield();
                return ObservableAsync.Return(x * Multiplier);
            })
            .ToListAsync();

        await Assert.That(result).Count().IsEqualTo(ExpectedCount);
        await Assert.That(result).Contains(FirstExpectedValue);
        await Assert.That(result).Contains(SecondExpectedValue);
    }

    /// <summary>Tests SelectMany with result selector projects pairs.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManyWithResultSelector_ThenProjectsPairs()
    {
        const int InnerMultiplier = 10;
        const int ExpectedCount = 2;

        var result = await ObservableAsync.Range(1, 2)
            .SelectMany(
                x => ObservableAsync.Return(x * InnerMultiplier),
                (outer, inner) => $"{outer}:{inner}")
            .ToListAsync();

        await Assert.That(result).Count().IsEqualTo(ExpectedCount);
        await Assert.That(result).Contains("1:10");
        await Assert.That(result).Contains("2:20");
    }

    /// <summary>Tests SelectMany null selector throws.</summary>
    [Test]
    public void WhenSelectManyNullSelector_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(() =>
            ObservableAsync.Return(1).SelectMany((Func<int, ObservableAsync<int>>)null!));

    /// <summary>Tests sync Scan emits running accumulation.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScanSync_ThenEmitsRunningAccumulation()
    {
        const int ExpectedSecond = 3;
        const int ExpectedThird = 6;
        const int ExpectedFourth = 10;

        var result = await ObservableAsync.Range(1, 4)
            .Scan(0, (acc, x) => acc + x)
            .ToListAsync();

        await Assert.That(result).IsEquivalentTo([1, ExpectedSecond, ExpectedThird, ExpectedFourth]);
    }

    /// <summary>Tests async Scan emits running accumulation.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScanAsync_ThenEmitsRunningAccumulation()
    {
        var result = await ObservableAsync.Range(1, 3)
            .Scan(string.Empty, async (acc, x, _) =>
            {
                await Task.Yield();
                return acc + x;
            })
            .ToListAsync();

        await Assert.That(result).IsEquivalentTo(["1", "12", "123"]);
    }

    /// <summary>Tests Scan null accumulator throws.</summary>
    [Test]
    public void WhenScanNullAccumulator_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(() =>
            ObservableAsync.Return(1).Scan(0, (Func<int, int, int>)null!));

    /// <summary>Exercises the sync-action <c>Do&lt;T&gt;(Action&lt;T&gt;, Action&lt;Exception&gt;, Action&lt;Result&gt;)</c>
    /// overload's non-null-callback branches in <c>DoSyncObserver</c>'s OnNext / OnErrorResume / OnCompleted.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoSyncWithAllCallbacks_ThenInvokesAndForwards()
    {
        const int ExpectedFirst = 7;
        const int ExpectedSecond = 8;
        var nextValues = new List<int>();
        var errors = new List<Exception>();
        var completions = new List<Result>();
        var errored = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var source = ObservableAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(ExpectedFirst, ct);
            await observer.OnErrorResumeAsync(new InvalidOperationException("resume"), ct);
            await observer.OnNextAsync(ExpectedSecond, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        await using var sub = await source
            .Do(
                nextValues.Add,
                exception =>
                {
                    errors.Add(exception);
                    errored.TrySetResult();
                },
                result =>
                {
                    completions.Add(result);
                    completed.TrySetResult();
                })
            .SubscribeAsync(static (_, _) => default);

        await Task.WhenAll(errored.Task, completed.Task).WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(nextValues).IsEquivalentTo([ExpectedFirst, ExpectedSecond]);
        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(completions).Count().IsEqualTo(1);
    }

    /// <summary>Exercises the no-arg <c>Do&lt;T&gt;()</c> overload's null-callback branches on
    /// the resumable-error and completion paths — pushes an <c>OnErrorResumeAsync</c> followed
    /// by a successful completion through <c>Do()</c> with all callbacks null, hitting the
    /// <c>onErrorResume?.Invoke</c> null arm and the <c>onCompleted?.Invoke</c> null arm.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoWithNoCallbacksAndSourceEmitsErrorResume_ThenForwardsBoth()
    {
        Exception? caught = null;
        var completed = false;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completionTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var source = ObservableAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException("resume"), ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        await using var sub = await source
            .Do()
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                },
                _ =>
                {
                    completed = true;
                    completionTcs.TrySetResult();
                    return default;
                });

        await Task.WhenAll(errorTcs.Task, completionTcs.Task).WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(caught).IsNotNull();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Exercises the no-arg <c>Do&lt;T&gt;()</c> overload — a pure pass-through that
    /// constructs a <c>DoSyncObservable</c> with all callbacks set to null.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoWithNoCallbacks_ThenPassesThroughValues()
    {
        const int ExpectedSecond = 2;
        const int ExpectedThird = 3;

        var result = await ObservableAsync.Range(1, 3).Do().ToListAsync();

        await Assert.That(result).IsEquivalentTo([1, ExpectedSecond, ExpectedThird]);
    }

    /// <summary>Tests sync Do invokes side effects.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoSync_ThenInvokesSideEffects()
    {
        const int ExpectedSecond = 2;
        const int ExpectedThird = 3;

        var sideEffects = new List<int>();

        var result = await ObservableAsync.Range(1, 3)
            .Do((x, _) =>
            {
                sideEffects.Add(x);
                return default;
            })
            .ToListAsync();

        await Assert.That(result).IsEquivalentTo([1, ExpectedSecond, ExpectedThird]);
        await Assert.That(sideEffects).IsEquivalentTo([1, ExpectedSecond, ExpectedThird]);
    }

    /// <summary>Tests async Do invokes side effects.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoAsync_ThenInvokesSideEffects()
    {
        const int ExpectedSecond = 2;
        const int ExpectedThird = 3;

        var sideEffects = new List<int>();

        var result = await ObservableAsync.Range(1, 3)
            .Do(async (x, _) =>
            {
                await Task.Yield();
                sideEffects.Add(x);
            })
            .ToListAsync();

        await Assert.That(result).IsEquivalentTo([1, ExpectedSecond, ExpectedThird]);
        await Assert.That(sideEffects).IsEquivalentTo([1, ExpectedSecond, ExpectedThird]);
    }

    /// <summary>Tests Do with completion handler invokes on completed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoWithCompletionHandler_ThenInvokesOnCompleted()
    {
        Result? completion = null;

        await ObservableAsync.Empty<int>()
            .Do(
                (Action<int>?)null,
                (Action<Exception>?)null,
                r => completion = r)
            .WaitCompletionAsync();

        await Assert.That(completion).IsNotNull();
        await Assert.That(completion!.Value.IsSuccess).IsTrue();
    }

    /// <summary>
    /// Verifies that Cast completes with a failure containing an <see cref="InvalidCastException"/>
    /// when the source emits an element that cannot be cast to the target type.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCastIncompatibleType_ThenCompletesWithFailure()
    {
        var source = ObservableAsync.Return<object>(42);
        Result? completionResult = null;
        var tcs = new TaskCompletionSource();

        await using var sub = await source.Cast<object, string>()
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    tcs.TrySetResult();
                    return default;
                });

        await AsyncTestHelpers.WaitForConditionAsync(
            () => tcs.Task.IsCompleted,
            TimeSpan.FromSeconds(5));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsFalse();
        await Assert.That(completionResult.Value.Exception).IsTypeOf<InvalidCastException>();
    }

    /// <summary>Tests Cast with compatible type casts correctly.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCastCompatibleType_ThenCasts()
    {
        var source = ObservableAsync.Return<object>("hello");

        var result = await source.Cast<object, string>().ToListAsync();

        await Assert.That(result).IsEquivalentTo(["hello"]);
    }

    /// <summary>Tests OfType with matching type filters correctly.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOfTypeMatchingType_ThenFiltersCorrectly()
    {
        var items = new object[] { 1, "two", 3, "four" };
        var source = items.ToObservableAsync();

        var strings = await source.OfType<object, string>().ToListAsync();

        await Assert.That(strings).IsEquivalentTo(["two", "four"]);
    }

    /// <summary>Tests OfType with no matches emits nothing.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOfTypeNoMatches_ThenEmitsNothing()
    {
        var source = new object[] { 1, 2, 3 }.ToObservableAsync();

        var strings = await source.OfType<object, string>().ToListAsync();

        await Assert.That(strings).IsEmpty();
    }

    /// <summary>
    /// Verifies that disposing a Prepend subscription from inside an OnNext callback
    /// triggers the early return guard when cancellation is requested during the prepend loop.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependCancelledBeforeAllValues_ThenEarlyReturn()
    {
        var received = new List<int>();
        var values = Enumerable.Range(1, 100).ToArray();
        IAsyncDisposable? subscription = null;

        var source = ObservableAsync.Never<int>();
        var pipeline = source.Prepend(values);

        subscription = await pipeline.SubscribeAsync(
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

        await AsyncTestHelpers.WaitForConditionAsync(
            () => received.Count >= 3,
            TimeSpan.FromSeconds(5));

        await Assert.That(received.Count).IsGreaterThanOrEqualTo(MinReceivedCount);
    }

    /// <summary>
    /// Verifies that when Prepend's source throws an exception and the observer's OnCompletedAsync
    /// also throws, the completion exception is routed to the UnhandledExceptionHandler.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S112", Justification = "Deliberately throws a generic exception type to verify operator error-handling pathways.")]
    [SuppressMessage("Microsoft.Design", "CA2201", Justification = "Deliberately uses a generic exception type to verify operator error-handling pathways with arbitrary exception kinds.")]
    public async Task WhenPrependSourceThrowsAndCompletionAlsoThrows_ThenRoutedToHandler()
    {
        using var unhandled = new UnhandledExceptionCapture();
        var completionException = new InvalidOperationException("completion failed");

        var source = ObservableAsync.Create<int>((_, _) =>
            ValueTask.FromException<IAsyncDisposable>(new ApplicationException("source error")));

        var pipeline = source.Prepend(42);

        await using var sub = await pipeline.SubscribeAsync(
            (_, _) => default,
            null,
            _ => throw completionException);

        var exception = await unhandled.WaitForAsync("completion failed", TimeSpan.FromSeconds(5));

        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!).IsTypeOf<InvalidOperationException>();
        await Assert.That(exception!.Message).IsEqualTo("completion failed");
    }

    /// <summary>
    /// Verifies that async Do with an onErrorResume callback invokes the callback
    /// when the source emits a resumable error, and forwards the error downstream.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoAsyncWithOnErrorResume_ThenInvokesCallbackAndForwardsError()
    {
        var resumedErrors = new List<Exception>();
        var downstreamErrors = new List<Exception>();
        var tcs = new TaskCompletionSource();

        var source = ObservableAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(new InvalidOperationException("test error"), ct);
            await observer.OnNextAsync(2, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        await using var sub = await source
            .Do(
                (Func<int, CancellationToken, ValueTask>?)null,
                async (ex, _) =>
                {
                    await Task.Yield();
                    resumedErrors.Add(ex);
                },
                onCompleted: null)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    downstreamErrors.Add(ex);
                    return default;
                },
                _ =>
                {
                    tcs.TrySetResult();
                    return default;
                });

        await AsyncTestHelpers.WaitForConditionAsync(
            () => tcs.Task.IsCompleted,
            TimeSpan.FromSeconds(5));

        await Assert.That(resumedErrors).Count().IsEqualTo(1);
        await Assert.That(resumedErrors[0].Message).IsEqualTo("test error");
        await Assert.That(downstreamErrors).Count().IsEqualTo(1);
    }

    /// <summary>
    /// Verifies that async Do with an onCompleted callback invokes the callback
    /// when the source sequence completes.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoAsyncWithOnCompleted_ThenInvokesCallback()
    {
        Result? capturedResult = null;
        var tcs = new TaskCompletionSource();

        var source = ObservableAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(42, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        await using var sub = await source
            .Do(
                (Func<int, CancellationToken, ValueTask>?)null,
                onErrorResume: null,
                onCompleted: async result =>
                {
                    await Task.Yield();
                    capturedResult = result;
                })
            .SubscribeAsync(
                (_, _) => default,
                null,
                _ =>
                {
                    tcs.TrySetResult();
                    return default;
                });

        await AsyncTestHelpers.WaitForConditionAsync(
            () => tcs.Task.IsCompleted,
            TimeSpan.FromSeconds(5));

        await Assert.That(capturedResult).IsNotNull();
        await Assert.That(capturedResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>
    /// Verifies that sync Do with an onErrorResume callback invokes the callback
    /// when the source emits a resumable error, and forwards the error downstream.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoSyncWithOnErrorResume_ThenInvokesCallbackAndForwardsError()
    {
        var resumedErrors = new List<Exception>();
        var downstreamErrors = new List<Exception>();
        var tcs = new TaskCompletionSource();

        var source = ObservableAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(new InvalidOperationException("sync error"), ct);
            await observer.OnNextAsync(2, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        await using var sub = await source
            .Do(
                (Action<int>?)null,
                ex => resumedErrors.Add(ex),
                (Action<Result>?)null)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    downstreamErrors.Add(ex);
                    return default;
                },
                _ =>
                {
                    tcs.TrySetResult();
                    return default;
                });

        await AsyncTestHelpers.WaitForConditionAsync(
            () => tcs.Task.IsCompleted,
            TimeSpan.FromSeconds(5));

        await Assert.That(resumedErrors).Count().IsEqualTo(1);
        await Assert.That(resumedErrors[0].Message).IsEqualTo("sync error");
        await Assert.That(downstreamErrors).Count().IsEqualTo(1);
    }

    /// <summary>
    /// Verifies that ObserveOn with an IScheduler creates the correct async context
    /// and emits values through the pipeline.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnIScheduler_ThenEmitsValues()
    {
        var scheduler = ReactiveUI.Primitives.Concurrency.Sequencer.Immediate;

        const int ExpectedSecond = 2;
        const int ExpectedThird = 3;

        var result = await ObservableAsync.Range(1, 3)
            .ObserveOn(scheduler)
            .ToListAsync();

        await Assert.That(result).IsEquivalentTo(new[] { 1, ExpectedSecond, ExpectedThird });
    }

    /// <summary>
    /// Verifies that ObserveOn forwards resumable errors emitted by the source
    /// through the context-switched observer to the downstream error handler.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenObserveOnSourceEmitsResumableError_ThenForwardsErrorDownstream()
    {
        const int ExpectedSecond = 2;
        var downstreamErrors = new List<Exception>();
        var receivedValues = new List<int>();
        var tcs = new TaskCompletionSource();

        var source = ObservableAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(new InvalidOperationException("resumable"), ct);
            await observer.OnNextAsync(2, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        await using var sub = await source
            .ObserveOn(AsyncContext.Default)
            .SubscribeAsync(
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
                    tcs.TrySetResult();
                    return default;
                });

        await AsyncTestHelpers.WaitForConditionAsync(
            () => tcs.Task.IsCompleted,
            TimeSpan.FromSeconds(5));

        await Assert.That(receivedValues).IsEquivalentTo([1, ExpectedSecond]);
        await Assert.That(downstreamErrors).Count().IsEqualTo(1);
        await Assert.That(downstreamErrors[0]).IsTypeOf<InvalidOperationException>();
        await Assert.That(downstreamErrors[0].Message).IsEqualTo("resumable");
    }

    /// <summary>
    /// Verifies that when the CancellationToken passed to Prepend is already cancelled before
    /// the prepend loop begins iterating, the loop exits immediately without emitting any values.
    /// This covers the cancellation-requested early return guard inside the prepend foreach loop.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPrependTokenCancelledBeforeIteration_ThenEmitsNoValues()
    {
        const int MaxReceivedCount = 100;
        var received = new List<int>();
        using var cts = new CancellationTokenSource();

        var source = ObservableAsync.Never<int>();
        var pipeline = source.Prepend(Enumerable.Range(1, 100));

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

    /// <summary>
    /// Verifies that when the source observable passed to Prepend throws an exception during
    /// subscription and the downstream observer's OnCompletedAsync handler also throws,
    /// the secondary exception from the completion handler is routed to the
    /// <see cref="UnhandledExceptionHandler"/>.
    /// This covers the inner catch block that guards against completion handler failures.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S112", Justification = "Deliberately throws a generic exception type to verify operator error-handling pathways.")]
    [SuppressMessage("Microsoft.Design", "CA2201", Justification = "Deliberately uses a generic exception type to verify operator error-handling pathways with arbitrary exception kinds.")]
    public async Task WhenPrependSourceThrowsAndOnCompletedThrows_ThenSecondaryExceptionRoutedToHandler()
    {
        using var unhandled = new UnhandledExceptionCapture();
        var secondaryException = new InvalidOperationException("onCompleted blew up");

        var source = ObservableAsync.Create<int>((_, _) =>
            ValueTask.FromException<IAsyncDisposable>(new ApplicationException("source failure")));

        // Prepend a single value so the prepend loop completes, then SubscribeAsync on the
        // throwing source triggers the catch path. The completion handler throws a second
        // exception, which should be routed to the unhandled exception handler.
        var pipeline = source.Prepend(1);

        await using var sub = await pipeline.SubscribeAsync(
            (_, _) => default,
            null,
            _ => throw secondaryException);

        var exception = await unhandled.WaitForAsync("onCompleted blew up", TimeSpan.FromSeconds(5));

        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!).IsTypeOf<InvalidOperationException>();
        await Assert.That(exception!.Message).IsEqualTo("onCompleted blew up");
    }

    /// <summary>
    /// Verifies that Yield with a null source throws <see cref="ArgumentNullException"/>.
    /// </summary>
    [Test]
    public void WhenYieldNullSource_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(() =>
            ObservableAsync.Yield<int>(null!));

    /// <summary>
    /// Verifies that Yield forwards all elements from the source sequence.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenYield_ThenForwardsAllElements()
    {
        const int Expected2 = 2;
        const int Expected3 = 3;
        const int Expected4 = 4;
        const int Expected5 = 5;

        var result = await ObservableAsync.Range(1, 5)
            .Yield()
            .ToListAsync();

        await Assert.That(result).IsEquivalentTo([1, Expected2, Expected3, Expected4, Expected5]);
    }

    /// <summary>
    /// Verifies that Yield forwards completion from the source sequence.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenYield_ThenForwardsCompletion()
    {
        Result? capturedResult = null;
        var tcs = new TaskCompletionSource();

        await using var sub = await ObservableAsync.Return(42)
            .Yield()
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    capturedResult = result;
                    tcs.TrySetResult();
                    return default;
                });

        await AsyncTestHelpers.WaitForConditionAsync(
            () => tcs.Task.IsCompleted,
            TimeSpan.FromSeconds(5));

        await Assert.That(capturedResult).IsNotNull();
        await Assert.That(capturedResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>
    /// Verifies that Yield forwards errors from the source sequence.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenYieldSourceErrors_ThenForwardsError()
    {
        Result? capturedResult = null;
        var tcs = new TaskCompletionSource();

        var source = ObservableAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnCompletedAsync(Result.Failure(new InvalidOperationException("yield error")));
            return DisposableAsync.Empty;
        });

        await using var sub = await source
            .Yield()
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    capturedResult = result;
                    tcs.TrySetResult();
                    return default;
                });

        await AsyncTestHelpers.WaitForConditionAsync(
            () => tcs.Task.IsCompleted,
            TimeSpan.FromSeconds(5));

        await Assert.That(capturedResult).IsNotNull();
        await Assert.That(capturedResult!.Value.IsSuccess).IsFalse();
    }

    /// <summary>
    /// Verifies that the three-argument GroupBy overload throws <see cref="ArgumentNullException"/>
    /// when the source parameter is null.
    /// </summary>
    [Test]
    public void WhenGroupByWithSubjectSelectorNullSource_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(() =>
            ObservableAsync.GroupBy<int, int>(
                null!,
                static x => x,
                static _ => SubjectAsync.Create<int>()));

    /// <summary>
    /// Verifies that the three-argument GroupBy overload throws <see cref="ArgumentNullException"/>
    /// when the keySelector parameter is null.
    /// </summary>
    [Test]
    public void WhenGroupByWithSubjectSelectorNullKeySelector_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(() =>
            ObservableAsync.Empty<int>().GroupBy<int, int>(
                null!,
                static _ => SubjectAsync.Create<int>()));

    /// <summary>
    /// Verifies that the three-argument GroupBy overload with a custom group subject selector
    /// correctly groups elements by key using the provided subject factory.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGroupByWithCustomSubjectSelector_ThenGroupsByKey()
    {
        const int ExpectedGroupCount = 2;
        const int OddSecond = 3;
        const int OddThird = 5;
        const int EvenFirst = 2;
        const int EvenSecond = 4;
        const int EvenThird = 6;

        var source = Sequence123456.ToObservableAsync();

        var groups = new Dictionary<int, List<int>>();
        var tcs = new TaskCompletionSource();

        await using var sub = await source
            .GroupBy(
                static x => x % 2,
                static _ => SubjectAsync.Create<int>())
            .SubscribeAsync(
                async (group, ct) =>
                {
                    var list = new List<int>();
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
                    tcs.TrySetResult();
                    return default;
                });

        await AsyncTestHelpers.WaitForConditionAsync(
            () => tcs.Task.IsCompleted,
            TimeSpan.FromSeconds(5));

        await Assert.That(groups).Count().IsEqualTo(ExpectedGroupCount);
        await Assert.That(groups[1]).IsEquivalentTo([1, OddSecond, OddThird]);
        await Assert.That(groups[0]).IsEquivalentTo([EvenFirst, EvenSecond, EvenThird]);
    }

    /// <summary>
    /// Verifies that GroupBy forwards resumable errors from the source through to the downstream observer
    /// via <see cref="IObserverAsync{T}.OnErrorResumeAsync"/>.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGroupBySourceEmitsResumableError_ThenForwardsErrorDownstream()
    {
        var downstreamErrors = new List<Exception>();
        var tcs = new TaskCompletionSource();

        var source = ObservableAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnNextAsync(1, ct);
            await observer.OnErrorResumeAsync(new InvalidOperationException("group error"), ct);
            await observer.OnNextAsync(2, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        await using var sub = await source
            .GroupBy(static x => x, static _ => SubjectAsync.Create<int>())
            .SubscribeAsync(
                async (group, ct) =>
                {
                    await group.SubscribeAsync(
                        (_, _) => default,
                        null,
                        null,
                        ct);
                },
                (ex, _) =>
                {
                    downstreamErrors.Add(ex);
                    return default;
                },
                _ =>
                {
                    tcs.TrySetResult();
                    return default;
                });

        await AsyncTestHelpers.WaitForConditionAsync(
            () => tcs.Task.IsCompleted,
            TimeSpan.FromSeconds(5));

        await Assert.That(downstreamErrors).Count().IsEqualTo(1);
        await Assert.That(downstreamErrors[0]).IsTypeOf<InvalidOperationException>();
        await Assert.That(downstreamErrors[0].Message).IsEqualTo("group error");
    }

    /// <summary>
    /// Verifies that when Prepend's source throws during subscription and the raw observer's
    /// <see cref="IObserverAsync{T}.OnCompletedAsync"/> also throws, the secondary exception
    /// from the completion handler is routed to the <see cref="UnhandledExceptionHandler"/>.
    /// This exercises the inner catch block (lines 73-74) that guards against completion handler failures
    /// by using a raw <see cref="IObserverAsync{T}"/> implementation that bypasses the
    /// <see cref="ObserverAsync{T}"/> base class exception swallowing.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S112", Justification = "Deliberately throws a generic exception type to verify operator error-handling pathways.")]
    [SuppressMessage("Microsoft.Design", "CA2201", Justification = "Deliberately uses a generic exception type to verify operator error-handling pathways with arbitrary exception kinds.")]
    public async Task WhenPrependSourceThrowsAndRawObserverCompletionThrows_ThenRoutedToUnhandledHandler()
    {
        using var unhandled = new UnhandledExceptionCapture();
        var completionException = new InvalidOperationException("raw observer completion failed");

        var source = ObservableAsync.Create<int>((_, _) =>
            ValueTask.FromException<IAsyncDisposable>(new ApplicationException("source subscribe error")));

        var pipeline = source.Prepend(1);

        var rawObserver = new ThrowingOnCompletedObserver<int>(completionException);
        await using var sub = await pipeline.SubscribeAsync(rawObserver, CancellationToken.None);

        var exception = await unhandled.WaitForAsync("raw observer completion failed", TimeSpan.FromSeconds(5));

        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!).IsTypeOf<InvalidOperationException>();
        await Assert.That(exception!.Message).IsEqualTo("raw observer completion failed");
    }

    /// <summary>Tests Do with onErrorResume callback invokes callback on error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoWithOnErrorResume_ThenCallbackInvoked()
    {
        var directSource = new DirectSource<int>();
        var errors = new List<Exception>();
        var error = new InvalidOperationException("test");

        await using var sub = await directSource.Do(
            (Action<int>?)null,
            ex => errors.Add(ex),
            (Action<Result>?)null).SubscribeAsync(
            static (_, _) => default,
            static (_, _) => default,
            static _ => default);

        await directSource.EmitError(error);
        await directSource.Complete(Result.Success);

        await Assert.That(errors).Count().IsEqualTo(1);
    }

    /// <summary>Tests GroupBy source failure propagates.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGroupBySourceFails_ThenErrorPropagated()
    {
        const int GroupModulus = 2;
        var error = new InvalidOperationException("group-fail");

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await ObservableAsync.Throw<int>(error)
                .GroupBy(x => x % GroupModulus)
                .FirstAsync());
    }

    /// <summary>Tests Using operator disposes resource on source failure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUsingSourceThrows_ThenResourceDisposed()
    {
        var disposed = false;
        var error = new InvalidOperationException("test");

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await ObservableAsync.Using(
                _ =>
                    {
                        try
                        {
                            return ValueTask.FromResult(DisposableAsync.Create(() =>
                            {
                                disposed = true;
                                return default;
                            }));
                        }
                        catch (Exception exception)
                        {
                            return ValueTask.FromException<IAsyncDisposable>(exception);
                        }
                    },
                _ => ObservableAsync.Throw<int>(error)).FirstAsync());

        await Assert.That(disposed).IsTrue();
    }

    /// <summary>Verifies the async-accumulator <c>Scan</c> overload's sync-completed fast path —
    /// returning a synchronously-completed <see cref="ValueTask{TResult}"/> from the accumulator
    /// takes the inline <c>pending.Result</c> branch.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScanAsyncAccumulatorReturnsSync_ThenForwardsAccumulator()
    {
        const int ThirdRunningTotal = 3;
        const int SixthRunningTotal = 6;
        var result = await ObservableAsync.Range(1, ScanInputCount)
            .Scan(0, static (acc, x, _) => new ValueTask<int>(acc + x))
            .ToListAsync();

        await Assert.That(result).IsEquivalentTo([1, ThirdRunningTotal, SixthRunningTotal]);
    }

    /// <summary>Verifies that the sync-accumulator <c>Scan</c> overload forwards a non-terminal
    /// upstream error downstream.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScanSyncSourceErrorResume_ThenForwarded()
    {
        var subject = SubjectAsync.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await subject.Values
            .Scan(0, static (acc, x) => acc + x)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("scan-sync-error");
        await subject.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that the async-accumulator <c>Scan</c> overload forwards a non-terminal
    /// upstream error downstream.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScanAsyncSourceErrorResume_ThenForwarded()
    {
        var subject = SubjectAsync.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await subject.Values
            .Scan(0, static (acc, x, _) => new ValueTask<int>(acc + x))
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("scan-async-error");
        await subject.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>
    /// A raw <see cref="IObserverAsync{T}"/> implementation that throws a specified exception
    /// from <see cref="OnCompletedAsync"/>. Unlike <see cref="ObserverAsync{T}"/>, this
    /// implementation does not catch exceptions internally, allowing callers to observe
    /// the thrown exception directly.
    /// </summary>
    /// <typeparam name="T">The type of elements received by the observer.</typeparam>
    /// <param name="completionException">The exception to throw when <see cref="OnCompletedAsync"/> is called.</param>
    private sealed class ThrowingOnCompletedObserver<T>(Exception completionException) : IObserverAsync<T>
    {
        /// <inheritdoc/>
        public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        public ValueTask OnCompletedAsync(Result result) => throw completionException;

        /// <inheritdoc/>
        public ValueTask DisposeAsync() => default;
    }
}
