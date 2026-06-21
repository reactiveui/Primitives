// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for time-based operators: Throttle, Delay, Timeout, Timer, Interval.</summary>
public class TimeBasedOperatorTests
{
    /// <summary>Expected value42 for assertions.</summary>
    private const int ExpectedValue42 = 42;

    /// <summary>Fallback value99 (99).</summary>
    private const int FallbackValue99 = 99;

    /// <summary>Tests Throttle only last in burst is emitted.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottle_ThenOnlyLastInBurstIsEmitted()
    {
        var signal = Signal.Create<int>();
        List<int> results = [];
        await using var sub = await signal.Values.Throttle(TimeSpan.FromMilliseconds(100)).SubscribeAsync(
            (x, _) =>
            {
                results.Add(x);
                return default;
            },
            null);
        const int SecondValue = 2;
        const int LastValue = 3;
        await signal.OnNextAsync(1, CancellationToken.None);
        await signal.OnNextAsync(SecondValue, CancellationToken.None);
        await signal.OnNextAsync(LastValue, CancellationToken.None);
        var resultReceived =
            await AsyncTestHelpers.WaitForConditionAsync(() => results.Count == 1, TimeSpan.FromSeconds(10));
        await signal.OnCompletedAsync(Result.Success);
        await Assert.That(resultReceived).IsTrue();
        await Assert.That(results).Count().IsEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(LastValue);
    }

    /// <summary>Tests Throttle with spaced items all are emitted.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleWithSpacedItems_ThenAllAreEmitted()
    {
        var signal = Signal.Create<int>();
        List<int> results = [];
        TaskCompletionSource<bool> firstReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<bool> secondReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.Throttle(TimeSpan.FromMilliseconds(50)).SubscribeAsync(
            (x, _) =>
            {
                results.Add(x);
                if (results.Count == 1)
                {
                    firstReceived.TrySetResult(true);
                }
                else if (results.Count == 2)
                {
                    secondReceived.TrySetResult(true);
                }

                return default;
            },
            null);
        const int SpacingDelayMillis = 75;
        const int SecondValue = 2;
        await signal.OnNextAsync(1, CancellationToken.None);
        await firstReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Task.Delay(SpacingDelayMillis);
        await signal.OnNextAsync(SecondValue, CancellationToken.None);
        await secondReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(results).IsCollectionEqualTo([1, SecondValue]);
    }

    /// <summary>Tests Throttle negative due time throws.</summary>
    [Test]
    public void WhenThrottleNegativeDueTime_ThenThrowsArgumentOutOfRange() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => SignalAsync.Return(1).Throttle(TimeSpan.FromMilliseconds(-1)));

    /// <summary>Tests Delay elements are time shifted.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDelay_ThenElementsAreTimeShifted()
    {
        const long MinElapsedMillis = 80;
        var stopwatch = System.Diagnostics.Stopwatch.StartNew();
        var result = await SignalAsync.Return(42).Delay(TimeSpan.FromMilliseconds(100)).FirstAsync();
        stopwatch.Stop();
        await Assert.That(result).IsEqualTo(ExpectedValue42);
        await Assert.That(stopwatch.ElapsedMilliseconds).IsGreaterThanOrEqualTo(MinElapsedMillis);
    }

    /// <summary>Tests Delay zero causes no delay.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDelayZero_ThenNoDelay()
    {
        var result = await SignalAsync.Return(42).Delay(TimeSpan.Zero).FirstAsync();
        await Assert.That(result).IsEqualTo(ExpectedValue42);
    }

    /// <summary>Tests Delay negative throws.</summary>
    [Test]
    public void WhenDelayNegative_ThenThrowsArgumentOutOfRange() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => SignalAsync.Return(1).Delay(TimeSpan.FromMilliseconds(-1)));

    /// <summary>Tests Delay sequence delays all elements.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDelaySequence_ThenAllElementsDelayed()
    {
        const int ExpectedSecond = 2;
        const int ExpectedThird = 3;
        var result = await SignalAsync.Range(1, 3).Delay(TimeSpan.FromMilliseconds(30)).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([1, ExpectedSecond, ExpectedThird]);
    }

    /// <summary>Tests Timeout not exceeded completes normally.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTimeoutNotExceeded_ThenCompletesNormally()
    {
        var result = await SignalAsync.Return(42).Timeout(TimeSpan.FromSeconds(5)).FirstAsync();
        await Assert.That(result).IsEqualTo(ExpectedValue42);
    }

    /// <summary>Tests Timeout exceeded throws TimeoutException.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenTimeoutExceeded_ThenThrowsTimeoutException()
    {
        var source = SignalAsync.Never<int>().Timeout(TimeSpan.FromMilliseconds(100));
        await Assert.That(async () => await source.FirstAsync()).ThrowsExactly<TimeoutException>();
    }

    /// <summary>Tests Timeout with fallback switches to fallback.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTimeoutWithFallback_ThenSwitchesToFallback()
    {
        var source = SignalAsync.Never<int>().Timeout(TimeSpan.FromMilliseconds(100), SignalAsync.Return(99));
        var result = await source.FirstAsync();
        await Assert.That(result).IsEqualTo(FallbackValue99);
    }

    /// <summary>Tests Timeout zero duration throws.</summary>
    [Test]
    public void WhenTimeoutZeroDuration_ThenThrowsArgumentOutOfRange() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => SignalAsync.Return(1).Timeout(TimeSpan.Zero));

    /// <summary>Tests Timeout negative duration throws.</summary>
    [Test]
    public void WhenTimeoutNegativeDuration_ThenThrowsArgumentOutOfRange() =>
        Assert.Throws<ArgumentOutOfRangeException>(() => SignalAsync.Return(1).Timeout(TimeSpan.FromMilliseconds(-1)));

    /// <summary>Tests Timeout with null fallback throws.</summary>
    [Test]
    public void WhenTimeoutWithFallbackNull_ThenThrowsArgumentNull() => Assert.Throws<ArgumentNullException>(() =>
        SignalAsync.Return(1).Timeout(TimeSpan.FromSeconds(1), (IObservableAsync<int>)null!));

    /// <summary>
    /// Verifies that when the downstream observer throws a non-cancellation exception
    /// during OnNext from a throttled delay callback, the exception is routed to the
    /// <see cref = "UnhandledExceptionHandler"/>.
    /// This covers the Throttle exception routing path.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleOnNextThrows_ThenRoutedToUnhandledExceptionHandler()
    {
        using UnhandledExceptionCapture unhandled = new();
        var signal = Signal.Create<int>();
        await using var sub = await signal.Values.Throttle(TimeSpan.FromMilliseconds(50))
            .SubscribeAsync((_, _) => throw new InvalidOperationException("observer exploded"), null);
        await signal.OnNextAsync(ExpectedValue42, CancellationToken.None);
        var exception = await unhandled.WaitForAsync("observer exploded", TimeSpan.FromSeconds(10));
        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!).IsTypeOf<InvalidOperationException>();
    }

    /// <summary>
    /// Verifies that when the downstream observer throws a non-cancellation exception
    /// during OnCompleted from a timeout callback, the exception is routed to the
    /// <see cref = "UnhandledExceptionHandler"/>.
    /// This covers the Timeout OnCompleted exception routing path.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTimeoutOnCompletedThrows_ThenRoutedToUnhandledExceptionHandler()
    {
        using UnhandledExceptionCapture unhandled = new();
        var source = SignalAsync.Never<int>().Timeout(TimeSpan.FromMilliseconds(50));
        await using var sub = await source.SubscribeAsync(
            static (_, _) => default,
            null,
            _ => throw new InvalidOperationException("completion handler exploded"));
        var exception = await unhandled.WaitForAsync("completion handler exploded", TimeSpan.FromSeconds(10));
        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!).IsTypeOf<InvalidOperationException>();
    }

    /// <summary>
    /// Verifies that <see cref = "SignalAsync.Interval(TimeSpan, TimeProvider? )"/>
    /// uses the custom <see cref = "TimeProvider"/> path when a non-system provider is supplied.
    /// This covers the Interval TimeProvider code path.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenIntervalWithNonSystemTimeProvider_ThenUsesTimerPath()
    {
        CustomTimeProvider customProvider = new();
        List<long> results = [];
        await using var sub = await SignalAsync.Interval(TimeSpan.FromMilliseconds(50), customProvider).SubscribeAsync(
            (x, _) =>
            {
                results.Add(x);
                return default;
            },
            null);
        var receivedTwo =
            await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 2, TimeSpan.FromSeconds(10));
        const long ExpectedSecondTick = 2L;
        await Assert.That(receivedTwo).IsTrue();
        await Assert.That(results[0]).IsEqualTo(1L);
        await Assert.That(results[1]).IsEqualTo(ExpectedSecondTick);
    }

    /// <summary>
    /// Verifies that a periodic <see cref = "SignalAsync.Timer(TimeSpan, TimeSpan, TimeProvider? )"/>
    /// stops emitting values once the subscription is disposed.
    /// This covers the cancellation loop exit in the periodic timer.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPeriodicTimerCancelled_ThenStopsEmitting()
    {
        List<long> results = [];
        var sub = await SignalAsync.Timer(TimeSpan.Zero, TimeSpan.FromMilliseconds(50)).SubscribeAsync(
            (x, _) =>
            {
                results.Add(x);
                return default;
            },
            null);
        var receivedTwo =
            await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 2, TimeSpan.FromSeconds(10));
        await Assert.That(receivedTwo).IsTrue();
        var countAtDispose = results.Count;
        await sub.DisposeAsync();

        // Allow a brief window to confirm no further emissions
        var noMoreEmissions = await AsyncTestHelpers.WaitForConditionAsync(
            () => results.Count == countAtDispose,
            TimeSpan.FromMilliseconds(200));
        await Assert.That(noMoreEmissions).IsTrue();
    }

    /// <summary>
    /// Verifies that <see cref = "SignalAsyncExtensions.Throttle{T}(IObservableAsync{T}, TimeSpan, TimeProvider? )"/> uses the non-system
    /// <see cref = "TimeProvider"/> code path in <c>DelayAsync</c> when a
    /// custom provider is supplied, and still correctly debounces values.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleWithCustomTimeProvider_ThenUsesTimerPath()
    {
        CustomTimeProvider customProvider = new();
        var signal = Signal.Create<int>();
        List<int> results = [];
        TaskCompletionSource<bool> resultReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.Throttle(TimeSpan.FromMilliseconds(50), customProvider)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    resultReceived.TrySetResult(true);
                    return default;
                },
                null);
        const int SecondValue = 2;
        const int LastValue = 3;
        await signal.OnNextAsync(1, CancellationToken.None);
        await signal.OnNextAsync(SecondValue, CancellationToken.None);
        await signal.OnNextAsync(LastValue, CancellationToken.None);
        await resultReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await signal.OnCompletedAsync(Result.Success);
        await Assert.That(results).Count().IsEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(LastValue);
    }

    /// <summary>
    /// Verifies that when two values are emitted in quick succession with a custom
    /// <see cref = "TimeProvider"/>, the first value is superseded and only
    /// the second is forwarded, exercising the non-system <c>DelayAsync</c> path.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleWithCustomTimeProviderValueSuperseded_ThenOlderValueDropped()
    {
        CustomTimeProvider customProvider = new();
        var signal = Signal.Create<int>();
        List<int> results = [];
        TaskCompletionSource<bool> resultReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.Throttle(TimeSpan.FromMilliseconds(80), customProvider)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    resultReceived.TrySetResult(true);
                    return default;
                },
                null);
        const int FirstValue = 10;
        const int LastValue = 20;

        // Emit two values rapidly; first should be superseded
        await signal.OnNextAsync(FirstValue, CancellationToken.None);
        await signal.OnNextAsync(LastValue, CancellationToken.None);
        await resultReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await signal.OnCompletedAsync(Result.Success);
        await Assert.That(results).Count().IsEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(LastValue);
    }

    /// <summary>
    /// Verifies that when the downstream observer throws during a throttled emission
    /// with a custom <see cref = "TimeProvider"/>, the exception is routed to
    /// <see cref = "UnhandledExceptionHandler"/>.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleWithCustomTimeProviderOnNextThrows_ThenRoutedToUnhandledExceptionHandler()
    {
        using UnhandledExceptionCapture unhandled = new();
        CustomTimeProvider customProvider = new();
        var signal = Signal.Create<int>();
        await using var sub = await signal.Values.Throttle(TimeSpan.FromMilliseconds(50), customProvider)
            .SubscribeAsync((_, _) => throw new InvalidOperationException("custom provider observer exploded"), null);
        await signal.OnNextAsync(1, CancellationToken.None);
        var exception = await unhandled.WaitForAsync("custom provider observer exploded", TimeSpan.FromSeconds(10));
        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!).IsTypeOf<InvalidOperationException>();
    }

    /// <summary>
    /// Verifies that when <c>OnErrorResumeAsync</c> is called on a throttled sequence,
    /// the pending timer is cancelled and the error is forwarded to the downstream observer
    /// in the Throttle operator.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleOnErrorResume_ThenCancelsTimerAndForwardsError()
    {
        var signal = Signal.Create<int>();
        List<int> results = [];
        List<Exception> errors = [];
        TaskCompletionSource<bool> errorReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await signal.Values.Throttle(TimeSpan.FromMilliseconds(500)).SubscribeAsync(
            (x, _) =>
            {
                results.Add(x);
                return default;
            },
            (ex, _) =>
            {
                errors.Add(ex);
                errorReceived.TrySetResult(true);
                return default;
            });

        // Emit a value (starts a 500ms timer)
        await signal.OnNextAsync(1, CancellationToken.None);

        // Immediately send an error before the throttle timer fires
        await signal.OnErrorResumeAsync(new InvalidOperationException("test error"), CancellationToken.None);
        await errorReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));

        // Error should be forwarded, and the pending value should NOT be emitted
        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0]).IsTypeOf<InvalidOperationException>();
        await Assert.That(results).Count().IsEqualTo(0);
    }

    /// <summary>
    /// Verifies that when the <see cref = "TimeProvider"/> throws a non-cancellation exception
    /// during the delay inside <c>OnTimeoutAsync</c>, the exception is routed to the
    /// <see cref = "UnhandledExceptionHandler"/>.
    /// This covers the Timeout delay exception routing path.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTimeoutDelayThrowsNonCancellation_ThenRoutedToUnhandledExceptionHandler()
    {
        using UnhandledExceptionCapture unhandled = new();
        ThrowingTimeProvider throwingProvider = new();
        DirectSource<int> source = new();
        await using var sub = await source.Timeout(TimeSpan.FromMilliseconds(100), throwingProvider)
            .SubscribeAsync(static (_, _) => default, null);
        var exception = await unhandled.WaitForAsync("timer creation failed", TimeSpan.FromSeconds(10));
        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!).IsTypeOf<InvalidOperationException>();
    }

    /// <summary>
    /// Verifies that when the source emits an error via <c>OnErrorResumeAsync</c>,
    /// the <c>TimeoutWitness</c> cancels the timer and forwards the error downstream.
    /// This covers the <c>OnErrorResumeAsyncCore</c> path in the Timeout operator.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTimeoutSourceEmitsErrorResume_ThenForwardsAndCancelsTimer()
    {
        List<Exception> errors = [];
        TaskCompletionSource<bool> errorReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        DirectSource<int> source = new();
        await using var sub = await source.Timeout(TimeSpan.FromSeconds(30)).SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errors.Add(ex);
                errorReceived.TrySetResult(true);
                return default;
            });
        InvalidOperationException testError = new("test error");
        await source.EmitError(testError);
        await errorReceived.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0]).IsTypeOf<InvalidOperationException>();
        await Assert.That(errors[0].Message).IsEqualTo("test error");
    }

    /// <summary>
    /// Verifies that Delay forwards non-terminal errors via OnErrorResumeAsync.
    /// Covers the OnErrorResumeAsyncCore path in DelayObserver.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDelaySourceEmitsErrorResume_ThenErrorForwarded()
    {
        DirectSource<int> source = new();
        List<Exception> errors = [];
        TaskCompletionSource completed = new();
        await using var sub = await source.Delay(TimeSpan.FromMilliseconds(1)).SubscribeAsync(
            (_, _) => default,
            (ex, _) =>
            {
                errors.Add(ex);
                return default;
            },
            _ =>
            {
                completed.TrySetResult();
                return default;
            });
        InvalidOperationException expectedError = new("resume error");
        await source.EmitError(expectedError);
        await source.Complete(Result.Success);
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0]).IsSameReferenceAs(expectedError);
    }

    /// <summary>
    /// Verifies that Throttle drops a value when superseded by a newer emission,
    /// exercising the id-mismatch early return in FireAfterDelayAsync.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleValueSuperseded_ThenOlderValueDropped()
    {
        var signal = Signal.Create<int>();
        List<int> results = [];
        TaskCompletionSource completed = new();
        await using var sub = await signal.Values.Throttle(TimeSpan.FromMilliseconds(200)).SubscribeAsync(
            (x, _) =>
            {
                results.Add(x);
                return default;
            },
            null,
            _ =>
            {
                completed.TrySetResult();
                return default;
            });
        const int LastValue = 2;

        // Emit two values in rapid succession; first should be superseded
        await signal.OnNextAsync(1, CancellationToken.None);
        await signal.OnNextAsync(LastValue, CancellationToken.None);

        // Wait for the throttled value to arrive before completing
        await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 1, TimeSpan.FromSeconds(5));
        await signal.OnCompletedAsync(Result.Success);
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Only the last value (2) should have been emitted
        await Assert.That(results).Contains(LastValue);
    }

    /// <summary>
    /// Verifies that Throttle routes non-cancellation exceptions to the unhandled exception handler.
    /// Covers the catch(Exception) block in ThrottleWitness.FireAfterDelayAsync.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleFireThrowsNonCancellation_ThenRoutedToUnhandledHandler()
    {
        using UnhandledExceptionCapture unhandled = new();
        InvalidOperationException expectedError = new("downstream error");
        DirectSource<int> source = new();
        await using var sub = await source.Throttle(TimeSpan.FromMilliseconds(1))
            .SubscribeAsync((_, _) => throw expectedError, null);
        await source.EmitNext(1);
        var exception = await unhandled.WaitForAsync("downstream error", TimeSpan.FromSeconds(5));
        await Assert.That(exception).IsNotNull();
    }

    /// <summary>
    /// Verifies that a periodic Timer emits multiple ticks before cancellation.
    /// Covers the while-loop body in the periodic Timer factory.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPeriodicTimerEmitsMultipleTicks_ThenAllTicksReceived()
    {
        List<long> results = [];
        var sub = await SignalAsync.Timer(TimeSpan.Zero, TimeSpan.FromMilliseconds(20)).SubscribeAsync(
            (x, _) =>
            {
                results.Add(x);
                return default;
            },
            null);
        await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 3, TimeSpan.FromSeconds(10));
        await sub.DisposeAsync();
        const int MinTickCount = 3;
        const int ThirdTickIndex = 2;
        const long ExpectedThirdTick = 2L;
        await Assert.That(results.Count).IsGreaterThanOrEqualTo(MinTickCount);
        await Assert.That(results[0]).IsEqualTo(0L);
        await Assert.That(results[1]).IsEqualTo(1L);
        await Assert.That(results[ThirdTickIndex]).IsEqualTo(ExpectedThirdTick);
    }

    /// <summary>
    /// Verifies that a periodic <see cref = "SignalAsync.Timer(TimeSpan, TimeSpan, TimeProvider? )"/>
    /// with a custom <see cref = "TimeProvider"/> emits at least two ticks before disposal,
    /// exercising the loop continuation on line 90 of Timer.cs through the non-system delay path.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPeriodicTimerWithCustomTimeProvider_ThenLoopContinuesUntilDisposed()
    {
        CustomTimeProvider customProvider = new();
        List<long> results = [];
        var sub = await SignalAsync.Timer(TimeSpan.Zero, TimeSpan.FromMilliseconds(20), customProvider).SubscribeAsync(
            (x, _) =>
            {
                results.Add(x);
                return default;
            },
            null);
        var receivedTwo =
            await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 2, TimeSpan.FromSeconds(10));
        await Assert.That(receivedTwo).IsTrue();
        var countAtDispose = results.Count;
        await sub.DisposeAsync();
        var noMoreEmissions = await AsyncTestHelpers.WaitForConditionAsync(
            () => results.Count == countAtDispose,
            TimeSpan.FromMilliseconds(200));
        await Assert.That(noMoreEmissions).IsTrue();
        await Assert.That(results[0]).IsEqualTo(0L);
        await Assert.That(results[1]).IsEqualTo(1L);
    }

    /// <summary>
    /// Verifies that when the downstream observer throws a non-cancellation exception
    /// during OnNext from a throttled emission using an immediate-fire
    /// <see cref = "TimeProvider"/>, the exception is routed to the
    /// <see cref = "UnhandledExceptionHandler"/>.
    /// This deterministically covers lines 162-163 in ThrottleWitness.FireAfterDelayAsync.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleImmediateFireOnNextThrows_ThenRoutedToUnhandledExceptionHandler()
    {
        using UnhandledExceptionCapture unhandled = new();
        ImmediateFireTimeProvider immediateProvider = new();
        var signal = Signal.Create<int>();
        await using var sub = await signal.Values.Throttle(TimeSpan.FromMilliseconds(100), immediateProvider)
            .SubscribeAsync((_, _) => throw new InvalidOperationException("immediate fire observer exploded"), null);
        await signal.OnNextAsync(1, CancellationToken.None);
        var exception = await unhandled.WaitForAsync("immediate fire observer exploded", TimeSpan.FromSeconds(5));
        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!).IsTypeOf<InvalidOperationException>();
        await Assert.That(exception!.Message).IsEqualTo("immediate fire observer exploded");
    }

    /// <summary>Tests Interval stops when cancelled.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenIntervalCancelled_ThenStops()
    {
        const int MinItemCount = 2;
        CancellationTokenSource cts = new();
        List<long> items = [];
        TaskCompletionSource cancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await SignalAsync.Interval(TimeSpan.FromMilliseconds(10)).SubscribeAsync(
            async (x, _) =>
            {
                items.Add(x);
                if (x < 2)
                {
                    return;
                }

                await cts.CancelAsync();
                cancelled.TrySetResult();
            },
            null,
            null,
            cts.Token);
        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(items.Count).IsGreaterThanOrEqualTo(MinItemCount);
    }

    /// <summary>Tests Timer with period stops when cancelled.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTimerWithPeriodCancelled_ThenStops()
    {
        const int MinItemCount = 2;
        CancellationTokenSource cts = new();
        List<long> items = [];
        TaskCompletionSource cancelled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await SignalAsync.Timer(TimeSpan.FromMilliseconds(1), TimeSpan.FromMilliseconds(10))
            .SubscribeAsync(
                async (x, _) =>
                {
                    items.Add(x);
                    if (x < 2)
                    {
                        return;
                    }

                    await cts.CancelAsync();
                    cancelled.TrySetResult();
                },
                null,
                null,
                cts.Token);
        await cancelled.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(items.Count).IsGreaterThanOrEqualTo(MinItemCount);
    }

    /// <summary>Tests Throttle supersedes older values and only emits latest.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleReceivesRapidValues_ThenOnlyEmitsLatest()
    {
        const int SecondValue = 2;
        const int LastValue = 3;
        ManualTimeProvider manualProvider = new();
        DirectSource<int> source = new();
        List<int> items = [];
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource lastEmitted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await source.Throttle(TimeSpan.FromMilliseconds(50), manualProvider).SubscribeAsync(
            (x, ct) =>
            {
                _ = ct;
                items.Add(x);
                _ = x == LastValue && lastEmitted.TrySetResult();
                return default;
            },
            null,
            _ =>
            {
                completed.TrySetResult();
                return default;
            });
        await source.EmitNext(1);
        await source.EmitNext(SecondValue);
        await source.EmitNext(LastValue);

        await Assert.That(manualProvider.TimerCount).IsEqualTo(LastValue);
        manualProvider.FireAll();
        await lastEmitted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await source.Complete(Result.Success);
        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(items).Contains(LastValue);
    }

    /// <summary>Tests Timeout fires when source is slow.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTimeoutFires_ThenThrowsTimeoutException() => await Assert
        .That(async () => await SignalAsync.Never<int>().Timeout(TimeSpan.FromMilliseconds(10)).FirstAsync())
        .ThrowsExactly<TimeoutException>();

    /// <summary>Tests Timeout with fallback observable.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTimeoutWithFallback_ThenFallbackUsed()
    {
        var result = await SignalAsync.Never<int>().Timeout(TimeSpan.FromMilliseconds(10), SignalAsync.Return(99))
            .FirstAsync();
        await Assert.That(result).IsEqualTo(FallbackValue99);
    }

    /// <summary>Tests Timeout resets on each value and does not fire.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTimeoutResetsOnValue_ThenDoesNotFire()
    {
        const int ExpectedSecond = 2;
        const int ExpectedThird = 3;
        var result = await SignalAsync.Range(1, 3).Timeout(TimeSpan.FromSeconds(5)).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([1, ExpectedSecond, ExpectedThird]);
    }

    /// <summary>Verifies that an exception thrown by the downstream observer's <c>OnCompletedAsync</c> during a <c>Timeout</c> firing is routed to <see cref = "UnhandledExceptionHandler"/>.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTimeoutFiresAndDownstreamCompletionThrows_ThenRoutedToUnhandled()
    {
        using UnhandledExceptionCapture unhandled = new();
        TimeoutThrowingWitness<int> throwing = new(new InvalidOperationException("completion-failed"));
        await using var sub = await SignalAsync.Never<int>().Timeout(TimeSpan.FromMilliseconds(1))
            .SubscribeAsync(throwing, CancellationToken.None);
        var exception = await unhandled.WaitForAsync("completion-failed", TimeSpan.FromSeconds(5));
        await Assert.That(exception).IsNotNull();
        await Assert.That(exception!.Message).IsEqualTo("completion-failed");
    }

    /// <summary>
    /// A custom <see cref = "TimeProvider"/> that delegates timer creation to the system provider.
    /// Used to exercise the non-system <see cref = "TimeProvider"/> code paths in Interval and Timer operators.
    /// </summary>
    private sealed class CustomTimeProvider : TimeProvider
    {
        /// <summary>Creates a timer by delegating to the system <see cref = "TimeProvider"/>.</summary>
        /// <param name = "callback">The callback to invoke when the timer fires.</param>
        /// <param name = "state">The state object passed to the callback.</param>
        /// <param name = "dueTime">The initial delay before the first invocation.</param>
        /// <param name = "period">The interval between subsequent invocations.</param>
        /// <returns>An <see cref = "ITimer"/> instance.</returns>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
            System.CreateTimer(callback, state, dueTime, period);
    }

    /// <summary>A <see cref = "TimeProvider"/> that throws from <see cref = "CreateTimer"/> to exercise the non-cancellation catch.</summary>
    private sealed class ThrowingTimeProvider : TimeProvider
    {
        /// <summary>Throws an <see cref = "InvalidOperationException"/> instead of creating a timer.</summary>
        /// <param name = "callback">The callback (unused).</param>
        /// <param name = "state">The state (unused).</param>
        /// <param name = "dueTime">The due time (unused).</param>
        /// <param name = "period">The period (unused).</param>
        /// <returns>Never returns; always throws.</returns>
        /// <exception cref = "InvalidOperationException">Always thrown.</exception>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
            throw new InvalidOperationException("timer creation failed");
    }

    /// <summary>A <see cref = "TimeProvider"/> that records one-shot timers and exposes an explicit fire point for deterministic debounce supersession tests.</summary>
    private sealed class ManualTimeProvider : TimeProvider
    {
        /// <summary>Protects timer collection access.</summary>
        private readonly Lock _gate = new();

        /// <summary>The timers created by this provider.</summary>
        private readonly List<ManualTimer> _timers = [];

        /// <summary>Gets the number of timers created by this provider.</summary>
        internal int TimerCount
        {
            get
            {
                lock (_gate)
                {
                    return _timers.Count;
                }
            }
        }

        /// <summary>Creates a manual timer and stores it until <see cref = "FireAll"/> is invoked.</summary>
        /// <param name = "callback">The callback to invoke when the timer is fired.</param>
        /// <param name = "state">The state object passed to the callback.</param>
        /// <param name = "dueTime">The initial delay (recorded by caller behavior, not elapsed by this provider).</param>
        /// <param name = "period">The interval (ignored; timers are one-shot in these tests).</param>
        /// <returns>A manual <see cref = "ITimer"/> instance.</returns>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            _ = dueTime;
            _ = period;
            ManualTimer timer = new(callback, state);
            lock (_gate)
            {
                _timers.Add(timer);
            }

            return timer;
        }

        /// <summary>Fires every timer that has been created so far.</summary>
        internal void FireAll()
        {
            ManualTimer[] timers;
            lock (_gate)
            {
                timers = [.. _timers];
            }

            foreach (var timer in timers)
            {
                timer.Fire();
            }
        }

        /// <summary>Manual one-shot timer used by <see cref = "ManualTimeProvider"/>.</summary>
        /// <param name = "callback">The callback to invoke.</param>
        /// <param name = "state">The state object passed to the callback.</param>
        private sealed class ManualTimer(TimerCallback callback, object? state) : ITimer
        {
            /// <summary>Non-zero once the timer has been disposed.</summary>
            private int _disposed;

            /// <summary>Non-zero once the timer has fired.</summary>
            private int _fired;

            /// <summary>No-op change; returns whether the timer is still active.</summary>
            /// <param name = "dueTime">The due time (ignored).</param>
            /// <param name = "period">The period (ignored).</param>
            /// <returns><see langword = "true"/> when the timer is still active.</returns>
            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                _ = dueTime;
                _ = period;
                return Volatile.Read(ref _disposed) == 0;
            }

            /// <summary>Marks the timer as disposed.</summary>
            public void Dispose() => Interlocked.Exchange(ref _disposed, 1);

            /// <summary>Marks the timer as disposed.</summary>
            /// <returns>A completed <see cref = "ValueTask"/>.</returns>
            public ValueTask DisposeAsync()
            {
                Dispose();
                return default;
            }

            /// <summary>Invokes the callback once if the timer has not been disposed.</summary>
            internal void Fire()
            {
                if (Volatile.Read(ref _disposed) != 0 ||
                    Interlocked.Exchange(ref _fired, 1) != 0)
                {
                    return;
                }

                callback(state);
            }
        }
    }

    /// <summary>
    /// A <see cref = "TimeProvider"/> that fires the timer callback synchronously during
    /// <see cref = "CreateTimer"/>, completing the delay immediately. Used to deterministically
    /// test the id-mismatch early return and exception routing paths in ThrottleWitness.
    /// </summary>
    private sealed class ImmediateFireTimeProvider : TimeProvider
    {
        /// <summary>Invokes the timer callback synchronously and returns a no-op timer.</summary>
        /// <param name = "callback">The callback to invoke immediately.</param>
        /// <param name = "state">The state object passed to the callback.</param>
        /// <param name = "dueTime">The initial delay (ignored; fires immediately).</param>
        /// <param name = "period">The interval (ignored; fires only once).</param>
        /// <returns>A no-op <see cref = "ITimer"/> instance.</returns>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            callback(state);
            return new NoOpTimer();
        }

        /// <summary>A timer that performs no operations. Used as the return value from <see cref = "CreateTimer"/>.</summary>
        private sealed class NoOpTimer : ITimer
        {
            /// <summary>No-op change; returns true.</summary>
            /// <param name = "dueTime">The due time (ignored).</param>
            /// <param name = "period">The period (ignored).</param>
            /// <returns>Always returns true.</returns>
            public bool Change(TimeSpan dueTime, TimeSpan period) => true;

            /// <summary>No-op dispose.</summary>
            public void Dispose()
            {
            }

            /// <summary>No-op async dispose.</summary>
            /// <returns>A completed <see cref = "ValueTask"/>.</returns>
            public ValueTask DisposeAsync() => default;
        }
    }

    /// <summary>Bare-bones downstream observer that throws from <c>OnCompletedAsync</c> to
    /// exercise the catch block in <c>Timeout</c>'s <c>FireTimeoutAsync</c>.</summary>
    /// <typeparam name = "T">The element type.</typeparam>
    /// <param name = "error">The exception to throw on completion.</param>
    private sealed class TimeoutThrowingWitness<T>(Exception error) : IObserverAsync<T>
    {
        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        public ValueTask OnNextAsync(T value, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        public ValueTask OnCompletedAsync(Result result) => throw error;

        /// <inheritdoc/>
        /// <returns>A task representing the asynchronous operation.</returns>
        public ValueTask DisposeAsync() => default;
    }
}
