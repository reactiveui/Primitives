// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests timer-controlled emissions, deadlines, and cancellation.</summary>
public class TimeBasedOperatorTests
{
    /// <summary>The resumable source failure message.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>The failure message raised by an observer that rejects a forwarded element.</summary>
    private const string ObserverErrorMessage = "observer failed";

    /// <summary>The virtual delay requested by the operators.</summary>
    private static readonly TimeSpan Window = TimeSpan.FromSeconds(1);

    /// <summary>Only the latest value survives when all pending debounce callbacks run.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenThrottleReceivesRapidValues_ThenOnlyEmitsLatest()
    {
        const int SecondValue = 2;
        const int ThirdValue = 3;
        ManualTimeProvider time = new();
        List<int> values = [];
        CallbackWitnessAsync<int> observer = new((value, _) =>
        {
            values.Add(value);
            return default;
        });
        await using SignalAsyncExtensions.ThrottleSignal<int>.ThrottleWitness witness = new(observer, Window, time);
        var firstPending = witness.StartDelayAsync(1, CancellationToken.None);
        var secondPending = witness.StartDelayAsync(SecondValue, CancellationToken.None);
        var thirdPending = witness.StartDelayAsync(ThirdValue, CancellationToken.None);
        var first = await time.NextTimerAsync();
        var second = await time.NextTimerAsync();
        var third = await time.NextTimerAsync();
        await Assert.That(values).IsEmpty();
        await Assert.That(first.DueTime).IsEqualTo(Window);
        first.Fire();
        await firstPending;
        second.Fire();
        await secondPending;
        third.Fire();
        await thirdPending;
        await Assert.That(values).IsCollectionEqualTo([ThirdValue]);
    }

    /// <summary>Each quiet period forwards its own value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenThrottleWithSpacedItems_ThenAllAreEmitted()
    {
        const int SecondValue = 2;
        ManualTimeProvider time = new();
        List<int> values = [];
        CallbackWitnessAsync<int> observer = new((value, _) =>
        {
            values.Add(value);
            return default;
        });
        await using SignalAsyncExtensions.ThrottleSignal<int>.ThrottleWitness witness = new(observer, Window, time);
        await time.RunAsync(witness.StartDelayAsync(1, CancellationToken.None));
        await time.RunAsync(witness.StartDelayAsync(SecondValue, CancellationToken.None));
        await Assert.That(values).IsCollectionEqualTo([1, SecondValue]);
    }

    /// <summary>Errors, completion, and disposal invalidate pending debounce identifiers.</summary>
    /// <param name="terminal">The terminal transition to apply.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments("error")]
    [Arguments("completion")]
    [Arguments("dispose")]
    public async Task WhenThrottleTerminated_ThenPendingValueIsDiscarded(string terminal)
    {
        ManualTimeProvider time = new();
        List<int> values = [];
        List<Exception> errors = [];
        List<Result> completions = [];
        CallbackWitnessAsync<int> observer = new(
            (value, _) =>
        {
            values.Add(value);
            return default;
        },
            (error, _) =>
        {
            errors.Add(error);
            return default;
        },
            result =>
        {
            completions.Add(result);
            return default;
        });
        await using SignalAsyncExtensions.ThrottleSignal<int>.ThrottleWitness witness = new(observer, Window, time);
        var pending = witness.FireAfterDelayAsync(1, 0, CancellationToken.None);
        InvalidOperationException error = new(SourceErrorMessage);
        if (terminal == "error")
        {
            await witness.OnErrorResumeAsync(error, CancellationToken.None);
            await Assert.That(errors).Count().IsEqualTo(1);
            await Assert.That(errors[0]).IsSameReferenceAs(error);
        }
        else if (terminal == "completion")
        {
            await witness.OnCompletedAsync(Result.Success);
            await Assert.That(completions).Count().IsEqualTo(1);
        }
        else
        {
            await witness.DisposeAsync();
        }

        await time.RunAsync(pending);
        await Assert.That(values).IsEmpty();
    }

    /// <summary>Observer failures from delayed emission reach the unhandled exception handler.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenThrottleOnNextThrows_ThenRoutedToUnhandledExceptionHandler()
    {
        ManualTimeProvider time = new();
        using UnhandledExceptionCapture capture = new();
        CallbackWitnessAsync<int> observer = new(static (_, _) => throw new InvalidOperationException(ObserverErrorMessage));
        await using SignalAsyncExtensions.ThrottleSignal<int>.ThrottleWitness witness = new(observer, Window, time);
        await time.RunAsync(witness.FireAfterDelayAsync(1, 0, CancellationToken.None));
        var exception = await capture.WaitForAsync(ObserverErrorMessage);
        await Assert.That(exception).IsTypeOf<InvalidOperationException>();
    }

    /// <summary>Cancellation of a pending debounce delay does not report an unhandled failure.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenThrottleDelayCancelled_ThenNoValueIsForwarded()
    {
        ManualTimeProvider time = new();
        using CancellationTokenSource cancellation = new();
        List<int> values = [];
        CallbackWitnessAsync<int> observer = new((value, _) =>
        {
            values.Add(value);
            return default;
        });
        await using SignalAsyncExtensions.ThrottleSignal<int>.ThrottleWitness witness = new(observer, Window, time);
        var pending = witness.FireAfterDelayAsync(1, 0, cancellation.Token);
        await cancellation.CancelAsync();
        await pending;
        await Assert.That(values).IsEmpty();
    }

    /// <summary>An element stays pending until its registered delay fires.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenDelay_ThenElementsAreTimeShifted()
    {
        const int ExpectedValue = 42;
        ManualTimeProvider time = new();
        var result = SignalAsync.Return(ExpectedValue).Delay(Window, time).FirstAsync().AsTask();
        var timer = await time.NextTimerAsync();
        await Assert.That(result.IsCompleted).IsFalse();
        await Assert.That(timer.DueTime).IsEqualTo(Window);
        timer.Fire();
        await Assert.That(await result).IsEqualTo(ExpectedValue);
    }

    /// <summary>Every element requests its own delay and preserves source order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenDelaySequence_ThenAllElementsDelayed()
    {
        const int ThirdValue = 3;
        const int SecondValue = 2;
        ManualTimeProvider time = new();
        var result = await time.RunAsync(SignalAsync.Range(1, ThirdValue).Delay(Window, time).ToListAsync().AsTask());
        await Assert.That(result).IsCollectionEqualTo([1, SecondValue, ThirdValue]);
    }

    /// <summary>A zero delay preserves the original source instance.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenDelayZero_ThenNoDelay()
    {
        const int ExpectedValue = 42;
        var source = SignalAsync.Return(ExpectedValue);
        await Assert.That(source.Delay(TimeSpan.Zero)).IsSameReferenceAs(source);
        await Assert.That(await source.Delay(TimeSpan.Zero).FirstAsync()).IsEqualTo(ExpectedValue);
    }

    /// <summary>Resumable errors pass through delay without creating a timer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenDelaySourceEmitsErrorResume_ThenErrorForwarded()
    {
        ManualTimeProvider time = new();
        var source = Signal.Create<int>();
        Exception? actual = null;
        await using var subscription = await source.Values.Delay(Window, time).SubscribeAsync(static (_, _) => default, (error, _) =>
        {
            actual = error;
            return default;
        });
        InvalidOperationException expected = new(SourceErrorMessage);
        await source.OnErrorResumeAsync(expected, CancellationToken.None);
        await Assert.That(actual).IsSameReferenceAs(expected);
    }

    /// <summary>A source that completes before its deadline preserves its values.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenTimeoutNotExceeded_ThenCompletesNormally()
    {
        const int ThirdValue = 3;
        const int SecondValue = 2;
        ManualTimeProvider time = new();
        var values = await SignalAsync.Range(1, ThirdValue).Timeout(Window, time).ToListAsync();
        await Assert.That(values).IsCollectionEqualTo([1, SecondValue, ThirdValue]);
    }

    /// <summary>Firing the deadline fails a source that has not emitted.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenTimeoutExceeded_ThenThrowsTimeoutException()
    {
        ManualTimeProvider time = new();
        var pending = SignalAsync.Never<int>().Timeout(Window, time).FirstAsync().AsTask();
        await Assert.That(pending.IsCompleted).IsFalse();
        await time.FireNextAsync();
        await Assert.That(() => pending).ThrowsExactly<TimeoutException>();
    }

    /// <summary>A fired deadline subscribes to the fallback source.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenTimeoutWithFallback_ThenSwitchesToFallback()
    {
        const int FallbackValue = 99;
        ManualTimeProvider time = new();
        var pending = SignalAsync.Never<int>().Timeout(Window, SignalAsync.Return(FallbackValue), time).FirstAsync().AsTask();
        await time.FireNextAsync();
        await Assert.That(await pending).IsEqualTo(FallbackValue);
    }

    /// <summary>A source failure other than the deadline is propagated instead of switching to the fallback.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenTimeoutWithFallbackSourceFails_ThenFailurePropagates()
    {
        const int FallbackValue = 99;
        ManualTimeProvider time = new();
        var failing = SignalAsync.Create<int>(static async (observer, _) =>
        {
            await observer.OnCompletedAsync(Result.Failure(new InvalidOperationException(SourceErrorMessage)));
            return DisposableAsync.Empty;
        });

        await Assert.That(async () => await failing.Timeout(Window, SignalAsync.Return(FallbackValue), time).FirstAsync())
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Each value rearms the same deadline timer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenTimeoutResetsOnValue_ThenDoesNotFire()
    {
        const int SecondValue = 2;
        ManualTimeProvider time = new();
        var source = Signal.Create<int>();
        List<int> values = [];
        await using var subscription = await source.Values.Timeout(Window, time).SubscribeAsync((value, _) =>
        {
            values.Add(value);
            return default;
        });
        var timer = await time.NextTimerAsync();
        await source.OnNextAsync(1, CancellationToken.None);
        await Assert.That(timer.DueTime).IsEqualTo(Window);
        await source.OnNextAsync(SecondValue, CancellationToken.None);
        await Assert.That(timer.DueTime).IsEqualTo(Window);
        await source.OnCompletedAsync(Result.Success);
        await Assert.That(timer.DueTime).IsEqualTo(Timeout.InfiniteTimeSpan);
        timer.Fire();
        await Assert.That(values).IsCollectionEqualTo([1, SecondValue]);
    }

    /// <summary>A resumable source error disables the deadline and reaches the observer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenTimeoutSourceEmitsErrorResume_ThenForwardsAndCancelsTimer()
    {
        ManualTimeProvider time = new();
        var source = Signal.Create<int>();
        Exception? actual = null;
        await using var subscription = await source.Values.Timeout(Window, time).SubscribeAsync(static (_, _) => default, (error, _) =>
        {
            actual = error;
            return default;
        });
        var timer = await time.NextTimerAsync();
        InvalidOperationException expected = new(SourceErrorMessage);
        await source.OnErrorResumeAsync(expected, CancellationToken.None);
        await Assert.That(actual).IsSameReferenceAs(expected);
        await Assert.That(timer.DueTime).IsEqualTo(Timeout.InfiniteTimeSpan);
    }

    /// <summary>Deadline completion failures reach the unhandled exception handler.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenTimeoutOnCompletedThrows_ThenRoutedToUnhandledExceptionHandler()
    {
        ManualTimeProvider time = new();
        using UnhandledExceptionCapture capture = new();
        await using var subscription = await SignalAsync.Never<int>().Timeout(Window, time).SubscribeAsync(
            new ThrowingCompletionWitness(),
            CancellationToken.None);
        await time.FireNextAsync();
        await Assert.That(await capture.WaitForAsync("completion failed")).IsTypeOf<InvalidOperationException>();
    }

    /// <summary>Failure to create a deadline timer is reported without failing subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenTimeoutDelayThrowsNonCancellation_ThenRoutedToUnhandledExceptionHandler()
    {
        using UnhandledExceptionCapture capture = new();
        await using var subscription = await SignalAsync.Never<int>().Timeout(Window, new ThrowingTimeProvider()).SubscribeAsync(static (_, _) => default);
        await Assert.That(await capture.WaitForAsync("timer creation failed")).IsTypeOf<InvalidOperationException>();
    }

    /// <summary>Periodic timers emit zero-based ticks only when their delays are fired.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenPeriodicTimerEmitsMultipleTicks_ThenAllTicksReceived()
    {
        const int ThirdValue = 3;
        const long SecondTick = 2L;
        ManualTimeProvider time = new();
        var pending = SignalAsync.Timer(Window, Window, time).Take(ThirdValue).ToListAsync().AsTask();
        var values = await time.RunAsync(pending);
        await Assert.That(values).IsCollectionEqualTo([0L, 1L, SecondTick]);
    }

    /// <summary>Disposing a pending timer prevents the registered callback from emitting.</summary>
    /// <param name="interval">Whether to use the interval factory.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WhenPeriodicTimerCancelled_ThenStopsEmitting(bool interval)
    {
        ManualTimeProvider time = new();
        List<long> values = [];
        var source = interval ? SignalAsync.Interval(Window, time) : SignalAsync.Timer(Window, Window, time);
        var subscription = await source.SubscribeAsync((value, _) =>
        {
            values.Add(value);
            return default;
        });
        var timer = await time.NextTimerAsync();
        await subscription.DisposeAsync();
        timer.Fire();
        await Assert.That(values).IsEmpty();
    }

    /// <summary>Cancellation completes a pending periodic delay and suppresses its registered callback.</summary>
    /// <param name="interval">Whether to use the interval subscription.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WhenPeriodicSubscriptionCancelled_ThenPendingTickIsSuppressed(bool interval)
    {
        ManualTimeProvider time = new();
        using CancellationTokenSource cancellation = new();
        List<long> values = [];
        CallbackWitnessAsync<long> observer = new((value, _) =>
        {
            values.Add(value);
            return default;
        });
        ITaskSignalJob<long> job = interval
            ? new IntervalSubscription(observer, Window, time)
            : new TimerSubscription(observer, Window, Window, time);
        var execution = TaskSignalState.ExecuteAsync(job, observer, cancellation.Token).AsTask();
        await time.FireNextAsync();
        var pendingTimer = await time.NextTimerAsync();
        await cancellation.CancelAsync();
        await execution;
        pendingTimer.Fire();
        await Assert.That(values).IsCollectionEqualTo([interval ? 1L : 0L]);
    }

    /// <summary>Intervals emit consecutive values starting at one.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenIntervalWithNonSystemTimeProvider_ThenUsesTimerPath()
    {
        const int SecondValue = 2;
        const long SecondTick = 2L;
        ManualTimeProvider time = new();
        var values = await time.RunAsync(SignalAsync.Interval(Window, time).Take(SecondValue).ToListAsync().AsTask());
        await Assert.That(values).IsCollectionEqualTo([1L, SecondTick]);
    }

    /// <summary>The default-provider overload constructs a throttle signal without starting a timer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenThrottleUsesDefaultProvider_ThenCreatesThrottleSignal() =>
        await Assert.That(SignalAsync.Return(1).Throttle(Window)).IsTypeOf<SignalAsyncExtensions.ThrottleSignal<int>>();

    /// <summary>A null provider falls back to the system provider.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenThrottleProviderNull_ThenCreatesThrottleSignal() =>
        await Assert.That(SignalAsync.Return(1).Throttle(Window, null)).IsTypeOf<SignalAsyncExtensions.ThrottleSignal<int>>();

    /// <summary>A null provider falls back to the system provider for timers, deadlines and delays.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenTimeProviderNull_ThenOperatorsUseSystemProvider()
    {
        var source = SignalAsync.Return(1);

        await Assert.That(SignalAsync.Timer(Window, (TimeProvider?)null)).IsTypeOf<TimerSignal>();
        await Assert.That(SignalAsync.Timer(Window, Window, null)).IsTypeOf<TimerSignal>();
        await Assert.That(source.Timeout(Window, (TimeProvider?)null)).IsTypeOf<SignalAsyncExtensions.TimeoutSignal<int>>();
        await Assert.That(source.Timeout(Window, source, null)).IsTypeOf<SignalAsyncExtensions.TimeoutWithFallbackSignal<int>>();
        await Assert.That(source.Delay(Window, (TimeProvider?)null)).IsTypeOf<SignalAsyncExtensions.DelaySignal<int>>();
        await Assert.That(source.Delay(Window)).IsTypeOf<SignalAsyncExtensions.DelaySignal<int>>();
    }

    /// <summary>A zero delay returns the source unchanged.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenDelayIsZero_ThenReturnsSource()
    {
        ManualTimeProvider time = new();
        var source = SignalAsync.Return(1);

        await Assert.That(source.Delay(TimeSpan.Zero)).IsSameReferenceAs(source);
        await Assert.That(source.Delay(TimeSpan.Zero, time)).IsSameReferenceAs(source);
        await Assert.That(source.Shift(TimeSpan.Zero)).IsSameReferenceAs(source);
    }

    /// <summary>Negative debounce intervals are rejected.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void WhenThrottleNegativeDueTime_ThenThrowsArgumentOutOfRange() =>
        Assert.Throws<ArgumentOutOfRangeException>(static () => SignalAsync.Return(1).Throttle(TimeSpan.FromTicks(-1)));

    /// <summary>A tick forwards the newest element held since the previous one.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenProbeReceivesRapidValues_ThenTickForwardsLatest()
    {
        const int SecondValue = 2;
        const int ThirdValue = 3;
        ManualTimeProvider time = new();
        List<int> values = [];
        CallbackWitnessAsync<int> observer = new((value, _) =>
        {
            values.Add(value);
            return default;
        });
        await using SignalAsyncExtensions.ProbeSignal<int>.ProbeWitness witness = new(observer, Window, time);
        var pending = witness.HoldAsync(1, CancellationToken.None);
        await witness.HoldAsync(SecondValue, CancellationToken.None);
        await witness.HoldAsync(ThirdValue, CancellationToken.None);
        var timer = await time.NextTimerAsync();
        await Assert.That(time.PendingTimerCount).IsEqualTo(0);
        await Assert.That(timer.DueTime).IsEqualTo(Window);
        await Assert.That(values).IsEmpty();
        timer.Fire();
        await pending;
        await Assert.That(values).IsCollectionEqualTo([ThirdValue]);
    }

    /// <summary>A tick with nothing held forwards nothing and frees the timer for the next element.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenProbeTickHasNothingHeld_ThenNothingIsForwarded()
    {
        const int SecondValue = 2;
        ManualTimeProvider time = new();
        List<int> values = [];
        CallbackWitnessAsync<int> observer = new((value, _) =>
        {
            values.Add(value);
            return default;
        });
        await using SignalAsyncExtensions.ProbeSignal<int>.ProbeWitness witness = new(observer, Window, time);
        await time.RunAsync(witness.TickAfterPeriodAsync(CancellationToken.None));
        await Assert.That(values).IsEmpty();
        await time.RunAsync(witness.HoldAsync(SecondValue, CancellationToken.None));
        await Assert.That(values).IsCollectionEqualTo([SecondValue]);
    }

    /// <summary>An element held when the source completes is forwarded ahead of the completion.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenProbeCompletesWithHeldValue_ThenValuePrecedesCompletion()
    {
        ManualTimeProvider time = new();
        List<string> notifications = [];
        CallbackWitnessAsync<int> observer = new(
            (value, _) =>
        {
            notifications.Add($"next {value}");
            return default;
        },
            static (_, _) => default,
            result =>
        {
            notifications.Add($"completed {result.IsSuccess}");
            return default;
        });
        await using SignalAsyncExtensions.ProbeSignal<int>.ProbeWitness witness = new(observer, Window, time);
        _ = witness.HoldAsync(1, CancellationToken.None);
        await witness.OnCompletedAsync(Result.Success);
        await Assert.That(notifications).IsCollectionEqualTo(["next 1", "completed True"]);
    }

    /// <summary>Completion with nothing held forwards only the completion.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenProbeCompletesWithNothingHeld_ThenOnlyCompletionIsForwarded()
    {
        ManualTimeProvider time = new();
        List<int> values = [];
        List<Result> completions = [];
        CallbackWitnessAsync<int> observer = new(
            (value, _) =>
        {
            values.Add(value);
            return default;
        },
            static (_, _) => default,
            result =>
        {
            completions.Add(result);
            return default;
        });
        await using SignalAsyncExtensions.ProbeSignal<int>.ProbeWitness witness = new(observer, Window, time);
        await witness.OnCompletedAsync(Result.Success);
        await Assert.That(values).IsEmpty();
        await Assert.That(completions).Count().IsEqualTo(1);
    }

    /// <summary>Disposal drops the held element so a later tick forwards nothing.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenProbeDisposed_ThenHeldValueIsDiscarded()
    {
        ManualTimeProvider time = new();
        List<int> values = [];
        CallbackWitnessAsync<int> observer = new((value, _) =>
        {
            values.Add(value);
            return default;
        });
        SignalAsyncExtensions.ProbeSignal<int>.ProbeWitness witness = new(observer, Window, time);
        var pending = witness.HoldAsync(1, CancellationToken.None);
        await witness.DisposeAsync();
        await time.RunAsync(pending);
        await Assert.That(values).IsEmpty();
    }

    /// <summary>Resumable errors reach the downstream observer unchanged.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenProbeSourceEmitsErrorResume_ThenErrorForwarded()
    {
        ManualTimeProvider time = new();
        List<Exception> errors = [];
        CallbackWitnessAsync<int> observer = new(
            static (_, _) => default,
            (error, _) =>
        {
            errors.Add(error);
            return default;
        });
        await using SignalAsyncExtensions.ProbeSignal<int>.ProbeWitness witness = new(observer, Window, time);
        InvalidOperationException expected = new(SourceErrorMessage);
        await witness.OnErrorResumeAsync(expected, CancellationToken.None);
        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>Observer failures raised by a tick reach the unhandled exception handler.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenProbeOnNextThrows_ThenRoutedToUnhandledExceptionHandler()
    {
        ManualTimeProvider time = new();
        using UnhandledExceptionCapture capture = new();
        CallbackWitnessAsync<int> observer = new(static (_, _) => throw new InvalidOperationException(ObserverErrorMessage));
        await using SignalAsyncExtensions.ProbeSignal<int>.ProbeWitness witness = new(observer, Window, time);
        await time.RunAsync(witness.HoldAsync(1, CancellationToken.None));
        var exception = await capture.WaitForAsync(ObserverErrorMessage);
        await Assert.That(exception).IsTypeOf<InvalidOperationException>();
    }

    /// <summary>A subscribed source samples through to the downstream observer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenSampleSubscribed_ThenForwardsHeldValueOnCompletion()
    {
        const int SecondValue = 2;
        ManualTimeProvider time = new();
        List<int> values = [];
        var source = Signal.Create<int>();
        await using var subscription = await source.Values.Sample(Window, time).SubscribeAsync((value, _) =>
        {
            values.Add(value);
            return default;
        });
        await source.OnNextAsync(1, CancellationToken.None);
        await source.OnNextAsync(SecondValue, CancellationToken.None);
        await source.OnCompletedAsync(Result.Success);
        await Assert.That(values).IsCollectionEqualTo([SecondValue]);
    }

    /// <summary>Sampling without a provider uses the system clock.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenProbeWithoutTimeProvider_ThenUsesSystemClock()
    {
        var source = SignalAsync.Return(1);
        await Assert.That(source.Probe(Window)).IsTypeOf<SignalAsyncExtensions.ProbeSignal<int>>();
        await Assert.That(source.Sample(Window)).IsTypeOf<SignalAsyncExtensions.ProbeSignal<int>>();
        await Assert.That(source.Sample(Window, null)).IsTypeOf<SignalAsyncExtensions.ProbeSignal<int>>();
    }

    /// <summary>Negative sampling periods are rejected.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void WhenProbeNegativePeriod_ThenThrowsArgumentOutOfRange() =>
        Assert.Throws<ArgumentOutOfRangeException>(static () => SignalAsync.Return(1).Probe(TimeSpan.FromTicks(-1)));

    /// <summary>Negative sampling intervals are rejected by the alias.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void WhenSampleNegativeInterval_ThenThrowsArgumentOutOfRange() =>
        Assert.Throws<ArgumentOutOfRangeException>(static () => SignalAsync.Return(1).Sample(TimeSpan.FromTicks(-1)));

    /// <summary>Negative delays are rejected.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void WhenDelayNegative_ThenThrowsArgumentOutOfRange() =>
        Assert.Throws<ArgumentOutOfRangeException>(static () => SignalAsync.Return(1).Delay(TimeSpan.FromTicks(-1)));

    /// <summary>Deadlines require a positive interval.</summary>
    /// <param name="ticks">The invalid interval in ticks.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    [Arguments(0L)]
    [Arguments(-1L)]
    public void WhenTimeoutNonPositive_ThenThrowsArgumentOutOfRange(long ticks) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => SignalAsync.Return(1).Timeout(TimeSpan.FromTicks(ticks)));

    /// <summary>A fallback observable is required by the fallback overload.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [Test]
    public void WhenTimeoutWithFallbackNull_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(static () => SignalAsync.Return(1).Timeout(Window, (IObservableAsync<int>)null!));

    /// <summary>Throws directly from completion without an observer wrapper catching it.</summary>
    private sealed class ThrowingCompletionWitness : IObserverAsync<int>
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnNextAsync(int value, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask OnErrorResumeAsync(Exception error, CancellationToken cancellationToken) => default;

        /// <inheritdoc/>
        public ValueTask OnCompletedAsync(Result result) => throw new InvalidOperationException("completion failed");

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask DisposeAsync() => default;
    }

    /// <summary>A provider that rejects timer creation.</summary>
    private sealed class ThrowingTimeProvider : TimeProvider
    {
        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
            throw new InvalidOperationException("timer creation failed");
    }
}
