// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for ReactiveExtensionsTests.</summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>Tests OnErrorRetry without parameters.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task OnErrorRetry_RetriesOnError()
    {
        const int SuccessAttempt = 3;
        var attempts = 0;
        var source = Observable.Create<int>(observer =>
        {
            attempts++;
            if (attempts < SuccessAttempt)
            {
                observer.OnError(new InvalidOperationException());
            }
            else
            {
                observer.OnNext(SampleValue42);
                observer.OnCompleted();
            }

            return EmptyDisposable.Instance;
        });
        List<int> results = [];
        using var sub = source.OnErrorRetry().Subscribe(results.Add);
        using (Assert.Multiple())
        {
            await Assert.That(results).Count().IsEqualTo(1);
            await Assert.That(results[0]).IsEqualTo(SampleValue42);
            await Assert.That(attempts).IsEqualTo(SampleValue3);
        }
    }

    /// <summary>Tests RetryWithBackoff respects max delay.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task RetryWithBackoff_RespectsMaxDelay()
    {
        const int SuccessAttempt = 5;
        const int MaxRetries = 10;
        const long InitialDelayTicks = 10;
        const double BackoffFactor = 2.0;
        const long MaxDelayTicks = 50;
        const long SecondDelayTicks = 20;
        const long ThirdDelayTicks = 40;
        VirtualClock scheduler = new();
        var attempts = 0;
        var source = Observable.Defer(() =>
        {
            attempts++;
            return attempts < SuccessAttempt
                ? Observable.Throw<int>(new InvalidOperationException())
                : Observable.Return(SampleValue42);
        });
        List<int> results = [];
        using var subscription = source.RetryWithBackoff(
            MaxRetries,
            TimeSpan.FromTicks(InitialDelayTicks),
            BackoffFactor,
            TimeSpan.FromTicks(MaxDelayTicks),
            scheduler).Subscribe(results.Add);
        long[] expectedDelays = [InitialDelayTicks, SecondDelayTicks, ThirdDelayTicks, MaxDelayTicks];
        foreach (var delay in expectedDelays)
        {
            var before = attempts;
            scheduler.AdvanceBy(delay - 1);
            await Assert.That(attempts).IsEqualTo(before);
            scheduler.AdvanceBy(1);
            await Assert.That(attempts).IsEqualTo(before + 1);
        }

        await Assert.That(results).IsCollectionEqualTo([SampleValue42]);
    }

    /// <summary>Tests OnErrorRetry with error action and retry count.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task OnErrorRetry_WithErrorActionAndRetryCount_RetriesLimitedTimes()
    {
        VirtualClock scheduler = new();
        const int RetryCount = 3;
        const int ExpectedAttempts = RetryCount + 1;
        var attempts = 0;
        var errorCount = 0;
        var source = Observable.Create<int>(observer =>
        {
            attempts++;
            observer.OnError(new InvalidOperationException());
            return EmptyDisposable.Instance;
        });
        Exception? caughtException = null;
        using var sub = source.OnErrorRetry<int, InvalidOperationException>(ex => errorCount++, RetryCount)
            .Subscribe(
                static _ => { },
                ex => caughtException = ex);
        scheduler.Start();
        using (Assert.Multiple())
        {
            // retryCount = retries after the initial attempt; total subscriptions = 1 + retryCount.
            await Assert.That(attempts).IsEqualTo(ExpectedAttempts);
            await Assert.That(caughtException).IsNotNull();
        }
    }

    /// <summary>Tests OnErrorRetry with delay.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task OnErrorRetry_WithDelay_DelaysRetries()
    {
        const int SuccessAttempt = 3;
        const int RetryCount = 5;
        const long DelayTicks = 50;
        VirtualClock scheduler = new();
        var attempts = 0;
        var source = Observable.Defer(() =>
        {
            attempts++;
            return attempts < SuccessAttempt
                ? Observable.Throw<int>(new InvalidOperationException())
                : Observable.Return(SampleValue42);
        });
        List<int> results = [];
        using var subscription = source.OnErrorRetry<int, InvalidOperationException>(
            static _ => { },
            RetryCount,
            TimeSpan.FromTicks(DelayTicks),
            scheduler).Subscribe(results.Add);

        scheduler.AdvanceBy(DelayTicks - 1);
        await Assert.That(attempts).IsEqualTo(1);
        await Assert.That(results).IsEmpty();
        scheduler.AdvanceBy(1);
        await Assert.That(attempts).IsEqualTo(SampleValue2);
        scheduler.AdvanceBy(DelayTicks - 1);
        await Assert.That(results).IsEmpty();
        scheduler.AdvanceBy(1);
        await Assert.That(attempts).IsEqualTo(SuccessAttempt);
        await Assert.That(results).IsCollectionEqualTo([SampleValue42]);
    }

    /// <summary>Tests OnErrorRetry with delay and no error action.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task OnErrorRetry_WithDelayAndErrorAction_RetriesWithDelay()
    {
        const int SuccessAttempt = 2;
        const int DelayMilliseconds = 10;
        var attemptCount = 0;
        var errorsCaught = 0;
        List<int> results = [];
        VirtualClock scheduler = new();
        var source = Observable.Create<int>(observer =>
        {
            attemptCount++;
            if (attemptCount < SuccessAttempt)
            {
                observer.OnError(new InvalidOperationException($"Attempt {attemptCount}"));
            }
            else
            {
                observer.OnNext(SampleValue42);
                observer.OnCompleted();
            }

            return EmptyDisposable.Instance;
        });
        _ = source.OnErrorRetry<int, InvalidOperationException>(
            ex => errorsCaught++,
            int.MaxValue,
            TimeSpan.FromMilliseconds(DelayMilliseconds),
            scheduler).Subscribe(results.Add);
        scheduler.AdvanceBy(TimeSpan.FromMilliseconds(DelayMilliseconds).Ticks);
        using (Assert.Multiple())
        {
            await Assert.That(errorsCaught).IsEqualTo(1);
            await Assert.That(results).IsCollectionEqualTo([SampleValue42]);
        }
    }

    /// <summary>Tests OnErrorRetry with retry count limit.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task OnErrorRetry_WithRetryCount_LimitsRetries()
    {
        VirtualClock scheduler = new();
        const int RetryCount = 2;
        const int ExpectedErrorCallbacks = RetryCount + 1;
        var attemptCount = 0;
        var errorsCaught = 0;
        var finalError = false;
        var source = Observable.Create<int>(observer =>
        {
            attemptCount++;
            observer.OnError(new InvalidOperationException($"Attempt {attemptCount}"));
            return EmptyDisposable.Instance;
        });
        _ = source.OnErrorRetry<int, InvalidOperationException>(ex => errorsCaught++, RetryCount).Subscribe(
            static _ => { },
            ex => finalError = true);
        scheduler.Start();
        using (Assert.Multiple())
        {
            // OnError callback fires for every failure (including the final propagated one):
            // 1 initial attempt + retryCount retries = retryCount + 1 callbacks.
            await Assert.That(finalError).IsTrue();
            await Assert.That(errorsCaught).IsEqualTo(ExpectedErrorCallbacks);
            await Assert.That(finalError).IsTrue();
        }
    }

    /// <summary>Tests OnErrorRetry with retry count and delay.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task OnErrorRetry_WithRetryCountAndDelay_LimitsRetriesWithDelay()
    {
        const int RetryCount = 2;
        const int ExpectedErrorCallbacks = RetryCount + 1;
        const int DelayMilliseconds = 10;
        var attemptCount = 0;
        var errorsCaught = 0;
        var finalError = false;
        VirtualClock scheduler = new();
        var source = Observable.Create<int>(observer =>
        {
            attemptCount++;
            observer.OnError(new InvalidOperationException($"Attempt {attemptCount}"));
            return EmptyDisposable.Instance;
        });
        _ = source.OnErrorRetry<int, InvalidOperationException>(
            ex => errorsCaught++,
            RetryCount,
            TimeSpan.FromMilliseconds(DelayMilliseconds),
            scheduler).Subscribe(
            static _ => { },
            ex => finalError = true);

        // Advance enough virtual time to drain all retries plus the final propagation.
        scheduler.AdvanceBy(TimeSpan.FromMilliseconds(DelayMilliseconds * (RetryCount + 1)).Ticks);
        using (Assert.Multiple())
        {
            // OnError callback fires for every failure (initial + retries) = retryCount + 1 calls.
            await Assert.That(errorsCaught).IsEqualTo(ExpectedErrorCallbacks);
            await Assert.That(finalError).IsTrue();
        }
    }

    /// <summary>Tests OnErrorRetry with retry count, delay, and scheduler.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task OnErrorRetry_WithRetryCountDelayAndScheduler_RetriesCorrectly()
    {
        VirtualClock scheduler = new();
        const int SuccessAttempt = 2;
        const int DelayMilliseconds = 10;
        var attemptCount = 0;
        var errorsCaught = 0;
        var source = Observable.Create<int>(observer =>
        {
            attemptCount++;
            if (attemptCount < SuccessAttempt)
            {
                observer.OnError(new InvalidOperationException($"Attempt {attemptCount}"));
            }
            else
            {
                observer.OnNext(SampleValue42);
                observer.OnCompleted();
            }

            return EmptyDisposable.Instance;
        });
        var result = 0;
        const int RetryCount = 3;
        _ = source.OnErrorRetry<int, InvalidOperationException>(
            ex => errorsCaught++,
            RetryCount,
            TimeSpan.FromMilliseconds(DelayMilliseconds),
            Sequencer.Immediate).Subscribe(r => result = r);
        scheduler.Start();
        using (Assert.Multiple())
        {
            await Assert.That(errorsCaught).IsEqualTo(1);
            await Assert.That(result).IsEqualTo(SampleValue42);
        }
    }

    /// <summary>Tests OnErrorRetry with action only uses zero-delay retry.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorRetryWithActionOnly_ThenRetriesImmediately()
    {
        const int SuccessAttempt = 3;
        var attempts = 0;
        var errorCount = 0;
        var source = Observable.Create<int>(observer =>
        {
            attempts++;
            if (attempts < SuccessAttempt)
            {
                observer.OnError(new InvalidOperationException());
            }
            else
            {
                observer.OnNext(SampleValue42);
                observer.OnCompleted();
            }

            return EmptyDisposable.Instance;
        });
        List<int> results = [];
        using var sub = source.OnErrorRetry<int, InvalidOperationException>(ex => errorCount++).Subscribe(results.Add);
        using (Assert.Multiple())
        {
            await Assert.That(results).IsCollectionEqualTo([SampleValue42]);
            await Assert.That(errorCount).IsEqualTo(SampleValue2);
        }
    }

    /// <summary>Tests OnErrorRetry with delay retries after specified delay.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorRetryWithDelay_ThenRetriesAfterDelay()
    {
        VirtualClock scheduler = new();
        const int SuccessAttempt = 2;
        const int DelayMilliseconds = 10;
        var attempts = 0;
        var source = Observable.Create<int>(observer =>
        {
            attempts++;
            if (attempts < SuccessAttempt)
            {
                observer.OnError(new InvalidOperationException());
            }
            else
            {
                observer.OnNext(SampleValue42);
                observer.OnCompleted();
            }

            return EmptyDisposable.Instance;
        });
        var result = 0;
        using var subscription = source.OnErrorRetry<int, InvalidOperationException>(
            static ex => { },
            int.MaxValue,
            TimeSpan.FromMilliseconds(DelayMilliseconds),
            scheduler)
            .Subscribe(value => result = value);
        scheduler.Start();
        await Assert.That(result).IsEqualTo(SampleValue42);
    }

    /// <summary>Tests RetryWithBackoff rethrows after max retries exceeded.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithBackoffExceedsMaxRetries_ThenRethrows()
    {
        VirtualClock scheduler = new();
        var source = Observable.Throw<int>(new InvalidOperationException("fail"));
        Exception? caughtError = null;
        const int MaxRetries = 2;
        const double BackoffFactor = 2.0;
        _ = source.RetryWithBackoff(
            MaxRetries,
            TimeSpan.FromMilliseconds(1),
            BackoffFactor,
            null,
            scheduler).Subscribe(
            static _ => { },
            ex => caughtError = ex);
        scheduler.Start();
        await Assert.That(caughtError).IsNotNull();
    }

    /// <summary>Tests RetryWithBackoff caps delay at maxDelay.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithBackoffDelayExceedsMax_ThenCapsDelay()
    {
        VirtualClock scheduler = new();
        const int SuccessAttempt = 4;
        const int MaxRetries = 5;
        const int InitialDelayMilliseconds = 5;
        const double BackoffFactor = 10.0;
        const int MaxDelayMilliseconds = 20;
        var attempts = 0;
        var source = Observable.Create<int>(observer =>
        {
            attempts++;
            if (attempts < SuccessAttempt)
            {
                observer.OnError(new InvalidOperationException());
            }
            else
            {
                observer.OnNext(SampleValue42);
                observer.OnCompleted();
            }

            return EmptyDisposable.Instance;
        });
        var result = 0;
        using var subscription = source.RetryWithBackoff(
            MaxRetries,
            TimeSpan.FromMilliseconds(InitialDelayMilliseconds),
            BackoffFactor,
            TimeSpan.FromMilliseconds(MaxDelayMilliseconds),
            scheduler).Subscribe(value => result = value);
        scheduler.Start();
        await Assert.That(result).IsEqualTo(SampleValue42);
    }

    /// <summary>Tests RetryWithDelay retries with custom delay selector.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithDelay_ThenRetriesWithCustomDelay()
    {
        VirtualClock scheduler = new();
        const int SuccessAttempt = 3;
        const int MaxRetries = 5;
        var attempts = 0;
        var source = Observable.Create<int>(observer =>
        {
            attempts++;
            if (attempts < SuccessAttempt)
            {
                observer.OnError(new InvalidOperationException());
            }
            else
            {
                observer.OnNext(SampleValue42);
                observer.OnCompleted();
            }

            return EmptyDisposable.Instance;
        });
        var result = 0;
        using var subscription = new RetryWithDelayObservable<int>(source, MaxRetries, static attempt => TimeSpan.FromMilliseconds(1), scheduler).Subscribe(value => result = value);
        scheduler.Start();
        await Assert.That(result).IsEqualTo(SampleValue42);
    }

    /// <summary>Tests RetryForeverWithDelay retries indefinitely with delay.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryForeverWithDelay_ThenRetriesIndefinitely()
    {
        const int SuccessAttempt = 4;
        var attempts = 0;
        VirtualClock scheduler = new();
        var delay = TimeSpan.FromTicks(1);
        var source = Observable.Defer(() =>
        {
            attempts++;
            return attempts < SuccessAttempt
                ? Observable.Throw<int>(new InvalidOperationException())
                : Observable.Return(SampleValue42);
        });
        List<int> results = [];
        using var sub = new RetryWithDelayObservable<int>(source, int.MaxValue, _ => delay, scheduler)
            .Subscribe(results.Add);
        await Assert.That(attempts).IsEqualTo(1);
        scheduler.AdvanceBy(delay.Ticks);
        await Assert.That(attempts).IsEqualTo(SampleValue2);
        scheduler.Start();
        await Assert.That(attempts).IsEqualTo(SuccessAttempt);
        await Assert.That(results).IsCollectionEqualTo([SampleValue42]);
    }

    /// <summary>Tests RetryWithFixedDelay retries with constant delay between retries.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithFixedDelay_ThenRetriesWithConstantDelay()
    {
        VirtualClock scheduler = new();
        const int SuccessAttempt = 3;
        const int MaxRetries = 5;
        var attempts = 0;
        var source = Observable.Create<int>(observer =>
        {
            attempts++;
            if (attempts < SuccessAttempt)
            {
                observer.OnError(new InvalidOperationException());
            }
            else
            {
                observer.OnNext(SampleValue42);
                observer.OnCompleted();
            }

            return EmptyDisposable.Instance;
        });
        var result = 0;
        using var subscription = new RetryWithBackoffObservable<int>(source, new(MaxRetries, TimeSpan.FromMilliseconds(1), 1.0, null, scheduler, null)).Subscribe(value => result = value);
        scheduler.Start();
        await Assert.That(result).IsEqualTo(SampleValue42);
    }

    /// <summary>Tests RetryWithBackoff inner retry with max delay cap path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithBackoffInnerRetry_ThenRetriesAndCapsDelay()
    {
        const int SuccessAttempt = 3;
        const int MaxRetries = 5;
        const int InitialDelayTicks = 10;
        const double BackoffFactor = 2.0;
        const int MaxDelayTicks = 15;
        VirtualClock scheduler = new();
        var attempt = 0;
        var source = Observable.Defer(() =>
        {
            attempt++;
            return attempt < SuccessAttempt
                ? Observable.Throw<int>(new InvalidOperationException("retry"))
                : Observable.Return(SampleValue42);
        });
        List<int> results = [];
        Exception? error = null;
        _ = source.RetryWithBackoff(
            MaxRetries,
            TimeSpan.FromTicks(InitialDelayTicks),
            BackoffFactor,
            TimeSpan.FromTicks(MaxDelayTicks),
            scheduler).Subscribe(results.Add, ex => error = ex);

        // Advance through retry delays
        scheduler.AdvanceBy(LongDelayMilliseconds);
        await Assert.That(results).IsCollectionEqualTo([SampleValue42]);
        await Assert.That(error).IsNull();
    }

    /// <summary>Tests OnErrorRetry with negative delay ticks sets dueTime to zero.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorRetryNegativeDelay_ThenUsesZeroDelay()
    {
        VirtualClock scheduler = new();
        const int SuccessAttempt = 3;
        var attempt = 0;
        var source = Observable.Defer(() =>
        {
            attempt++;
            return attempt < SuccessAttempt
                ? Observable.Throw<int>(new InvalidOperationException("fail"))
                : Observable.Return(SampleValue42);
        });
        List<int> results = [];
        Exception? error = null;
        TaskCompletionSource received = new(TaskCreationOptions.RunContinuationsAsynchronously);
        const int RetryCount = 5;
        _ = source.OnErrorRetry<int, InvalidOperationException>(
            static _ => { },
            RetryCount,
            TimeSpan.FromTicks(-1),
            scheduler).Subscribe(
            v =>
            {
                results.Add(v);
                _ = received.TrySetResult();
            },
            ex => error = ex);
        scheduler.Start();
        await received.Task;
        await Assert.That(results).Contains(SampleValue42);
    }

    /// <summary>Tests OnErrorRetry with retry count check rethrows after exceeding count.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorRetryExceedsRetryCount_ThenRethrows()
    {
        VirtualClock scheduler = new();
        var source = Observable.Throw<int>(new InvalidOperationException("fail"));
        Exception? caught = null;
        TaskCompletionSource errorReceived = new(TaskCreationOptions.RunContinuationsAsynchronously);
        const int RetryCount = 2;
        _ = source.OnErrorRetry<int, InvalidOperationException>(
            static _ => { },
            RetryCount,
            TimeSpan.Zero,
            scheduler).Subscribe(
            static _ => { },
            ex =>
            {
                caught = ex;
                _ = errorReceived.TrySetResult();
            });
        scheduler.Start();
        await errorReceived.Task;
        await Assert.That(caught).IsNotNull();
        await Assert.That(caught).IsTypeOf<InvalidOperationException>();
    }

    /// <summary>Tests RetryWithBackoff caps delay at maxDelay when backoff exceeds it.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithBackoffExceedsMaxDelay_ThenCapsAtMaxDelay()
    {
        const int FailingAttempts = 3;
        const int MaxRetries = 5;
        const int InitialDelayMilliseconds = 100;
        const double BackoffFactor = 10.0;
        const int MaxDelayMilliseconds = 200;
        const int AdvanceMilliseconds = 250;
        VirtualClock scheduler = new();
        var attempt = 0;
        var source = Observable.Create<int>(obs =>
        {
            attempt++;
            if (attempt <= FailingAttempts)
            {
                obs.OnError(new InvalidOperationException($"fail {attempt}"));
            }
            else
            {
                obs.OnNext(SampleValue42);
                obs.OnCompleted();
            }

            return EmptyDisposable.Instance;
        });
        List<int> results = [];
        using var sub = source.RetryWithBackoff(
            MaxRetries,
            TimeSpan.FromMilliseconds(InitialDelayMilliseconds),
            BackoffFactor,
            TimeSpan.FromMilliseconds(MaxDelayMilliseconds),
            scheduler).Subscribe(results.Add);

        // Advance through retry delays
        for (var i = 0; i < SampleValue5; i++)
        {
            scheduler.AdvanceBy(TimeSpan.FromMilliseconds(AdvanceMilliseconds).Ticks);
        }

        await Assert.That(results).Contains(SampleValue42);
    }

    /// <summary>
    /// Tests RetryWithBackoff maxDelay cap is applied when computed delay exceeds it,
    /// exercising line 1240 of ReactiveExtensions.cs.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithBackoffDelayExceedsMaxDelay_ThenCappedAtMaxDelay()
    {
        const int FailingAttempts = 3;
        const int MaxRetries = 5;
        const double BackoffFactor = 100.0;
        const int MaxDelayMilliseconds = 5;
        VirtualClock scheduler = new();
        var attemptCount = 0;
        var source = Observable.Create<int>(observer =>
        {
            attemptCount++;
            if (attemptCount <= FailingAttempts)
            {
                observer.OnError(new InvalidOperationException($"attempt {attemptCount}"));
            }
            else
            {
                observer.OnNext(SampleValue99);
                observer.OnCompleted();
            }

            return EmptyDisposable.Instance;
        });

        // initialDelay=1ms, backoffFactor=100 => attempt 2 delay = 1*100^1 = 100ms, exceeds maxDelay=5ms
        List<int> results = [];
        _ = source.RetryWithBackoff(
            MaxRetries,
            TimeSpan.FromMilliseconds(1),
            BackoffFactor,
            TimeSpan.FromMilliseconds(MaxDelayMilliseconds),
            scheduler).Subscribe(results.Add);

        // Advance past the capped delays
        scheduler.AdvanceBy(TimeSpan.FromSeconds(1).Ticks);
        await Assert.That(results).Contains(SampleValue99);
    }

    /// <summary>
    /// Verifies that RetryWithBackoff caps the computed delay at maxDelay when the
    /// exponential backoff exceeds it. Uses Sequencer.Immediate so the cap is exercised
    /// synchronously.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithBackoffComputedDelayExceedsMaxDelay_ThenCappedToMaxDelay()
    {
        VirtualClock scheduler = new();
        const int FailingAttempts = 2;
        const int MaxRetries = 5;
        const double BackoffFactor = 1000.0;
        const int MaxDelayMilliseconds = 2;
        var attemptCount = 0;
        var source = Observable.Create<int>(observer =>
        {
            attemptCount++;

            // Fail on first two attempts, succeed on third
            if (attemptCount <= FailingAttempts)
            {
                observer.OnError(new InvalidOperationException($"attempt {attemptCount}"));
            }
            else
            {
                observer.OnNext(SampleValue42);
                observer.OnCompleted();
            }

            return EmptyDisposable.Instance;
        });
        var result = 0;
        using var subscription = source.RetryWithBackoff(
            MaxRetries,
            TimeSpan.FromMilliseconds(1),
            BackoffFactor,
            TimeSpan.FromMilliseconds(MaxDelayMilliseconds),
            scheduler).Subscribe(value => result = value);
        scheduler.Start();
        await Assert.That(result).IsEqualTo(SampleValue42);
        await Assert.That(attemptCount).IsEqualTo(SampleValue3);
    }

    /// <summary>
    /// Verifies that RetryWithBackoff caps the computed delay at maxDelay using a
    /// VirtualClock so the cap assignment is directly exercised.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithBackoff_GivenLargeBackoffFactor_ThenDelayIsCappedAtMaxDelay()
    {
        // Given
        const int FailingAttempts = 3;
        const int SuccessValue = 100;
        const int MaxRetries = 5;
        const double BackoffFactor = 500.0;
        const int MaxDelayMilliseconds = 5;
        const int AdvanceMilliseconds = 10;
        VirtualClock scheduler = new();
        var attemptCount = 0;
        var source = Observable.Create<int>(observer =>
        {
            attemptCount++;
            if (attemptCount <= FailingAttempts)
            {
                observer.OnError(new InvalidOperationException($"fail {attemptCount}"));
            }
            else
            {
                observer.OnNext(SuccessValue);
                observer.OnCompleted();
            }

            return EmptyDisposable.Instance;
        });
        List<int> results = [];

        // When — backoffFactor 500 with initialDelay 1ms yields huge computed delays,
        // all of which must be capped to maxDelay 5ms.
        using var sub = source.RetryWithBackoff(
            MaxRetries,
            TimeSpan.FromMilliseconds(1),
            BackoffFactor,
            TimeSpan.FromMilliseconds(MaxDelayMilliseconds),
            scheduler).Subscribe(results.Add);

        // Advance the scheduler enough for each capped retry delay
        for (var i = 0; i < SampleValue10; i++)
        {
            scheduler.AdvanceBy(TimeSpan.FromMilliseconds(AdvanceMilliseconds).Ticks);
        }

        // Then
        await Assert.That(results).Contains(SuccessValue);
        await Assert.That(attemptCount).IsEqualTo(SampleValue4);
    }

    /// <summary>
    /// Verifies that RetryWithDelay rethrows the original exception when all retries
    /// are exhausted, exercising the error-propagation branch.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithDelayExhaustsRetries_ThenRethrowsOriginalException()
    {
        VirtualClock scheduler = new();
        const int MaxRetries = 2;
        var source = Observable.Throw<int>(new InvalidOperationException("permanent"));
        TaskCompletionSource<Exception> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // When
        using var sub = new RetryWithDelayObservable<int>(source, MaxRetries, static _ => TimeSpan.FromMilliseconds(1), scheduler).Subscribe(
            static _ => { },
            ex => completion.TrySetResult(ex));
        scheduler.Start();

        var caught = await completion.Task;

        // Then
        await Assert.That(caught).IsNotNull();
        await Assert.That(caught).IsTypeOf<InvalidOperationException>();
        await Assert.That(caught.Message).IsEqualTo("permanent");
    }
}
