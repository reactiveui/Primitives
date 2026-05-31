// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Disposables;
using System.Reactive.Linq;
using ReactiveUI.Primitives.Async.Tests;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for ReactiveExtensionsTests.</summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>
    /// Tests OnErrorRetry without parameters.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task OnErrorRetry_RetriesOnError()
    {
        var attempts = 0;
        var source = Observable.Create<int>(observer =>
        {
            attempts++;
            if (attempts < 3)
            {
                observer.OnError(new InvalidOperationException());
            }
            else
            {
                observer.OnNext(42);
                observer.OnCompleted();
            }

            return Disposable.Empty;
        });

        var results = new List<int>();
        using var sub = source.OnErrorRetry().Subscribe(results.Add);

        using (Assert.Multiple())
        {
            await Assert.That(results).Count().IsEqualTo(1);
            await Assert.That(results[0]).IsEqualTo(SampleValue42);
            await Assert.That(attempts).IsEqualTo(SampleValue3);
        }
    }

    /// <summary>
    /// Tests RetryWithBackoff respects max delay.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task RetryWithBackoff_RespectsMaxDelay()
    {
        var attempts = 0;
        var source = Observable.Create<int>(observer =>
        {
            attempts++;
            if (attempts < 5)
            {
                observer.OnError(new InvalidOperationException());
            }
            else
            {
                observer.OnNext(42);
                observer.OnCompleted();
            }

            return Disposable.Empty;
        });

        var result = source.RetryWithBackoff(
            maxRetries: 10,
            initialDelay: TimeSpan.FromMilliseconds(10),
            backoffFactor: 2.0,
            maxDelay: TimeSpan.FromMilliseconds(50),
            scheduler: null)
            .Wait();

        await Assert.That(result).IsEqualTo(SampleValue42);
    }

    /// <summary>
    /// Tests OnErrorRetry with error action and retry count.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task OnErrorRetry_WithErrorActionAndRetryCount_RetriesLimitedTimes()
    {
        const int RetryCount = 3;
        const int ExpectedAttempts = RetryCount + 1;
        var attempts = 0;
        var errorCount = 0;
        var source = Observable.Create<int>(observer =>
        {
            attempts++;
            observer.OnError(new InvalidOperationException());
            return Disposable.Empty;
        });

        Exception? caughtException = null;

        using var sub = source.OnErrorRetry<int, InvalidOperationException>(ex => errorCount++, retryCount: RetryCount)
            .Subscribe(_ => { }, ex => caughtException = ex);

        using (Assert.Multiple())
        {
            // retryCount = retries after the initial attempt; total subscriptions = 1 + retryCount.
            await Assert.That(attempts).IsEqualTo(ExpectedAttempts);
            await Assert.That(caughtException).IsNotNull();
        }
    }

    /// <summary>
    /// Tests OnErrorRetry with delay.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task OnErrorRetry_WithDelay_DelaysRetries()
    {
        var attempts = 0;
        var source = Observable.Create<int>(observer =>
        {
            attempts++;
            if (attempts < 3)
            {
                observer.OnError(new InvalidOperationException());
            }
            else
            {
                observer.OnNext(42);
                observer.OnCompleted();
            }

            return Disposable.Empty;
        });

        var startTimestamp = TimeProvider.System.GetTimestamp();

        var result = source.OnErrorRetry<int, InvalidOperationException>(
            ex => { },
            retryCount: 5,
            delay: TimeSpan.FromMilliseconds(50))
            .Wait();

        var elapsed = TimeProvider.System.GetElapsedTime(startTimestamp);

        using (Assert.Multiple())
        {
            await Assert.That(result).IsEqualTo(SampleValue42);
            await Assert.That(elapsed.TotalMilliseconds).IsGreaterThanOrEqualTo(MinimumExpectedMilliseconds);
        }
    }

    /// <summary>
    /// Tests OnErrorRetry with delay and no error action.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task OnErrorRetry_WithDelayAndErrorAction_RetriesWithDelay()
    {
        var attemptCount = 0;
        var errorsCaught = 0;
        var results = new List<int>();
        var scheduler = new VirtualClock();

        var source = Observable.Create<int>(observer =>
        {
            attemptCount++;
            if (attemptCount < 2)
            {
                observer.OnError(new InvalidOperationException($"Attempt {attemptCount}"));
            }
            else
            {
                observer.OnNext(42);
                observer.OnCompleted();
            }

            return Disposable.Empty;
        });

        source.OnErrorRetry<int, InvalidOperationException>(
                ex => errorsCaught++,
                retryCount: int.MaxValue,
                delay: TimeSpan.FromMilliseconds(10),
                delayScheduler: scheduler)
            .Subscribe(results.Add);

        scheduler.AdvanceBy(TimeSpan.FromMilliseconds(10).Ticks);

        using (Assert.Multiple())
        {
            await Assert.That(errorsCaught).IsEqualTo(1);
            await Assert.That(results).IsCollectionEqualTo([SampleValue42]);
        }
    }

    /// <summary>
    /// Tests OnErrorRetry with retry count limit.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task OnErrorRetry_WithRetryCount_LimitsRetries()
    {
        const int RetryCount = 2;
        const int ExpectedErrorCallbacks = RetryCount + 1;
        var attemptCount = 0;
        var errorsCaught = 0;
        var finalError = false;

        var source = Observable.Create<int>(observer =>
        {
            attemptCount++;
            observer.OnError(new InvalidOperationException($"Attempt {attemptCount}"));
            return Disposable.Empty;
        });

        source.OnErrorRetry<int, InvalidOperationException>(
                ex => errorsCaught++,
                retryCount: RetryCount)
            .Subscribe(_ => { }, ex => finalError = true);

        var finalErrorReceived = await AsyncTestHelpers.WaitForConditionAsync(
            () => finalError,
            TimeSpan.FromSeconds(2));

        using (Assert.Multiple())
        {
            // OnError callback fires for every failure (including the final propagated one):
            // 1 initial attempt + retryCount retries = retryCount + 1 callbacks.
            await Assert.That(finalErrorReceived).IsTrue();
            await Assert.That(errorsCaught).IsEqualTo(ExpectedErrorCallbacks);
            await Assert.That(finalError).IsTrue();
        }
    }

    /// <summary>
    /// Tests OnErrorRetry with retry count and delay.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task OnErrorRetry_WithRetryCountAndDelay_LimitsRetriesWithDelay()
    {
        const int RetryCount = 2;
        const int ExpectedErrorCallbacks = RetryCount + 1;
        const int DelayMilliseconds = 10;
        var attemptCount = 0;
        var errorsCaught = 0;
        var finalError = false;
        var scheduler = new VirtualClock();

        var source = Observable.Create<int>(observer =>
        {
            attemptCount++;
            observer.OnError(new InvalidOperationException($"Attempt {attemptCount}"));
            return Disposable.Empty;
        });

        source.OnErrorRetry<int, InvalidOperationException>(
                ex => errorsCaught++,
                retryCount: RetryCount,
                delay: TimeSpan.FromMilliseconds(DelayMilliseconds),
                delayScheduler: scheduler)
            .Subscribe(_ => { }, ex => finalError = true);

        // Advance enough virtual time to drain all retries plus the final propagation.
        scheduler.AdvanceBy(TimeSpan.FromMilliseconds(DelayMilliseconds * (RetryCount + 1)).Ticks);

        using (Assert.Multiple())
        {
            // OnError callback fires for every failure (initial + retries) = retryCount + 1 calls.
            await Assert.That(errorsCaught).IsEqualTo(ExpectedErrorCallbacks);
            await Assert.That(finalError).IsTrue();
        }
    }

    /// <summary>
    /// Tests OnErrorRetry with retry count, delay, and scheduler.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task OnErrorRetry_WithRetryCountDelayAndScheduler_RetriesCorrectly()
    {
        var attemptCount = 0;
        var errorsCaught = 0;

        var source = Observable.Create<int>(observer =>
        {
            attemptCount++;
            if (attemptCount < 2)
            {
                observer.OnError(new InvalidOperationException($"Attempt {attemptCount}"));
            }
            else
            {
                observer.OnNext(42);
                observer.OnCompleted();
            }

            return Disposable.Empty;
        });

        var result = 0;
        source.OnErrorRetry<int, InvalidOperationException>(
                ex => errorsCaught++,
                retryCount: 3,
                delay: TimeSpan.FromMilliseconds(10),
                delayScheduler: Sequencer.Immediate)
            .Subscribe(r => result = r);

        using (Assert.Multiple())
        {
            await Assert.That(errorsCaught).IsEqualTo(1);
            await Assert.That(result).IsEqualTo(SampleValue42);
        }
    }

    /// <summary>
    /// Tests OnErrorRetry with action only uses zero-delay retry.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorRetryWithActionOnly_ThenRetriesImmediately()
    {
        var attempts = 0;
        var errorCount = 0;
        var source = Observable.Create<int>(observer =>
        {
            attempts++;
            if (attempts < 3)
            {
                observer.OnError(new InvalidOperationException());
            }
            else
            {
                observer.OnNext(42);
                observer.OnCompleted();
            }

            return Disposable.Empty;
        });

        var results = new List<int>();
        using var sub = source.OnErrorRetry<int, InvalidOperationException>(ex => errorCount++)
            .Subscribe(results.Add);

        using (Assert.Multiple())
        {
            await Assert.That(results).IsCollectionEqualTo([SampleValue42]);
            await Assert.That(errorCount).IsEqualTo(SampleValue2);
        }
    }

    /// <summary>
    /// Tests OnErrorRetry with delay retries after specified delay.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorRetryWithDelay_ThenRetriesAfterDelay()
    {
        var attempts = 0;
        var source = Observable.Create<int>(observer =>
        {
            attempts++;
            if (attempts < 2)
            {
                observer.OnError(new InvalidOperationException());
            }
            else
            {
                observer.OnNext(42);
                observer.OnCompleted();
            }

            return Disposable.Empty;
        });

        var result = source.OnErrorRetry<int, InvalidOperationException>(
            ex => { },
            TimeSpan.FromMilliseconds(10))
            .Wait();

        await Assert.That(result).IsEqualTo(SampleValue42);
    }

    /// <summary>
    /// Tests RetryWithBackoff rethrows after max retries exceeded.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithBackoffExceedsMaxRetries_ThenRethrows()
    {
        var source = Observable.Throw<int>(new InvalidOperationException("fail"));
        Exception? caughtError = null;

        source.RetryWithBackoff(
            maxRetries: 2,
            initialDelay: TimeSpan.FromMilliseconds(1),
            backoffFactor: 2.0,
            maxDelay: null,
            scheduler: Sequencer.Immediate)
            .Subscribe(_ => { }, ex => caughtError = ex);

        await Assert.That(caughtError).IsNotNull();
    }

    /// <summary>
    /// Tests RetryWithBackoff caps delay at maxDelay.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithBackoffDelayExceedsMax_ThenCapsDelay()
    {
        var attempts = 0;
        var source = Observable.Create<int>(observer =>
        {
            attempts++;
            if (attempts < 4)
            {
                observer.OnError(new InvalidOperationException());
            }
            else
            {
                observer.OnNext(42);
                observer.OnCompleted();
            }

            return Disposable.Empty;
        });

        var result = source.RetryWithBackoff(
            maxRetries: 5,
            initialDelay: TimeSpan.FromMilliseconds(5),
            backoffFactor: 10.0,
            maxDelay: TimeSpan.FromMilliseconds(20),
            scheduler: Sequencer.Immediate)
            .Wait();

        await Assert.That(result).IsEqualTo(SampleValue42);
    }

    /// <summary>
    /// Tests RetryWithDelay retries with custom delay selector.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithDelay_ThenRetriesWithCustomDelay()
    {
        var attempts = 0;
        var source = Observable.Create<int>(observer =>
        {
            attempts++;
            if (attempts < 3)
            {
                observer.OnError(new InvalidOperationException());
            }
            else
            {
                observer.OnNext(42);
                observer.OnCompleted();
            }

            return Disposable.Empty;
        });

        var result = source.RetryWithDelay(5, attempt => TimeSpan.FromMilliseconds(1)).Wait();

        await Assert.That(result).IsEqualTo(SampleValue42);
    }

    /// <summary>
    /// Tests RetryForeverWithDelay retries indefinitely with delay.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryForeverWithDelay_ThenRetriesIndefinitely()
    {
        var attempts = 0;
        var source = Observable.Create<int>(observer =>
        {
            attempts++;
            if (attempts < 4)
            {
                observer.OnError(new InvalidOperationException());
            }
            else
            {
                observer.OnNext(42);
                observer.OnCompleted();
            }

            return Disposable.Empty;
        });

        var result = source.RetryForeverWithDelay(TimeSpan.FromMilliseconds(1)).Wait();

        await Assert.That(result).IsEqualTo(SampleValue42);
    }

    /// <summary>
    /// Tests RetryWithFixedDelay retries with constant delay between retries.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithFixedDelay_ThenRetriesWithConstantDelay()
    {
        var attempts = 0;
        var source = Observable.Create<int>(observer =>
        {
            attempts++;
            if (attempts < 3)
            {
                observer.OnError(new InvalidOperationException());
            }
            else
            {
                observer.OnNext(42);
                observer.OnCompleted();
            }

            return Disposable.Empty;
        });

        var result = source.RetryWithFixedDelay(5, TimeSpan.FromMilliseconds(1)).Wait();

        await Assert.That(result).IsEqualTo(SampleValue42);
    }

    /// <summary>
    /// Tests RetryWithBackoff inner retry with max delay cap path.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithBackoffInnerRetry_ThenRetriesAndCapsDelay()
    {
        var scheduler = new VirtualClock();
        var attempt = 0;
        var source = Observable.Defer(() =>
        {
            attempt++;
            return attempt < 3
                ? Observable.Throw<int>(new InvalidOperationException("retry"))
                : Observable.Return(42);
        });

        var results = new List<int>();
        Exception? error = null;

        source.RetryWithBackoff(
            maxRetries: 5,
            initialDelay: TimeSpan.FromTicks(10),
            backoffFactor: 2.0,
            maxDelay: TimeSpan.FromTicks(15),
            scheduler: scheduler)
            .Subscribe(results.Add, ex => error = ex);

        // Advance through retry delays
        scheduler.AdvanceBy(LongDelayMilliseconds);

        await Assert.That(results).IsCollectionEqualTo([SampleValue42]);
        await Assert.That(error).IsNull();
    }

    /// <summary>
    /// Tests OnErrorRetry with negative delay ticks sets dueTime to zero.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorRetryNegativeDelay_ThenUsesZeroDelay()
    {
        var attempt = 0;
        var source = Observable.Defer(() =>
        {
            attempt++;
            return attempt < 3
                ? Observable.Throw<int>(new InvalidOperationException("fail"))
                : Observable.Return(42);
        });

        var results = new List<int>();
        Exception? error = null;
        var received = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        source.OnErrorRetry<int, InvalidOperationException>(
            _ => { },
            retryCount: 5,
            delay: TimeSpan.FromTicks(-1))
            .Subscribe(
                v =>
                {
                    results.Add(v);
                    received.TrySetResult();
                },
                ex => error = ex);

        await received.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(results).Contains(SampleValue42);
    }

    /// <summary>
    /// Tests OnErrorRetry with retry count check rethrows after exceeding count.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorRetryExceedsRetryCount_ThenRethrows()
    {
        var source = Observable.Throw<int>(new InvalidOperationException("fail"));
        Exception? caught = null;
        var errorReceived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        source.OnErrorRetry<int, InvalidOperationException>(
            _ => { },
            retryCount: 2,
            delay: TimeSpan.Zero)
            .Subscribe(
                _ => { },
                ex =>
                {
                    caught = ex;
                    errorReceived.TrySetResult();
                });

        await errorReceived.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(caught).IsNotNull();
        await Assert.That(caught).IsTypeOf<InvalidOperationException>();
    }

    /// <summary>
    /// Tests RetryWithBackoff caps delay at maxDelay when backoff exceeds it.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithBackoffExceedsMaxDelay_ThenCapsAtMaxDelay()
    {
        var scheduler = new VirtualClock();
        var attempt = 0;
        var source = Observable.Create<int>(obs =>
        {
            attempt++;
            if (attempt <= 3)
            {
                obs.OnError(new InvalidOperationException($"fail {attempt}"));
            }
            else
            {
                obs.OnNext(42);
                obs.OnCompleted();
            }

            return Disposable.Empty;
        });

        var results = new List<int>();
        using var sub = source
            .RetryWithBackoff(
                5,
                initialDelay: TimeSpan.FromMilliseconds(100),
                backoffFactor: 10.0,
                maxDelay: TimeSpan.FromMilliseconds(200),
                scheduler: scheduler)
            .Subscribe(results.Add);

        // Advance through retry delays
        for (var i = 0; i < SampleValue5; i++)
        {
            scheduler.AdvanceBy(TimeSpan.FromMilliseconds(250).Ticks);
        }

        await Assert.That(results).Contains(SampleValue42);
    }

    /// <summary>
    /// Tests RetryWithBackoff maxDelay cap is applied when computed delay exceeds it,
    /// exercising line 1240 of ReactiveExtensions.cs.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithBackoffDelayExceedsMaxDelay_ThenCappedAtMaxDelay()
    {
        var scheduler = new VirtualClock();
        var attemptCount = 0;

        var source = Observable.Create<int>(observer =>
        {
            attemptCount++;
            if (attemptCount <= 3)
            {
                observer.OnError(new InvalidOperationException($"attempt {attemptCount}"));
            }
            else
            {
                observer.OnNext(99);
                observer.OnCompleted();
            }

            return Disposable.Empty;
        });

        // initialDelay=1ms, backoffFactor=100 => attempt 2 delay = 1*100^1 = 100ms, exceeds maxDelay=5ms
        var results = new List<int>();
        source.RetryWithBackoff(
            maxRetries: 5,
            initialDelay: TimeSpan.FromMilliseconds(1),
            backoffFactor: 100.0,
            maxDelay: TimeSpan.FromMilliseconds(5),
            scheduler: scheduler)
            .Subscribe(results.Add);

        // Advance past the capped delays
        scheduler.AdvanceBy(TimeSpan.FromSeconds(1).Ticks);

        await Assert.That(results).Contains(SampleValue99);
    }

    /// <summary>
    /// Verifies that RetryWithBackoff caps the computed delay at maxDelay when the
    /// exponential backoff exceeds it. Uses Sequencer.Immediate so the cap is exercised
    /// synchronously.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithBackoffComputedDelayExceedsMaxDelay_ThenCappedToMaxDelay()
    {
        var attemptCount = 0;
        var source = Observable.Create<int>(observer =>
        {
            attemptCount++;

            // Fail on first two attempts, succeed on third
            if (attemptCount <= 2)
            {
                observer.OnError(new InvalidOperationException($"attempt {attemptCount}"));
            }
            else
            {
                observer.OnNext(42);
                observer.OnCompleted();
            }

            return Disposable.Empty;
        });

        // initialDelay=1ms, backoffFactor=1000 => computed delay = 1000ms >> maxDelay=2ms
        // This ensures the cap path at line 1240 is hit
        var result = source.RetryWithBackoff(
            maxRetries: 5,
            initialDelay: TimeSpan.FromMilliseconds(1),
            backoffFactor: 1000.0,
            maxDelay: TimeSpan.FromMilliseconds(2),
            scheduler: Sequencer.Immediate)
            .Wait();

        await Assert.That(result).IsEqualTo(SampleValue42);
        await Assert.That(attemptCount).IsEqualTo(SampleValue3);
    }

    /// <summary>
    /// Verifies that RetryWithBackoff caps the computed delay at maxDelay using a
    /// VirtualClock so the cap assignment is directly exercised.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithBackoff_GivenLargeBackoffFactor_ThenDelayIsCappedAtMaxDelay()
    {
        // Given
        var scheduler = new VirtualClock();
        var attemptCount = 0;
        var source = Observable.Create<int>(observer =>
        {
            attemptCount++;

            if (attemptCount <= 3)
            {
                observer.OnError(new InvalidOperationException($"fail {attemptCount}"));
            }
            else
            {
                observer.OnNext(100);
                observer.OnCompleted();
            }

            return Disposable.Empty;
        });

        var results = new List<int>();

        // When — backoffFactor 500 with initialDelay 1ms yields huge computed delays,
        // all of which must be capped to maxDelay 5ms.
        using var sub = source
            .RetryWithBackoff(
                maxRetries: 5,
                initialDelay: TimeSpan.FromMilliseconds(1),
                backoffFactor: 500.0,
                maxDelay: TimeSpan.FromMilliseconds(5),
                scheduler: scheduler)
            .Subscribe(results.Add);

        // Advance the scheduler enough for each capped retry delay
        for (var i = 0; i < SampleValue10; i++)
        {
            scheduler.AdvanceBy(TimeSpan.FromMilliseconds(10).Ticks);
        }

        // Then
        await Assert.That(results).Contains(SchedulerWindowTicks);
        await Assert.That(attemptCount).IsEqualTo(SampleValue4);
    }

    /// <summary>
    /// Verifies that RetryWithDelay rethrows the original exception when all retries
    /// are exhausted, exercising the error-propagation branch.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRetryWithDelayExhaustsRetries_ThenRethrowsOriginalException()
    {
        // Given — source always fails
        var source = Observable.Throw<int>(new InvalidOperationException("permanent"));
        Exception? caught = null;

        // When
        using var sub = source
            .RetryWithDelay(2, _ => TimeSpan.FromMilliseconds(1))
            .Subscribe(
                static _ => { },
                ex => caught = ex);

        // Allow time for the retry attempts to complete
        await AsyncTestHelpers.WaitForConditionAsync(() => caught is not null, TimeSpan.FromSeconds(5));

        // Then
        await Assert.That(caught).IsNotNull();
        await Assert.That(caught).IsTypeOf<InvalidOperationException>();
        await Assert.That(caught!.Message).IsEqualTo("permanent");
    }
}
