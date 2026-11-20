// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for <see cref="TimeoutWitness{T}"/>.</summary>
public sealed class TimeoutWitnessTests
{
    /// <summary>The value sent after the deadline.</summary>
    private const int LateValue = 42;

    /// <summary>The event recorded when the observer completes with a failure.</summary>
    private const string TimeoutEvent = "timeout";

    /// <summary>The inter-element deadline used by the tests.</summary>
    private static readonly TimeSpan DueTime = TimeSpan.FromMilliseconds(20);

    /// <summary>How long a test waits for an asynchronous hand-off before failing.</summary>
    private static readonly TimeSpan HandOffTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies a value that arrives after the timeout fired is not forwarded after the timeout completion.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnNextAsync_AfterTimeoutFires_IsNotForwarded()
    {
        ManualTimeProvider time = new();
        List<string> events = [];
        CallbackWitnessAsync<int> observer = new(
            (value, _) =>
            {
                events.Add($"next {value}");
                return default;
            },
            null,
            result =>
            {
                events.Add(result.IsFailure ? TimeoutEvent : "completed");
                return default;
            });
        await using TimeoutWitness<int> witness = new(observer, DueTime, time);
        witness.StartTimer(CancellationToken.None);

        (await time.NextTimerAsync()).Fire();
        await witness.OnNextAsync(LateValue, CancellationToken.None);

        await Assert.That(string.Join(", ", events)).IsEqualTo(TimeoutEvent);
    }

    /// <summary>Verifies the timeout completion waits for a downstream <c>OnNextAsync</c> that is still running.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TimeoutFires_WhileOnNextIsRunning_DeliversCompletionAfterOnNextReturns()
    {
        ManualTimeProvider time = new();
        List<string> events = [];
        TaskCompletionSource releaseNext = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        CallbackWitnessAsync<int> observer = new(
            async (value, _) =>
            {
                lock (events)
                {
                    events.Add($"next {value} start");
                }

                await releaseNext.Task.ConfigureAwait(false);
                lock (events)
                {
                    events.Add($"next {value} end");
                }
            },
            null,
            result =>
            {
                lock (events)
                {
                    events.Add(result.IsFailure ? TimeoutEvent : "completed");
                }

                _ = completed.TrySetResult();
                return default;
            });
        await using TimeoutWitness<int> witness = new(observer, DueTime, time);
        witness.StartTimer(CancellationToken.None);
        var timer = await time.NextTimerAsync();

        var pendingNext = witness.OnNextAsync(1, CancellationToken.None).AsTask();
        timer.Fire();
        await Assert.That(completed.Task.IsCompleted).IsFalse();

        releaseNext.SetResult();
        await Assert.That(pendingNext).CompletesWithin(HandOffTimeout);
        await Assert.That(completed.Task).CompletesWithin(HandOffTimeout);
        await Assert.That(string.Join(", ", events)).IsEqualTo($"next 1 start, next 1 end, {TimeoutEvent}");
    }

    /// <summary>Verifies a source completion after the timeout fired does not complete the observer a second time.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnCompletedAsync_AfterTimeoutFires_DoesNotCompleteTwice()
    {
        ManualTimeProvider time = new();
        List<Result> completions = [];
        CallbackWitnessAsync<int> observer = new(static (_, _) => default, null, result =>
        {
            completions.Add(result);
            return default;
        });
        await using TimeoutWitness<int> witness = new(observer, DueTime, time);
        witness.StartTimer(CancellationToken.None);

        (await time.NextTimerAsync()).Fire();
        await witness.OnCompletedAsync(Result.Success);

        await Assert.That(completions).Count().IsEqualTo(1);
        await Assert.That(completions[0].Exception).IsTypeOf<TimeoutException>();
    }

    /// <summary>Verifies timer creation edge cases when already disposed, raced by completion, or throwing from the time provider.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task StartTimer_DisposedRacedOrThrowingTimeProvider_HandlesCleanly()
    {
        ManualTimeProvider time = new();
        CallbackWitnessAsync<int> observer = new(static (_, _) => default);

        TimeoutWitness<int> disposedWitness = new(observer, DueTime, time);
        await disposedWitness.DisposeAsync();
        disposedWitness.StartTimer(CancellationToken.None);

        CompletingOnCreateTimeProvider completingTime = new();
        await using TimeoutWitness<int> racedWitness = new(observer, DueTime, completingTime);
        completingTime.Target = racedWitness;
        racedWitness.StartTimer(CancellationToken.None);
        await completingTime.CompletionTask!;
        await Assert.That(completingTime.TimerDisposed).IsTrue();

        using UnhandledExceptionCapture unhandled = new();
        InvalidOperationException createFailure = new("create-timer");
        ThrowingTimeProvider throwingTime = new(createFailure);
        await using TimeoutWitness<int> createFaultWitness = new(observer, DueTime, throwingTime);
        createFaultWitness.StartTimer(CancellationToken.None);
        var capturedCreate = await unhandled.WaitForAsync(ex => ReferenceEquals(ex, createFailure));
        await Assert.That(capturedCreate).IsSameReferenceAs(createFailure);
    }

    /// <summary>Verifies a resumed error is forwarded and leaves the deadline running, and throwing timeout completions report unhandled exceptions.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OnErrorResumeAndFaultingTimeoutObserver_ForwardAndReportUnhandled()
    {
        ManualTimeProvider time = new();
        List<Exception> resumed = [];
        List<Result> completions = [];
        CallbackWitnessAsync<int> observer = new(
            static (_, _) => default,
            (ex, _) =>
            {
                resumed.Add(ex);
                return default;
            },
            result =>
            {
                completions.Add(result);
                return default;
            });

        await using TimeoutWitness<int> resumeWitness = new(observer, DueTime, time);
        resumeWitness.StartTimer(CancellationToken.None);
        var resumeTimer = await time.NextTimerAsync();
        Exception resumeError = new InvalidOperationException("resume");
        await resumeWitness.OnErrorResumeAsync(resumeError, CancellationToken.None);
        resumeTimer.Fire();
        await Assert.That(resumed).IsCollectionEqualTo([resumeError]);
        await Assert.That(completions).Count().IsEqualTo(1);
        await Assert.That(completions[0].Exception).IsTypeOf<TimeoutException>();

        using UnhandledExceptionCapture unhandled = new();
        InvalidOperationException timeoutObserverFailure = new("timeout-observer");
        CallbackWitnessAsync<int> faultingObserver = new(
            static (_, _) => default,
            null,
            result =>
            {
                _ = result;
                throw timeoutObserverFailure;
            });
        await using TimeoutWitness<int> faultingTimeout = new(faultingObserver, DueTime, time);
        faultingTimeout.StartTimer(CancellationToken.None);
        (await time.NextTimerAsync()).Fire();
        var capturedTimeout = await unhandled.WaitForAsync(ex => ReferenceEquals(ex, timeoutObserverFailure));
        await Assert.That(capturedTimeout).IsSameReferenceAs(timeoutObserverFailure);
    }

    /// <summary>Time provider that completes the target witness while creating its timer.</summary>
    private sealed class CompletingOnCreateTimeProvider : TimeProvider
    {
        /// <summary>Gets or sets the witness to complete during timer creation.</summary>
        public TimeoutWitness<int>? Target { get; set; }

        /// <summary>Gets the completion task started during timer creation.</summary>
        public Task? CompletionTask { get; private set; }

        /// <summary>Gets a value indicating whether the created timer was disposed.</summary>
        public bool TimerDisposed { get; private set; }

        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            CompletionTask = CompleteTargetAsync();
            callback(state);
            return new TrackingTimer(() => TimerDisposed = true);
        }

        /// <summary>Completes the target witness if present.</summary>
        /// <returns>A task representing the completion call.</returns>
        private async Task CompleteTargetAsync()
        {
            if (Target is not null)
            {
                await Target.OnCompletedAsync(Result.Success).ConfigureAwait(false);
            }
        }

        /// <summary>Minimal timer that records synchronous disposal.</summary>
        /// <param name="onDispose">Callback invoked on disposal.</param>
        private sealed class TrackingTimer(Action onDispose) : ITimer
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Change(TimeSpan dueTime, TimeSpan period) => true;

            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose() => onDispose();

            /// <inheritdoc/>
            public ValueTask DisposeAsync()
            {
                onDispose();
                return default;
            }
        }
    }

    /// <summary>Time provider whose <see cref="CreateTimer"/> throws a configured exception.</summary>
    /// <param name="error">The exception to throw.</param>
    private sealed class ThrowingTimeProvider(Exception error) : TimeProvider
    {
        /// <inheritdoc/>
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
            throw error;
    }
}
