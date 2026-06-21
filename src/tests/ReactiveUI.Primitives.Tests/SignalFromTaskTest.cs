// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for creating signals from tasks.</summary>
public class SignalFromTaskTest
{
    /// <summary>The value emitted by the simple from-task result test.</summary>
    private const int EmittedValue = 2;

    /// <summary>The value produced by the successful pending task.</summary>
    private const int SuccessValue = 7;

    /// <summary>The value produced by the disposed pending subscription.</summary>
    private const int DisposedValue = 99;

    /// <summary>Maximum wait for cancellation callbacks that are intentionally driven by timers.</summary>
    private const int CancellationCallbackTimeoutMilliseconds = 15_000;

    /// <summary>Delay used before checking that a task has started.</summary>
    private const int InitialDelayMilliseconds = 500;

    /// <summary>Delay before token cancellation is requested.</summary>
    private const int TokenCancellationDelayMilliseconds = 1000;

    /// <summary>Delay used to simulate cancellation cleanup.</summary>
    private const int CleanupDelayMilliseconds = 5000;

    /// <summary>
    /// Time spent performing synchronous cancellation cleanup work. Kept short so the
    /// blocking <see cref = "Thread.Sleep(int)"/> does not occupy a thread-pool thread long
    /// enough to delay the timer-driven cancellation callbacks the tests wait on (which
    /// previously tipped the loaded CI runners over <see cref = "CancellationCallbackTimeoutMilliseconds"/>).
    /// </summary>
    private const int CleanupWorkMilliseconds = 250;

    /// <summary>Delay used to wait for cancellation cleanup to finish.</summary>
    private const int CancellationWaitDelayMilliseconds = 6000;

    /// <summary>Delay used by the command body.</summary>
    private const int CommandDelayMilliseconds = 10_000;

    /// <summary>Delay used to wait for normal command completion.</summary>
    private const int CompletionWaitDelayMilliseconds = 11_000;

    /// <summary>Exception message used by user exception tests.</summary>
    private const string BreakExecutionMessage = "break execution";

    /// <summary>Status text recorded when a command starts.</summary>
    private const string StartedCommand = "started command";

    /// <summary>Status text recorded when cancellation cleanup starts.</summary>
    private const string StartingCancellingCommand = "starting cancelling command";

    /// <summary>Status text recorded when cancellation cleanup finishes.</summary>
    private const string FinishedCancellingCommand = "finished cancelling command";

    /// <summary>Status text recorded when the command completes normally.</summary>
    private const string FinishedCommandNormally = "finished command Normally";

    /// <summary>Status text recorded by the exception handler.</summary>
    private const string ExceptionShouldBeHere = "Exception Should Be here";

    /// <summary>Status text recorded by the finalizer callback.</summary>
    private const string ShouldAlwaysComeHere = "Should always come here.";

    /// <summary>Covers from-task cancellation callback argument validation.</summary>
    [Test]
    public void FromTaskValidatesCancellationCallback()
    {
        var taskSignal = Signal.FromTask(_ => Task.FromResult(1), Sequencer.Immediate);
        try
        {
            _ = Assert.Throws<ArgumentNullException>(() => taskSignal.GetOperationCanceled(null!));
        }
        finally
        {
            (taskSignal as IDisposable)?.Dispose();
        }
    }

    /// <summary>Covers from-task result emission and completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromTaskEmitsResult()
    {
        var taskSignal = Signal.FromTask(_ => Task.FromResult(EmittedValue), Sequencer.Immediate);
        try
        {
            List<int> taskValues = [];
            var taskCompleted = 0;
            _ = taskSignal.Subscribe(taskValues.Add, error => throw error, () => taskCompleted++);
            await Assert.That(taskValues.SequenceEqual([EmittedValue])).IsTrue();
            await Assert.That(taskCompleted).IsEqualTo(1);
        }
        finally
        {
            (taskSignal as IDisposable)?.Dispose();
        }
    }

    /// <summary>Covers non-completed task factory continuations for success, fault, cancellation, and disposed subscriptions.</summary>
    /// <returns>A task that completes when all continuations have been observed.</returns>
    [Test]
    public async Task TaskFactoryContinuationsCoverPendingTaskBranches()
    {
        ConcurrentQueue<int> values = new();
        ConcurrentQueue<string> errors = new();
        void AddValue(int value) => values.Enqueue(value);
        void AddError(Exception error) => errors.Enqueue(error.GetType().Name);

        bool ObservedPendingBranches()
        {
            var observedValues = values.ToArray();
            var observedErrors = errors.ToArray();
            return Array.IndexOf(observedValues, SuccessValue) >= 0
                   && Array.IndexOf(observedErrors, nameof(InvalidOperationException)) >= 0
                   && Array.IndexOf(observedErrors, nameof(TaskCanceledException)) >= 0;
        }

        TaskCompletionSource<int> success = new();
        TaskCompletionSource<int> fault = new();
        TaskCompletionSource<int> canceled = new();
        TaskCompletionSource<int> disposed = new();
        var disposedSubscription = Signal.FromTask(disposed.Task).Subscribe(_ => AddValue(DisposedValue), AddError);
        disposedSubscription.Dispose();
        _ = Signal.FromTask(success.Task).Subscribe(AddValue, AddError);
        _ = Signal.FromTask(fault.Task).Subscribe(AddValue, AddError);
        _ = Signal.FromTask(canceled.Task).Subscribe(AddValue, AddError);
        success.SetResult(SuccessValue);
        fault.SetException(new InvalidOperationException("pending-fault"));
        canceled.SetCanceled(new(true));
        disposed.SetResult(DisposedValue);
        await TestPolling.SpinUntil(ObservedPendingBranches, TimeSpan.FromSeconds(2)).ConfigureAwait(false);
        var finalValues = values.ToArray();
        var finalErrors = errors.ToArray();
        await Assert.That(finalValues.Length).IsEqualTo(1);
        await Assert.That(finalValues[0]).IsEqualTo(SuccessValue);
        await Assert.That(finalErrors).Contains(nameof(InvalidOperationException));
        await Assert.That(finalErrors).Contains(nameof(TaskCanceledException));
    }

    /// <summary>Signals from task handles user exceptions.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTaskHandlesUserExceptions()
    {
        StatusTrail statusTrail = new();
        var position = 0;
        var fixture = Signal.FromTask(async cts =>
        {
            RecordStatus(statusTrail, ref position, StartedCommand);
            await Task.Delay(CommandDelayMilliseconds, cts.Token)
                .HandleCancellation(() => RecordCancellationCleanup(statusTrail, ref position)).ConfigureAwait(true);
            if (!cts.IsCancellationRequested)
            {
                RecordStatus(statusTrail, ref position, FinishedCommandNormally);
            }

            throw new InvalidOperationException(BreakExecutionMessage);
        }).Recover<RxVoid, Exception>(ex =>
        {
            RecordStatus(statusTrail, ref position, ExceptionShouldBeHere);
            return Signal.Fail<RxVoid>(ex);
        }).OnCleanup(() => RecordStatus(statusTrail, ref position, ShouldAlwaysComeHere));
        var result = false;
        using var subscription = fixture.Subscribe(_ => result = true);
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);
        await Assert.That(StatusMessages(statusTrail)).Contains(StartedCommand);
        await Task.Delay(CommandDelayMilliseconds).ConfigureAwait(true);
        subscription.Dispose();
        await Task.Delay(CancellationWaitDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(StatusMessages(statusTrail)).DoesNotContain(StartingCancellingCommand);
        await Assert.That(StatusMessages(statusTrail)).Contains(ShouldAlwaysComeHere);
        await Assert.That(StatusMessages(statusTrail)).DoesNotContain(FinishedCancellingCommand);
        await Assert.That(StatusMessages(statusTrail)).Contains(ExceptionShouldBeHere);
        await Assert.That(StatusMessages(statusTrail)).Contains(FinishedCommandNormally);
        await Assert.That(result).IsFalse();
    }

    /// <summary>Signals from task handles cancellation.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTaskHandlesCancellation()
    {
        StatusTrail statusTrail = new();
        var position = 0;
        var fixture = Signal.FromTask(async cts =>
        {
            RecordStatus(statusTrail, ref position, StartedCommand);
            await Task.Delay(CommandDelayMilliseconds, cts.Token)
                .HandleCancellation(() => RecordCancellationCleanup(statusTrail, ref position)).ConfigureAwait(true);
            if (!cts.IsCancellationRequested)
            {
                RecordStatus(statusTrail, ref position, FinishedCommandNormally);
            }

            return RxVoid.Default;
        }).Recover<RxVoid, Exception>(ex =>
        {
            RecordStatus(statusTrail, ref position, ExceptionShouldBeHere);
            return Signal.Fail<RxVoid>(ex);
        }).OnCleanup(() => RecordStatus(statusTrail, ref position, ShouldAlwaysComeHere));
        var result = false;
        using var subscription = fixture.Subscribe(_ => result = true);
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);
        await Assert.That(StatusMessages(statusTrail)).Contains(StartedCommand);
        subscription.Dispose();
        await Task.Delay(CancellationWaitDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(StatusMessages(statusTrail)).Contains(StartingCancellingCommand);
        await Assert.That(StatusMessages(statusTrail)).Contains(ShouldAlwaysComeHere);
        await Assert.That(StatusMessages(statusTrail)).Contains(FinishedCancellingCommand);
        await Assert.That(StatusMessages(statusTrail)).DoesNotContain(FinishedCommandNormally);
        await Assert.That(result).IsFalse();
    }

    /// <summary>Signals from task handles token cancellation.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTaskHandlesTokenCancellation()
    {
        StatusTrail statusTrail = new();
        TaskCompletionSource cleanupCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource finallyCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var position = 0;
        var fixture = Signal.FromTask(async cts =>
        {
            RecordStatus(statusTrail, ref position, StartedCommand);
            await Task.Delay(TokenCancellationDelayMilliseconds, cts.Token).HandleCancellation().ConfigureAwait(true);
            var cancellationTask = CancelAfterDelayAsync(cts);
            await Task.Delay(CleanupDelayMilliseconds, cts.Token).HandleCancellation(() =>
            {
                RecordCancellationCleanup(statusTrail, ref position);
                _ = cleanupCompleted.TrySetResult();
            }).ConfigureAwait(true);
            await cancellationTask.ConfigureAwait(false);
            if (!cts.IsCancellationRequested)
            {
                RecordStatus(statusTrail, ref position, FinishedCommandNormally);
            }

            return RxVoid.Default;
        }).Recover<RxVoid, Exception>(ex =>
        {
            RecordStatus(statusTrail, ref position, ExceptionShouldBeHere);
            return Signal.Fail<RxVoid>(ex);
        }).OnCleanup(() =>
        {
            RecordStatus(statusTrail, ref position, ShouldAlwaysComeHere);
            _ = finallyCompleted.TrySetResult();
        });
        var result = false;
        using var subscription = fixture.Subscribe(_ => result = true);
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);
        await Assert.That(StatusMessages(statusTrail)).Contains(StartedCommand);
        await WaitForAsync(
            Task.WhenAll(cleanupCompleted.Task, finallyCompleted.Task),
            CancellationCallbackTimeoutMilliseconds).ConfigureAwait(false);
        await Assert.That(StatusMessages(statusTrail)).Contains(StartingCancellingCommand);
        await Assert.That(StatusMessages(statusTrail)).Contains(ShouldAlwaysComeHere);
        await Assert.That(StatusMessages(statusTrail)).Contains(FinishedCancellingCommand);
        await Assert.That(StatusMessages(statusTrail)).DoesNotContain(FinishedCommandNormally);
        await Assert.That(result).IsFalse();
    }

    /// <summary>Signals from task handles cancellation in base.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTaskHandlesCancellationInBase()
    {
        StatusTrail statusTrail = new();
        var position = 0;
        var fixture = Signal.FromTask(async cts =>
        {
            RecordStatus(statusTrail, ref position, StartedCommand);
            await Task.Delay(CommandDelayMilliseconds, cts.Token).ConfigureAwait(true);
            if (!cts.IsCancellationRequested)
            {
                RecordStatus(statusTrail, ref position, FinishedCommandNormally);
            }

            return RxVoid.Default;
        }).Recover<RxVoid, Exception>(ex =>
        {
            RecordStatus(statusTrail, ref position, ExceptionShouldBeHere);
            return Signal.Fail<RxVoid>(ex);
        }).OnCleanup(() => RecordStatus(statusTrail, ref position, ShouldAlwaysComeHere));
        using var subscription = fixture.Subscribe();
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);
        await Assert.That(StatusMessages(statusTrail)).Contains(StartedCommand);
        subscription.Dispose();
        await Task.Delay(CancellationWaitDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(StatusMessages(statusTrail)).DoesNotContain(FinishedCommandNormally);
        await Assert.That(statusTrail.LastMessage).IsEqualTo(ShouldAlwaysComeHere);
    }

    /// <summary>Signals from task handles completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTaskHandlesCompletion()
    {
        StatusTrail statusTrail = new();
        var position = 0;
        var fixture = Signal.FromTask(async cts =>
        {
            RecordStatus(statusTrail, ref position, StartedCommand);
            await Task.Delay(CommandDelayMilliseconds, cts.Token)
                .HandleCancellation(() => RecordCancellationCleanup(statusTrail, ref position)).ConfigureAwait(true);
            if (!cts.IsCancellationRequested)
            {
                RecordStatus(statusTrail, ref position, FinishedCommandNormally);
            }

            return RxVoid.Default;
        }).Recover<RxVoid, Exception>(ex =>
        {
            RecordStatus(statusTrail, ref position, ExceptionShouldBeHere);
            return Signal.Fail<RxVoid>(ex);
        }).OnCleanup(() => RecordStatus(statusTrail, ref position, ShouldAlwaysComeHere));
        var result = false;
        using var subscription = fixture.Subscribe(_ => result = true);
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);
        await Assert.That(StatusMessages(statusTrail)).Contains(StartedCommand);
        await Task.Delay(CompletionWaitDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(StatusMessages(statusTrail)).DoesNotContain(StartingCancellingCommand);
        await Assert.That(StatusMessages(statusTrail)).DoesNotContain(FinishedCancellingCommand);
        await Assert.That(StatusMessages(statusTrail)).Contains(FinishedCommandNormally);
        await Assert.That(statusTrail.LastMessage).IsEqualTo(ShouldAlwaysComeHere);
        await Assert.That(result).IsTrue();
    }

    /// <summary>Signals from task t handles user exceptions.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTask_T_HandlesUserExceptions()
    {
        StatusTrail statusTrail = new();
        var position = 0;
        var fixture = Signal.FromTask<RxVoid>(async cts =>
        {
            RecordStatus(statusTrail, ref position, StartedCommand);
            await Task.Delay(CommandDelayMilliseconds, cts.Token)
                .HandleCancellation(() => RecordCancellationCleanup(statusTrail, ref position)).ConfigureAwait(true);
            if (!cts.IsCancellationRequested)
            {
                RecordStatus(statusTrail, ref position, FinishedCommandNormally);
            }

            throw new InvalidOperationException(BreakExecutionMessage);
        }).Recover<RxVoid, Exception>(ex =>
        {
            RecordStatus(statusTrail, ref position, ExceptionShouldBeHere);
            return Signal.Fail<RxVoid>(ex);
        }).OnCleanup(() => RecordStatus(statusTrail, ref position, ShouldAlwaysComeHere));
        var result = false;
        using var subscription = fixture.Subscribe(_ => result = true);
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);
        await Assert.That(StatusMessages(statusTrail)).Contains(StartedCommand);
        await Task.Delay(CommandDelayMilliseconds).ConfigureAwait(true);
        subscription.Dispose();
        await Task.Delay(CancellationWaitDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(StatusMessages(statusTrail)).DoesNotContain(StartingCancellingCommand);
        await Assert.That(StatusMessages(statusTrail)).Contains(ShouldAlwaysComeHere);
        await Assert.That(StatusMessages(statusTrail)).DoesNotContain(FinishedCancellingCommand);
        await Assert.That(StatusMessages(statusTrail)).Contains(ExceptionShouldBeHere);
        await Assert.That(StatusMessages(statusTrail)).Contains(FinishedCommandNormally);
        await Assert.That(result).IsFalse();
    }

    /// <summary>Signals from task t handles cancellation.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTask_T_HandlesCancellation()
    {
        StatusTrail statusTrail = new();
        var position = 0;
        var fixture = Signal.FromTask<RxVoid>(async cts =>
        {
            RecordStatus(statusTrail, ref position, StartedCommand);
            await Task.Delay(CommandDelayMilliseconds, cts.Token)
                .HandleCancellation(() => RecordCancellationCleanup(statusTrail, ref position)).ConfigureAwait(true);
            if (!cts.IsCancellationRequested)
            {
                RecordStatus(statusTrail, ref position, FinishedCommandNormally);
            }

            return RxVoid.Default;
        }).Recover<RxVoid, Exception>(ex =>
        {
            RecordStatus(statusTrail, ref position, ExceptionShouldBeHere);
            return Signal.Fail<RxVoid>(ex);
        }).OnCleanup(() => RecordStatus(statusTrail, ref position, ShouldAlwaysComeHere));
        var result = false;
        using var subscription = fixture.Subscribe(_ => result = true);
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);
        await Assert.That(StatusMessages(statusTrail)).Contains(StartedCommand);
        subscription.Dispose();
        await Task.Delay(CancellationWaitDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(StatusMessages(statusTrail)).Contains(StartingCancellingCommand);
        await Assert.That(StatusMessages(statusTrail)).Contains(ShouldAlwaysComeHere);
        await Assert.That(StatusMessages(statusTrail)).Contains(FinishedCancellingCommand);
        await Assert.That(StatusMessages(statusTrail)).DoesNotContain(FinishedCommandNormally);
        await Assert.That(result).IsFalse();
    }

    /// <summary>Signals from task t handles token cancellation.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTask_T_HandlesTokenCancellation()
    {
        StatusTrail statusTrail = new();
        TaskCompletionSource cleanupCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource finallyCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var position = 0;
        var fixture = Signal.FromTask<RxVoid>(async cts =>
        {
            RecordStatus(statusTrail, ref position, StartedCommand);
            await Task.Delay(TokenCancellationDelayMilliseconds, cts.Token).HandleCancellation().ConfigureAwait(true);
            var cancellationTask = CancelAfterDelayAsync(cts);
            await Task.Delay(CleanupDelayMilliseconds, cts.Token).HandleCancellation(() =>
            {
                RecordCancellationCleanup(statusTrail, ref position);
                _ = cleanupCompleted.TrySetResult();
            }).ConfigureAwait(true);
            await cancellationTask.ConfigureAwait(false);
            if (!cts.IsCancellationRequested)
            {
                RecordStatus(statusTrail, ref position, FinishedCommandNormally);
            }

            return RxVoid.Default;
        }).Recover<RxVoid, Exception>(ex =>
        {
            RecordStatus(statusTrail, ref position, ExceptionShouldBeHere);
            return Signal.Fail<RxVoid>(ex);
        }).OnCleanup(() =>
        {
            RecordStatus(statusTrail, ref position, ShouldAlwaysComeHere);
            _ = finallyCompleted.TrySetResult();
        });
        var result = false;
        using var subscription = fixture.Subscribe(_ => result = true);
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);
        await Assert.That(StatusMessages(statusTrail)).Contains(StartedCommand);
        await WaitForAsync(
            Task.WhenAll(cleanupCompleted.Task, finallyCompleted.Task),
            CancellationCallbackTimeoutMilliseconds).ConfigureAwait(false);
        await Assert.That(StatusMessages(statusTrail)).Contains(StartingCancellingCommand);
        await Assert.That(StatusMessages(statusTrail)).Contains(ShouldAlwaysComeHere);
        await Assert.That(StatusMessages(statusTrail)).Contains(FinishedCancellingCommand);
        await Assert.That(StatusMessages(statusTrail)).DoesNotContain(FinishedCommandNormally);
        await Assert.That(result).IsFalse();
    }

    /// <summary>Signals from task t handles cancellation in base.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTask_T_HandlesCancellationInBase()
    {
        StatusTrail statusTrail = new();
        var position = 0;
        var fixture = Signal.FromTask<RxVoid>(async cts =>
        {
            RecordStatus(statusTrail, ref position, StartedCommand);
            await Task.Delay(CommandDelayMilliseconds, cts.Token).ConfigureAwait(true);
            if (!cts.IsCancellationRequested)
            {
                RecordStatus(statusTrail, ref position, FinishedCommandNormally);
            }

            return RxVoid.Default;
        }).Recover<RxVoid, Exception>(ex =>
        {
            RecordStatus(statusTrail, ref position, ExceptionShouldBeHere);
            return Signal.Fail<RxVoid>(ex);
        }).OnCleanup(() => RecordStatus(statusTrail, ref position, ShouldAlwaysComeHere));
        using var subscription = fixture.Subscribe();
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);
        await Assert.That(StatusMessages(statusTrail)).Contains(StartedCommand);
        subscription.Dispose();
        await Task.Delay(CancellationWaitDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(StatusMessages(statusTrail)).DoesNotContain(FinishedCommandNormally);
        await Assert.That(statusTrail.LastMessage).IsEqualTo(ShouldAlwaysComeHere);
    }

    /// <summary>Signals from task t handles completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task SignalFromTask_T_HandlesCompletion()
    {
        StatusTrail statusTrail = new();
        var position = 0;
        var fixture = Signal.FromTask<RxVoid>(async cts =>
        {
            RecordStatus(statusTrail, ref position, StartedCommand);
            await Task.Delay(CommandDelayMilliseconds, cts.Token)
                .HandleCancellation(() => RecordCancellationCleanup(statusTrail, ref position)).ConfigureAwait(true);
            if (!cts.IsCancellationRequested)
            {
                RecordStatus(statusTrail, ref position, FinishedCommandNormally);
            }

            return RxVoid.Default;
        }).Recover<RxVoid, Exception>(ex =>
        {
            RecordStatus(statusTrail, ref position, ExceptionShouldBeHere);
            return Signal.Fail<RxVoid>(ex);
        }).OnCleanup(() => RecordStatus(statusTrail, ref position, ShouldAlwaysComeHere));
        var result = false;
        using var subscription = fixture.Subscribe(_ => result = true);
        await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(true);
        await Assert.That(StatusMessages(statusTrail)).Contains(StartedCommand);
        await Task.Delay(CompletionWaitDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(StatusMessages(statusTrail)).DoesNotContain(StartingCancellingCommand);
        await Assert.That(StatusMessages(statusTrail)).DoesNotContain(FinishedCancellingCommand);
        await Assert.That(StatusMessages(statusTrail)).Contains(FinishedCommandNormally);
        await Assert.That(statusTrail.LastMessage).IsEqualTo(ShouldAlwaysComeHere);
        await Assert.That(result).IsTrue();
    }

    /// <summary>Gets the recorded status messages.</summary>
    /// <param name = "statusTrail">The status trail.</param>
    /// <returns>The recorded messages.</returns>
    private static string[] StatusMessages(StatusTrail statusTrail) => statusTrail.Messages();

    /// <summary>Waits for a timed test callback to complete.</summary>
    /// <param name = "task">The task to await.</param>
    /// <param name = "timeoutMilliseconds">The timeout in milliseconds.</param>
    /// <returns>A <see cref = "Task"/> representing the asynchronous operation.</returns>
    private static async Task WaitForAsync(Task task, int timeoutMilliseconds)
    {
        var timeout = Task.Delay(timeoutMilliseconds);
        var completed = await Task.WhenAny(task, timeout).ConfigureAwait(false);
        if (completed == timeout)
        {
            throw new TimeoutException($"Timed out after {timeoutMilliseconds}ms waiting for cancellation callbacks.");
        }

        await task.ConfigureAwait(false);
    }

    /// <summary>Records a status message.</summary>
    /// <param name = "statusTrail">The status trail.</param>
    /// <param name = "position">The current status position.</param>
    /// <param name = "message">The message to record.</param>
    private static void RecordStatus(StatusTrail statusTrail, ref int position, string message) =>
        statusTrail.Add(ref position, message);

    /// <summary>Records synchronous cancellation cleanup.</summary>
    /// <param name = "statusTrail">The status trail.</param>
    /// <param name = "position">The current status position.</param>
    private static void RecordCancellationCleanup(StatusTrail statusTrail, ref int position)
    {
        RecordStatus(statusTrail, ref position, StartingCancellingCommand);
        Thread.Sleep(CleanupWorkMilliseconds);
        RecordStatus(statusTrail, ref position, FinishedCancellingCommand);
    }

    /// <summary>Cancels the source after the token cancellation delay.</summary>
    /// <param name = "cts">The cancellation source.</param>
    /// <returns>A <see cref = "Task"/> representing the asynchronous operation.</returns>
    private static async Task CancelAfterDelayAsync(CancellationTokenSource cts)
    {
        await Task.Delay(TokenCancellationDelayMilliseconds).ConfigureAwait(false);
        await cts.CancelAsync().ConfigureAwait(false);
    }

    /// <summary>Thread-safe status trail used by async cancellation tests.</summary>
    private sealed class StatusTrail
    {
        /// <summary>Synchronizes access to the recorded statuses.</summary>
        private readonly Lock _gate = new();

        /// <summary>Stores the recorded status positions and messages.</summary>
        private readonly List<(int Position, string Message)> _items = [];

        /// <summary>Gets the last recorded status message.</summary>
        public string LastMessage
        {
            get
            {
                lock (_gate)
                {
                    return _items[^1].Message;
                }
            }
        }

        /// <summary>Adds a status message.</summary>
        /// <param name = "position">The current status position.</param>
        /// <param name = "message">The message.</param>
        public void Add(ref int position, string message)
        {
            lock (_gate)
            {
                _items.Add((position++, message));
            }
        }

        /// <summary>Creates a snapshot of the recorded messages.</summary>
        /// <returns>The message snapshot.</returns>
        public string[] Messages()
        {
            lock (_gate)
            {
                var messages = new string[_items.Count];
                for (var i = 0; i < messages.Length; i++)
                {
                    messages[i] = _items[i].Message;
                }

                return messages;
            }
        }
    }
}
