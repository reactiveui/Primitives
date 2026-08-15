// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;
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

    /// <summary>
    /// Maximum wait for cancellation callbacks that are intentionally driven by timers. The callbacks fire on pool
    /// threads, so on a saturated runner they can arrive far later than on an idle box; a cancellation that never
    /// fires the callbacks never signals, so this window only has to outlast a slow runner and costs nothing when
    /// the callbacks arrive promptly.
    /// </summary>
    private const int CancellationCallbackTimeoutMilliseconds = 30_000;

    /// <summary>Message carried by a failure an observer raises from inside a notification.</summary>
    private const string ObserverFailureMessage = "observer failed";

    /// <summary>Delay used before checking that a task has started.</summary>
    private const int InitialDelayMilliseconds = 500;

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

    /// <summary>How long a poll waits for an observed notification before failing.</summary>
    private static readonly TimeSpan PollTimeout = TimeSpan.FromSeconds(2);

    /// <summary>Covers from-task cancellation callback argument validation.</summary>
    [Test]
    public void FromTaskValidatesCancellationCallback()
    {
        var taskSignal = Signal.FromTask(static _ => Task.FromResult(1), Sequencer.Immediate);
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
        var taskSignal = Signal.FromTask(static _ => Task.FromResult(EmittedValue), Sequencer.Immediate);
        try
        {
            List<int> taskValues = [];
            var taskCompleted = 0;
            _ = taskSignal.Subscribe(taskValues.Add, static error => throw error, () => taskCompleted++);
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

        TaskCompletionSource<int> success = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<int> fault = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<int> canceled = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<int> disposed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var disposedSubscription = Signal.FromTask(disposed.Task).Subscribe(_ => AddValue(DisposedValue), AddError);
        disposedSubscription.Dispose();
        _ = Signal.FromTask(success.Task).Subscribe(AddValue, AddError);
        _ = Signal.FromTask(fault.Task).Subscribe(AddValue, AddError);
        _ = Signal.FromTask(canceled.Task).Subscribe(AddValue, AddError);
        success.SetResult(SuccessValue);
        fault.SetException(new InvalidOperationException("pending-fault"));
        canceled.SetCanceled(new(true));
        disposed.SetResult(DisposedValue);
        await TestPolling.SpinUntil(ObservedPendingBranches, PollTimeout).ConfigureAwait(false);
        var finalValues = values.ToArray();
        var finalErrors = errors.ToArray();
        await Assert.That(finalValues.Length).IsEqualTo(1);
        await Assert.That(finalValues[0]).IsEqualTo(SuccessValue);
        await Assert.That(finalErrors).Contains(nameof(InvalidOperationException));
        await Assert.That(finalErrors).Contains(nameof(TaskCanceledException));
    }

    /// <summary>A synchronously completed task emits its result and completes through the immediate path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task ImmediateSynchronousSuccessEmitsResultAndCompletes()
    {
        ConcurrentQueue<int> values = new();
        ConcurrentQueue<string> errors = new();
        var completed = 0;
        var taskSignal = Signal.FromTask(static _ => Task.FromResult(SuccessValue), Sequencer.Immediate);
        try
        {
            _ = taskSignal.Subscribe(
                values.Enqueue,
                error => errors.Enqueue(error.GetType().Name),
                () => Interlocked.Increment(ref completed));
            await Assert.That(values.SequenceEqual([SuccessValue])).IsTrue();
            await Assert.That(errors).IsEmpty();
            await Assert.That(Volatile.Read(ref completed)).IsEqualTo(1);
        }
        finally
        {
            (taskSignal as IDisposable)?.Dispose();
        }
    }

    /// <summary>A synchronously canceled task errors with a cancellation through the immediate path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task ImmediateSynchronousCanceledTaskErrors()
    {
        ConcurrentQueue<int> values = new();
        ConcurrentQueue<string> errors = new();
        var taskSignal = Signal.FromTask(static _ => Task.FromCanceled<int>(new(true)), Sequencer.Immediate);
        try
        {
            _ = taskSignal.Subscribe(values.Enqueue, error => errors.Enqueue(error.GetType().Name), static () => { });
            await Assert.That(values).IsEmpty();
            await Assert.That(errors.SequenceEqual([nameof(OperationCanceledException)])).IsTrue();
        }
        finally
        {
            (taskSignal as IDisposable)?.Dispose();
        }
    }

    /// <summary>A synchronously faulted task forwards the exception through the immediate path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task ImmediateSynchronousFaultedTaskForwardsError()
    {
        ConcurrentQueue<int> values = new();
        ConcurrentQueue<string> errors = new();
        var taskSignal =
            Signal.FromTask(
                static _ => Task.FromException<int>(new InvalidOperationException(BreakExecutionMessage)),
                Sequencer.Immediate);
        try
        {
            _ = taskSignal.Subscribe(values.Enqueue, error => errors.Enqueue(error.GetType().Name), static () => { });
            await Assert.That(values).IsEmpty();
            await Assert.That(errors.SequenceEqual([nameof(InvalidOperationException)])).IsTrue();
        }
        finally
        {
            (taskSignal as IDisposable)?.Dispose();
        }
    }

    /// <summary>A throwing task factory forwards the exception through the immediate path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task ImmediateFactoryThrowForwardsError()
    {
        ConcurrentQueue<int> values = new();
        ConcurrentQueue<string> errors = new();
        var taskSignal = Signal.FromTask<int>(
            static _ => throw new InvalidOperationException(BreakExecutionMessage),
            Sequencer.Immediate);
        try
        {
            _ = taskSignal.Subscribe(values.Enqueue, error => errors.Enqueue(error.GetType().Name), static () => { });
            await Assert.That(values).IsEmpty();
            await Assert.That(errors.SequenceEqual([nameof(InvalidOperationException)])).IsTrue();
        }
        finally
        {
            (taskSignal as IDisposable)?.Dispose();
        }
    }

    /// <summary>A throwing task factory forwards the exception through the scheduled path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task ScheduledFactoryThrowForwardsError()
    {
        ConcurrentQueue<int> values = new();
        ConcurrentQueue<string> errors = new();
        var taskSignal = Signal.FromTask<int>(static _ => throw new InvalidOperationException(BreakExecutionMessage));
        try
        {
            _ = taskSignal.Subscribe(values.Enqueue, error => errors.Enqueue(error.GetType().Name), static () => { });
            await TestPolling.SpinUntil(() => !errors.IsEmpty, PollTimeout).ConfigureAwait(false);
            await Assert.That(values).IsEmpty();
            await Assert.That(errors.SequenceEqual([nameof(InvalidOperationException)])).IsTrue();
        }
        finally
        {
            (taskSignal as IDisposable)?.Dispose();
        }
    }

    /// <summary>A synchronously completed task emits its result through the scheduled synchronous fast path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task ScheduledSynchronousSuccessEmitsResultAndCompletes()
    {
        ConcurrentQueue<int> values = new();
        ConcurrentQueue<string> errors = new();
        var completed = 0;
        var taskSignal = Signal.FromTask(static _ => Task.FromResult(SuccessValue), Sequencer.CurrentThread);
        try
        {
            _ = taskSignal.Subscribe(
                values.Enqueue,
                error => errors.Enqueue(error.GetType().Name),
                () => Interlocked.Increment(ref completed));
            await TestPolling.SpinUntil(() => Volatile.Read(ref completed) == 1, PollTimeout)
                .ConfigureAwait(false);
            await Assert.That(values.SequenceEqual([SuccessValue])).IsTrue();
            await Assert.That(errors).IsEmpty();
        }
        finally
        {
            (taskSignal as IDisposable)?.Dispose();
        }
    }

    /// <summary>A synchronously canceled task errors through the scheduled synchronous fast path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task ScheduledSynchronousCanceledTaskErrors()
    {
        ConcurrentQueue<int> values = new();
        ConcurrentQueue<string> errors = new();
        var taskSignal = Signal.FromTask(static _ => Task.FromCanceled<int>(new(true)), Sequencer.CurrentThread);
        try
        {
            _ = taskSignal.Subscribe(values.Enqueue, error => errors.Enqueue(error.GetType().Name), static () => { });
            await TestPolling.SpinUntil(() => !errors.IsEmpty, PollTimeout).ConfigureAwait(false);
            await Assert.That(values).IsEmpty();
            await Assert.That(errors.SequenceEqual([nameof(OperationCanceledException)])).IsTrue();
        }
        finally
        {
            (taskSignal as IDisposable)?.Dispose();
        }
    }

    /// <summary>Disposing a subscription whose cancellation source was already disposed swallows the resulting error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task DisposeAfterCancellationSourceDisposedSwallowsObjectDisposedException()
    {
        ConcurrentQueue<int> values = new();
        ConcurrentQueue<string> errors = new();
        TaskCompletionSource<int> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var cts = new CancellationTokenSource();
        var taskSignal = Signal.FromTask(_ => gate.Task, Sequencer.Immediate, cts);
        try
        {
            var subscription =
                taskSignal.Subscribe(values.Enqueue, error => errors.Enqueue(error.GetType().Name), static () => { });

            // Dispose the cancellation source out from under the subscription, then dispose the
            // subscription. The disposer wins the gate and calls Cancel on the disposed source,
            // which must swallow the ObjectDisposedException.
            cts.Dispose();
            subscription.Dispose();

            gate.SetResult(SuccessValue);
            await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(false);
            await Assert.That(values).IsEmpty();
            await Assert.That(errors).IsEmpty();
        }
        finally
        {
            (taskSignal as IDisposable)?.Dispose();
        }
    }

    /// <summary>A pending task that faults after subscription forwards the exception via the continuation through the immediate path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task ImmediatePendingTaskFaultForwardsErrorViaContinuation()
    {
        ConcurrentQueue<int> values = new();
        ConcurrentQueue<string> errors = new();
        TaskCompletionSource<int> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var taskSignal = Signal.FromTask(_ => gate.Task, Sequencer.Immediate);
        try
        {
            _ = taskSignal.Subscribe(values.Enqueue, error => errors.Enqueue(error.GetType().Name), static () => { });
            gate.SetException(new InvalidOperationException(BreakExecutionMessage));
            await TestPolling.SpinUntil(() => !errors.IsEmpty, PollTimeout).ConfigureAwait(false);
            await Assert.That(values).IsEmpty();
            await Assert.That(errors.SequenceEqual([nameof(InvalidOperationException)])).IsTrue();
        }
        finally
        {
            (taskSignal as IDisposable)?.Dispose();
        }
    }

    /// <summary>A pending task that completes after subscription emits the result via the continuation through the immediate path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task ImmediatePendingTaskSuccessEmitsResultViaContinuation()
    {
        ConcurrentQueue<int> values = new();
        ConcurrentQueue<string> errors = new();
        var completed = 0;
        TaskCompletionSource<int> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var taskSignal = Signal.FromTask(_ => gate.Task, Sequencer.Immediate);
        try
        {
            _ = taskSignal.Subscribe(
                values.Enqueue,
                error => errors.Enqueue(error.GetType().Name),
                () => Interlocked.Increment(ref completed));
            gate.SetResult(SuccessValue);
            await TestPolling.SpinUntil(() => Volatile.Read(ref completed) == 1, PollTimeout)
                .ConfigureAwait(false);
            await Assert.That(values.SequenceEqual([SuccessValue])).IsTrue();
            await Assert.That(errors).IsEmpty();
            await Assert.That(Volatile.Read(ref completed)).IsEqualTo(1);
        }
        finally
        {
            (taskSignal as IDisposable)?.Dispose();
        }
    }

    /// <summary>Disposing the immediate subscription before the awaited task completes suppresses the terminal notification.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task DisposeBeforeImmediateTaskCompletionSuppressesTerminalNotification()
    {
        ConcurrentQueue<int> values = new();
        ConcurrentQueue<string> errors = new();
        var completed = 0;
        TaskCompletionSource<int> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var taskSignal = Signal.FromTask(_ => gate.Task, Sequencer.Immediate);
        try
        {
            var subscription = taskSignal.Subscribe(
                values.Enqueue,
                error => errors.Enqueue(error.GetType().Name),
                () => Interlocked.Increment(ref completed));

            // Dispose while the awaited task is still pending, then release the continuation.
            subscription.Dispose();
            gate.SetResult(SuccessValue);

            // Give the continuation ample time to run; it must observe the dispose and stay silent.
            await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(false);
            await Assert.That(values).IsEmpty();
            await Assert.That(errors).IsEmpty();
            await Assert.That(Volatile.Read(ref completed)).IsEqualTo(0);
        }
        finally
        {
            (taskSignal as IDisposable)?.Dispose();
        }
    }

    /// <summary>Disposing the scheduled subscription before the awaited task completes suppresses the terminal notification.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task DisposeBeforeScheduledTaskCompletionSuppressesTerminalNotification()
    {
        ConcurrentQueue<int> values = new();
        ConcurrentQueue<string> errors = new();
        var completed = 0;
        TaskCompletionSource<int> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var taskSignal = Signal.FromTask(_ => gate.Task);
        try
        {
            var subscription = taskSignal.Subscribe(
                values.Enqueue,
                error => errors.Enqueue(error.GetType().Name),
                () => Interlocked.Increment(ref completed));

            // Dispose while the awaited task is still pending, then release the continuation.
            subscription.Dispose();
            gate.SetResult(SuccessValue);

            // Give the continuation ample time to run; it must observe the dispose and stay silent.
            await Task.Delay(InitialDelayMilliseconds).ConfigureAwait(false);
            await Assert.That(values).IsEmpty();
            await Assert.That(errors).IsEmpty();
            await Assert.That(Volatile.Read(ref completed)).IsEqualTo(0);
        }
        finally
        {
            (taskSignal as IDisposable)?.Dispose();
        }
    }

    /// <summary>The non-generic RxVoid factory honors the scheduler overload and emits a completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task RxVoidFactoryWithSchedulerEmitsCompletion()
    {
        var completed = 0;
        var taskSignal = Signal.FromTask(static _ => Task.FromResult(RxVoid.Default), Sequencer.Immediate);
        try
        {
            _ = taskSignal.Subscribe(static _ => { }, static error => throw error, () => Interlocked.Increment(ref completed));
            await Assert.That(Volatile.Read(ref completed)).IsEqualTo(1);
        }
        finally
        {
            (taskSignal as IDisposable)?.Dispose();
        }
    }

    /// <summary>The immediate signal reports cancellation and disposal state and fires the cancellation callback once disposed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task ImmediateSignalReportsStateAndFiresCancellationCallbackOnDispose()
    {
        TaskCompletionSource<int> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var taskSignal = Signal.FromTask(_ => gate.Task, Sequencer.Immediate);
        var canceledRaised = 0;
        await Assert.That(taskSignal.IsCancellationRequested).IsFalse();
        await Assert.That(taskSignal.IsDisposed).IsFalse();
        taskSignal.GetOperationCanceled(Witness.Create<Exception>(_ => Interlocked.Increment(ref canceledRaised)));

        ((IDisposable)taskSignal).Dispose();

        await Assert.That(taskSignal.IsDisposed).IsTrue();
        await Assert.That(taskSignal.IsCancellationRequested).IsTrue();
        await TestPolling.SpinUntil(() => Volatile.Read(ref canceledRaised) == 1, PollTimeout)
            .ConfigureAwait(false);
        await Assert.That(Volatile.Read(ref canceledRaised)).IsEqualTo(1);

        // A second dispose is a no-op (covers the already-disposed early return).
        ((IDisposable)taskSignal).Dispose();
        await Assert.That(taskSignal.IsDisposed).IsTrue();
    }

    /// <summary>Subscribing to a disposed immediate signal throws.</summary>
    [Test]
    public void ImmediateSignalSubscribeAfterDisposeThrows()
    {
        var taskSignal = Signal.FromTask(static _ => Task.FromResult(SuccessValue), Sequencer.Immediate);
        ((IDisposable)taskSignal).Dispose();
        _ = Assert.Throws<ObjectDisposedException>(() => taskSignal.Subscribe(static _ => { }));
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
        var subscription = fixture.Subscribe(_ => result = true);
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
        var subscription = fixture.Subscribe(_ => result = true);
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
        TaskCompletionSource<CancellationTokenSource> cancellationReady =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource cleanupCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource finallyCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var position = 0;
        var fixture = Signal.FromTask(async cts =>
        {
            RecordStatus(statusTrail, ref position, StartedCommand);
            _ = cancellationReady.TrySetResult(cts);
            await Task.Delay(Timeout.InfiniteTimeSpan, cts.Token).HandleCancellation(() =>
            {
                RecordCancellationCleanup(statusTrail, ref position);
                _ = cleanupCompleted.TrySetResult();
            }).ConfigureAwait(true);
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
        var cancellationSource = await cancellationReady.Task.ConfigureAwait(false);
        await Assert.That(StatusMessages(statusTrail)).Contains(StartedCommand);
        await cancellationSource.CancelAsync().ConfigureAwait(false);
        await WaitForCancellationCallbacks(cleanupCompleted.Task, finallyCompleted.Task).ConfigureAwait(false);
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
        var subscription = fixture.Subscribe();
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
        TaskCompletionSource executionStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource releaseExecution = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource finallyCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var position = 0;
        var fixture = Signal.FromTask<RxVoid>(async cts =>
        {
            RecordStatus(statusTrail, ref position, StartedCommand);
            _ = executionStarted.TrySetResult();
            await releaseExecution.Task.WaitAsync(cts.Token)
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
        }).OnCleanup(() =>
        {
            RecordStatus(statusTrail, ref position, ShouldAlwaysComeHere);
            _ = finallyCompleted.TrySetResult();
        });
        var result = false;
        using var subscription = fixture.Subscribe(_ => result = true);
        await executionStarted.Task.WaitAsync(PollTimeout).ConfigureAwait(false);
        await Assert.That(StatusMessages(statusTrail)).Contains(StartedCommand);
        _ = releaseExecution.TrySetResult();
        await finallyCompleted.Task.WaitAsync(PollTimeout).ConfigureAwait(false);
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
        var subscription = fixture.Subscribe(_ => result = true);
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
        TaskCompletionSource<CancellationTokenSource> cancellationReady =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource cleanupCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource finallyCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var position = 0;
        var fixture = Signal.FromTask<RxVoid>(async cts =>
        {
            RecordStatus(statusTrail, ref position, StartedCommand);
            _ = cancellationReady.TrySetResult(cts);
            await Task.Delay(Timeout.InfiniteTimeSpan, cts.Token).HandleCancellation(() =>
            {
                RecordCancellationCleanup(statusTrail, ref position);
                _ = cleanupCompleted.TrySetResult();
            }).ConfigureAwait(true);
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
        var cancellationSource = await cancellationReady.Task.WaitAsync(PollTimeout).ConfigureAwait(false);
        await Assert.That(StatusMessages(statusTrail)).Contains(StartedCommand);
        await cancellationSource.CancelAsync().ConfigureAwait(false);
        await WaitForCancellationCallbacks(cleanupCompleted.Task, finallyCompleted.Task).ConfigureAwait(false);
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
        var subscription = fixture.Subscribe();
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

    /// <summary>
    /// Verifies the immediate task signal exposes itself as its own source and owns a cancellation source, and
    /// that a task whose token was cancelled before it produced its result errors instead of emitting it.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task ImmediateTaskSignalErrorsWhenTheTokenIsCancelledBeforeTheResultArrives()
    {
        TaskCompletionSource<int> completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var taskSignal = Signal.FromTask(_ => completion.Task, Sequencer.Immediate);
        try
        {
            await Assert.That(taskSignal.CancellationTokenSource).IsNotNull();
            await Assert.That(ReferenceEquals(taskSignal.Source, taskSignal)).IsTrue();
            ConcurrentQueue<int> values = new();
            ConcurrentQueue<Exception> errors = new();
            _ = taskSignal.Subscribe(values.Enqueue, errors.Enqueue, static () => { });
            await taskSignal.CancellationTokenSource!.CancelAsync();
            completion.SetResult(SuccessValue);
            await TestPolling.SpinUntil(() => !errors.IsEmpty, PollTimeout).ConfigureAwait(false);
            await Assert.That(errors.Count).IsEqualTo(1);
            _ = errors.TryPeek(out var error);
            await Assert.That(error!).IsTypeOf<OperationCanceledException>();
            await Assert.That(values).IsEmpty();
        }
        finally
        {
            (taskSignal as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// Verifies a task that faults after its subscription was disposed notifies nobody. The task continuation still
    /// runs, and it must find the subscription already stopped rather than push a terminal at an observer that has
    /// unsubscribed.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task FromTaskDropsAFaultRaisedAfterTheSubscriptionWasDisposed()
    {
        TaskCompletionSource<int> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        RecordingObserver observer = new();

        var subscription = Signal.FromTask(pending.Task).Subscribe(observer);
        subscription.Dispose();

        pending.SetException(new InvalidOperationException(ObserverFailureMessage));

        // The continuation runs on the thread pool; give it room to reach the stopped subscription.
        await Task.Delay(PollTimeout);

        await Assert.That(observer.ErrorCount).IsEqualTo(0);
        await Assert.That(observer.NextCount).IsEqualTo(0);
        await Assert.That(observer.CompletedCount).IsEqualTo(0);
    }

    /// <summary>
    /// Verifies an observer that throws out of its own <see cref = "IObserver{T}.OnNext"/> does not get a second
    /// terminal. The task's terminal was already claimed before the value was pushed, so the observer's failure is
    /// swallowed rather than turned into an <see cref = "IObserver{T}.OnError"/> the observer never expected.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task FromAsyncSwallowsAFailureTheObserverRaisedFromOnNext()
    {
        TaskCompletionSource<int> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        ThrowingObserver observer = new(new InvalidOperationException(ObserverFailureMessage));

        using var subscription = Signal.FromAsync(_ => pending.Task).Subscribe(observer);
        pending.SetResult(SuccessValue);

        await Assert.That(observer.Observed.Task.WaitAsync(PollTimeout)).ThrowsNothing();
        await Task.Delay(PollTimeout);

        await Assert.That(observer.NextCount).IsEqualTo(1);
        await Assert.That(observer.ErrorCount).IsEqualTo(0);
        await Assert.That(observer.CompletedCount).IsEqualTo(0);
    }

    /// <summary>
    /// Verifies an observer that disposes its own subscription and then throws is met with silence. Disposal owns that
    /// cancellation path, so the failure must not be reported back down a subscription the observer has just torn down.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task FromAsyncStaysSilentWhenTheObserverDisposesItselfAndThenThrows()
    {
        TaskCompletionSource<int> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        SelfDisposingThrowingObserver observer = new(new InvalidOperationException(ObserverFailureMessage));

        observer.Subscription = Signal.FromAsync(_ => pending.Task).Subscribe(observer);
        pending.SetResult(SuccessValue);

        await Assert.That(observer.Observed.Task.WaitAsync(PollTimeout)).ThrowsNothing();
        await Task.Delay(PollTimeout);

        await Assert.That(observer.NextCount).IsEqualTo(1);
        await Assert.That(observer.ErrorCount).IsEqualTo(0);
        await Assert.That(observer.CompletedCount).IsEqualTo(0);
    }

    /// <summary>
    /// Verifies the external-cancellation overload still runs the factory when the caller passes a token that can never
    /// be cancelled. There is nothing to register against, so the subscription must start rather than register a
    /// callback on a token that will never fire.
    /// </summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous operation.</returns>
    [Test]
    public async Task FromAsyncWithAnUncancellableTokenStillRunsTheFactory()
    {
        RecordingObserver observer = new();

        using var subscription = Signal
            .FromAsync(static _ => Task.FromResult(SuccessValue), CancellationToken.None)
            .Subscribe(observer);

        await Assert.That(observer.NextCount).IsEqualTo(1);
        await Assert.That(observer.LastValue).IsEqualTo(SuccessValue);
        await Assert.That(observer.CompletedCount).IsEqualTo(1);
        await Assert.That(observer.ErrorCount).IsEqualTo(0);
    }

    /// <summary>Gets the recorded status messages.</summary>
    /// <param name = "statusTrail">The status trail.</param>
    /// <returns>The recorded messages.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static string[] StatusMessages(StatusTrail statusTrail) => statusTrail.Messages();

    /// <summary>Awaits both timer-driven cancellation callbacks, or fails once the generous window closes.</summary>
    /// <param name = "cleanupCompleted">Completes when the cancellation cleanup callback has run.</param>
    /// <param name = "finallyCompleted">Completes when the terminal cleanup callback has run.</param>
    /// <returns>A <see cref = "Task"/> representing the asynchronous operation.</returns>
    /// <exception cref = "TimeoutException">Neither callback had run once the generous wait window closed.</exception>
    /// <remarks>
    /// The callbacks are driven by timers and run on pool threads, so the wait must stay asynchronous: blocking a pool
    /// thread would starve the very chain it is waiting on. It also must not decide the outcome from which continuation
    /// the pool dequeues first — <c>Task.WhenAny</c> reports whichever continuation ran first, not whichever task
    /// completed first, so on a saturated runner the timeout can be reported as the winner even though the callbacks
    /// already fired. This awaits the race for liveness but then decides from the callback tasks' own completion state,
    /// which <see cref = "TaskCompletionSource.TrySetResult"/> flips synchronously and no pool pressure can misreport.
    /// A cancellation that never fires the callbacks never completes, so the generous window still fails on a real hang.
    /// </remarks>
    private static async Task WaitForCancellationCallbacks(Task cleanupCompleted, Task finallyCompleted)
    {
        var callbacks = Task.WhenAll(cleanupCompleted, finallyCompleted);
        _ = await Task.WhenAny(callbacks, Task.Delay(CancellationCallbackTimeoutMilliseconds)).ConfigureAwait(false);
        if (!cleanupCompleted.IsCompleted || !finallyCompleted.IsCompleted)
        {
            throw new TimeoutException(
                $"Timed out after {CancellationCallbackTimeoutMilliseconds}ms waiting for cancellation callbacks.");
        }

        await callbacks.ConfigureAwait(false);
    }

    /// <summary>Records a status message.</summary>
    /// <param name = "statusTrail">The status trail.</param>
    /// <param name = "position">The current status position.</param>
    /// <param name = "message">The message to record.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

    /// <summary>Observer that counts the notifications it received.</summary>
    private class RecordingObserver : IObserver<int>
    {
        /// <summary>Gets the number of values received.</summary>
        public int NextCount { get; private set; }

        /// <summary>Gets the number of failures received.</summary>
        public int ErrorCount { get; private set; }

        /// <summary>Gets the number of completions received.</summary>
        public int CompletedCount { get; private set; }

        /// <summary>Gets the most recently received value.</summary>
        public int LastValue { get; private set; }

        /// <inheritdoc/>
        public virtual void OnNext(int value)
        {
            NextCount++;
            LastValue = value;
        }

        /// <inheritdoc/>
        public void OnError(Exception error) => ErrorCount++;

        /// <inheritdoc/>
        public void OnCompleted() => CompletedCount++;
    }

    /// <summary>Observer that throws out of <see cref = "IObserver{T}.OnNext"/> after recording the value.</summary>
    /// <param name = "failure">The failure the observer raises.</param>
    private class ThrowingObserver(Exception failure) : RecordingObserver
    {
        /// <summary>The failure raised from the value notification.</summary>
        private readonly Exception _failure = failure;

        /// <summary>Gets a task that completes once the observer has seen its value.</summary>
        public TaskCompletionSource Observed { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <inheritdoc/>
        public override void OnNext(int value)
        {
            base.OnNext(value);
            _ = Observed.TrySetResult();
            throw _failure;
        }
    }

    /// <summary>Observer that tears its own subscription down before it throws.</summary>
    /// <param name = "failure">The failure the observer raises.</param>
    private sealed class SelfDisposingThrowingObserver(Exception failure) : ThrowingObserver(failure)
    {
        /// <summary>Gets or sets the subscription the observer disposes from inside its value notification.</summary>
        public IDisposable? Subscription { get; set; }

        /// <inheritdoc/>
        public override void OnNext(int value)
        {
            Subscription?.Dispose();
            base.OnNext(value);
        }
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
                _items.Add((position, message));
                position++;
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
