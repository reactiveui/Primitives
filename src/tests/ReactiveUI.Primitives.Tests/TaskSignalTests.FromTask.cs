// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections.Concurrent;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies synchronous task factory results and validation.</summary>
public partial class TaskSignalTests
{
    /// <summary>The value emitted by the simple from-task result test.</summary>
    private const int EmittedValue = 2;

    /// <summary>The value produced by the successful pending task.</summary>
    private const int SuccessValue = 7;

    /// <summary>Exception message used by user exception tests.</summary>
    private const string BreakExecutionMessage = "break execution";

    /// <summary>A null cancellation callback is rejected.</summary>
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

    /// <summary>A from-task signal emits the task result and completes.</summary>
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
            await Assert.That(values.SequenceEqual([SuccessValue])).IsTrue();
            await Assert.That(errors).IsEmpty();
            await Assert.That(completed).IsEqualTo(1);
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
            await Assert.That(values).IsEmpty();
            await Assert.That(errors.SequenceEqual([nameof(OperationCanceledException)])).IsTrue();
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

    /// <summary>A factory that returns no task reports an invalid operation on both delivery paths.</summary>
    /// <param name="immediate">Whether the signal delivers on the immediate sequencer.</param>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task FactoryReturningNoTaskForwardsInvalidOperation(bool immediate)
    {
        ConcurrentQueue<Exception> errors = new();
        var taskSignal = Signal.FromTask<int>(
            static _ => null!,
            immediate ? Sequencer.Immediate : (ISequencer)Sequencer.CurrentThread);
        try
        {
            _ = taskSignal.Subscribe(static _ => { }, errors.Enqueue, static () => { });
            await Assert.That(errors.Count).IsEqualTo(1);
            await Assert.That(errors.Single()).IsTypeOf<InvalidOperationException>();
        }
        finally
        {
            (taskSignal as IDisposable)?.Dispose();
        }
    }

    /// <summary>The immediate factory rejects a missing execution delegate.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous unit test.</returns>
    [Test]
    public async Task ImmediateFactoryRejectsAMissingExecution() =>
        await Assert.That(static () => Signal.FromTask<int>(null!, Sequencer.Immediate))
            .ThrowsExactly<ArgumentNullException>();

    /// <summary>Subscribing to a disposed immediate signal throws.</summary>
    [Test]
    public void ImmediateSignalSubscribeAfterDisposeThrows()
    {
        var taskSignal = Signal.FromTask(static _ => Task.FromResult(SuccessValue), Sequencer.Immediate);
        ((IDisposable)taskSignal).Dispose();
        _ = Assert.Throws<ObjectDisposedException>(() => taskSignal.Subscribe(static _ => { }));
    }
}
