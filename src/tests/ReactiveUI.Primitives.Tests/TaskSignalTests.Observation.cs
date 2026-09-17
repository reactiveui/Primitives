// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies task notification and cancellation ordering.</summary>
public partial class TaskSignalTests
{
    /// <summary>A canceled result or token produces one cancellation error.</summary>
    /// <param name="canceledResult">Whether cancellation is carried by the result.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ObservationReportsCancellation(bool canceledResult)
    {
        using CancellationTokenSource source = new();
        if (!canceledResult)
        {
            await source.CancelAsync();
        }

        var result = Task.FromResult((SuccessValue, canceledResult));
        Signal.TaskStopGate gate = new();
        TaskNotificationObserver observer = new();
        await Signal.ObserveTask(result, observer, gate, source.Token);
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completions).IsEqualTo(0);
        await Assert.That(observer.Error).IsTypeOf<OperationCanceledException>();
        await Assert.That(gate.TryStop()).IsFalse();
    }

    /// <summary>Pending factories deliver one terminal result through either subscription path.</summary>
    /// <param name="immediate">Whether notifications use the immediate sequencer.</param>
    /// <param name="outcome">Zero for success, one for failure, or two for cancellation.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(true, 0)]
    [Arguments(true, 1)]
    [Arguments(true, 2)]
    [Arguments(false, 0)]
    [Arguments(false, 1)]
    [Arguments(false, 2)]
    public async Task PendingFactoryDeliversTerminalResult(bool immediate, int outcome)
    {
        TaskCompletionSource<int> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var signal = Signal.FromTask(_ => pending.Task, immediate ? Sequencer.Immediate : Sequencer.CurrentThread);
        TaskNotificationObserver observer = new();
        using var signalLifetime = (IDisposable)signal;
        using var subscription = signal.Subscribe(observer);
        CompletePending(pending, outcome);
        await observer.Terminal.Task;
        await Assert.That(observer.Values.Count).IsEqualTo(outcome == 0 ? 1 : 0);
        await Assert.That(observer.Completions).IsEqualTo(outcome == 0 ? 1 : 0);
        if (outcome == 0)
        {
            await Assert.That(observer.Values[0]).IsEqualTo(SuccessValue);
            await Assert.That(observer.Error).IsNull();
        }
        else if (outcome == 1)
        {
            await Assert.That(observer.Error).IsTypeOf<InvalidOperationException>();
        }
        else
        {
            await Assert.That(observer.Error is OperationCanceledException).IsTrue();
        }
    }

    /// <summary>A disposal claim suppresses either a value or a fault after observation finishes.</summary>
    /// <param name="fault">Whether the task fails.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DisposedObservationSuppressesTerminalResult(bool fault)
    {
        TaskCompletionSource<(int Value, bool IsCanceled)> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Signal.TaskStopGate gate = new();
        TaskNotificationObserver observer = new();
        var observation = Signal.ObserveTask(pending.Task, observer, gate, CancellationToken.None);
        await Assert.That(gate.TryStop()).IsTrue();
        if (fault)
        {
            pending.SetException(new InvalidOperationException(BreakExecutionMessage));
        }
        else
        {
            pending.SetResult((SuccessValue, false));
        }

        await observation;
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Error).IsNull();
        await Assert.That(observer.Completions).IsEqualTo(0);
    }

    /// <summary>Disposal tolerates a released token source and suppresses the subsequent result.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task DisposalToleratesReleasedCancellationSource()
    {
        CancellationTokenSource source = new();
        Signal.TaskStopGate gate = new();
        TaskNotificationObserver observer = new();
        TaskCompletionSource<(int Value, bool IsCanceled)> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var token = source.Token;
        var subscription = Signal.CancelOnDispose(gate, source);
        source.Dispose();
        subscription.Dispose();
        pending.SetResult((SuccessValue, false));
        await Signal.ObserveTask(pending.Task, observer, gate, token);
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Error).IsNull();
        await Assert.That(observer.Completions).IsEqualTo(0);
    }

    /// <summary>Disposing a pending subscription cancels the token in either subscription path.</summary>
    /// <param name="immediate">Whether the immediate path is selected.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task SubscriptionDisposalCancelsPendingFactory(bool immediate)
    {
        TaskCompletionSource<int> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var signal = Signal.FromTask(_ => pending.Task, immediate ? Sequencer.Immediate : Sequencer.CurrentThread);
        using var lifetime = (IDisposable)signal;
        var subscription = signal.Subscribe(static _ => { });
        subscription.Dispose();
        await Assert.That(signal.IsCancellationRequested).IsTrue();
        pending.SetResult(SuccessValue);
    }

    /// <summary>Cancellation before a result reaches the observer produces one error.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ImmediateSignalCancellationPrecedesResult()
    {
        TaskCompletionSource<int> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var signal = Signal.FromTask(_ => pending.Task, Sequencer.Immediate);
        using var lifetime = (IDisposable)signal;
        TaskNotificationObserver observer = new();
        using var subscription = signal.Subscribe(observer);
        await Assert.That(ReferenceEquals(signal.Source, signal)).IsTrue();
        await signal.CancellationTokenSource!.CancelAsync();
        pending.SetResult(SuccessValue);
        await observer.Terminal.Task;
        await Assert.That(observer.Error).IsTypeOf<OperationCanceledException>();
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completions).IsEqualTo(0);
    }

    /// <summary>Repeated disposal raises the cancellation callback once and preserves the disposed state.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task ImmediateSignalDisposalRaisesCancellationOnce()
    {
        var signal = Signal.FromTask(static _ => Task.FromResult(SuccessValue), Sequencer.Immediate);
        var cancellations = 0;
        signal.GetOperationCanceled(Witness.Create<Exception>(_ => cancellations++));
        await Assert.That(signal.IsDisposed).IsFalse();
        await Assert.That(signal.IsCancellationRequested).IsFalse();
        ((IDisposable)signal).Dispose();
        ((IDisposable)signal).Dispose();
        await Assert.That(signal.IsDisposed).IsTrue();
        await Assert.That(signal.IsCancellationRequested).IsTrue();
        await Assert.That(cancellations).IsEqualTo(1);
    }

    /// <summary>Completes a pending factory with the selected outcome.</summary>
    /// <param name="pending">The controlled task source.</param>
    /// <param name="outcome">The terminal outcome.</param>
    private static void CompletePending(TaskCompletionSource<int> pending, int outcome)
    {
        if (outcome == 0)
        {
            pending.SetResult(SuccessValue);
        }
        else if (outcome == 1)
        {
            pending.SetException(new InvalidOperationException(BreakExecutionMessage));
        }
        else
        {
            pending.SetCanceled();
        }
    }
}
