// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for the synchronous-completion path of <see cref="FromAsyncSubscription{T}"/>.</summary>
public sealed class FromAsyncSubscriptionTests
{
    /// <summary>The value produced by the task factory that completes synchronously.</summary>
    private const int FactoryValue = 11;

    /// <summary>Cancellation raised while the factory runs is the only notification, and the task result is dropped.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ExternalCancellationDuringTheFactoryForwardsCancellationAndDropsTheCompletedResult()
    {
        using CancellationTokenSource externalCancellation = new();
        RecordingWitness<int> witness = new();

        FromAsyncSubscription<int> subscription = new(
            witness,
            _ =>
            {
                externalCancellation.Cancel();
                return Task.FromResult(FactoryValue);
            },
            externalCancellation.Token);

        using var handle = subscription.Start();

        await Assert.That(witness.Values.Count).IsEqualTo(0);
        await Assert.That(witness.Completed).IsEqualTo(0);
        await Assert.That(witness.Errors.Count).IsEqualTo(1);
        await Assert.That(witness.Errors[0]).IsTypeOf<TaskCanceledException>();
    }

    /// <summary>A canceled factory task forwards external cancellation even if another callback interrupted notification.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task Start_InterruptedCancellationCallbacks_ForwardsCancellationOnce()
    {
        using CancellationTokenSource externalCancellation = new();
        RecordingWitness<int> witness = new();
        InvalidOperationException expected = new("cancellation callback failed");
        Exception? callbackFailure = null;
        using FromAsyncSubscription<int> subscription = new(
            witness,
            _ =>
            {
                using var registration = externalCancellation.Token.UnsafeRegister(static state => throw (Exception)state!, expected);
                try
                {
                    externalCancellation.Cancel(true);
                }
                catch (InvalidOperationException error)
                {
                    callbackFailure = error;
                }

                return Task.FromCanceled<int>(externalCancellation.Token);
            },
            externalCancellation.Token);

        using var handle = subscription.Start();

        await Assert.That(callbackFailure).IsSameReferenceAs(expected);
        await Assert.That(witness.Values).IsEmpty();
        await Assert.That(witness.Completed).IsEqualTo(0);
        await Assert.That(witness.Errors).Count().IsEqualTo(1);
        await Assert.That(witness.Errors[0]).IsTypeOf<TaskCanceledException>();
    }

    /// <summary>A completion helper cannot send a second terminal notification after external cancellation won.</summary>
    /// <param name="status">The terminal status of the factory task.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(TaskStatus.RanToCompletion)]
    [Arguments(TaskStatus.Canceled)]
    [Arguments(TaskStatus.Faulted)]
    public async Task CompleteSynchronously_ExternalCancellationAlreadyWon_DoesNotNotifyAgain(TaskStatus status)
    {
        using CancellationTokenSource externalSource = new();
        using AsyncSubscriptionLifetime lifetime = new();
        RecordingWitness<int> observer = new();
        using FromAsyncExternalCancellation<int> cancellation = new(observer, lifetime, externalSource.Token);
        using var linkedSource = cancellation.CreateLinkedSource(lifetime.Token);
        _ = cancellation.Start();
        await externalSource.CancelAsync();

        var completed = status switch
        {
            TaskStatus.RanToCompletion => FromAsyncSubscription<int>.CompleteSynchronously(
                FactoryValue,
                observer,
                lifetime,
                cancellation,
                linkedSource),
            TaskStatus.Canceled => FromAsyncSubscription<int>.CancelSynchronously(
                Task.FromCanceled<int>(externalSource.Token),
                observer,
                lifetime,
                cancellation,
                linkedSource),
            TaskStatus.Faulted => FromAsyncSubscription<int>.FaultSynchronously(
                Task.FromException<int>(new InvalidOperationException("task failed")),
                observer,
                lifetime,
                cancellation,
                linkedSource),
            _ => false
        };

        await Assert.That(completed).IsTrue();
        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(observer.Errors).Count().IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsTypeOf<TaskCanceledException>();
    }

    /// <summary>Synchronous fault forwarding preserves the original exception, including an empty aggregate.</summary>
    /// <param name="aggregate">Whether the original failure is an aggregate with no inner exceptions.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task FaultSynchronously_FaultedTask_PreservesOriginalException(bool aggregate)
    {
        RecordingWitness<int> observer = new();
        Exception expected = aggregate ? new AggregateException() : new InvalidOperationException("task failed");
        using FromAsyncSubscription<int> subscription = new(observer, _ => Task.FromException<int>(expected));

        using var handle = subscription.Start();

        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(observer.Errors).Count().IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(expected);
    }
}
