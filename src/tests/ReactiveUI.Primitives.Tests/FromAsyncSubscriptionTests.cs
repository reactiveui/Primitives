// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
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
            CancelThenReturn(externalCancellation),
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
                callbackFailure = CancelThroughThrowingCallback(externalCancellation, expected);
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

    /// <summary>A factory that throws forwards the failure once, with or without a linked external token.</summary>
    /// <param name="linked">Whether an external cancellation token is linked into the subscription.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Start_FactoryThrows_ForwardsFailure(bool linked)
    {
        using CancellationTokenSource external = new();
        RecordingWitness<int> observer = new();
        InvalidOperationException expected = new("factory failed");
        using FromAsyncSubscription<int> subscription = new(
            observer,
            _ => throw expected,
            linked ? external.Token : CancellationToken.None);

        using var handle = subscription.Start();

        await Assert.That(observer.Errors).Count().IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>Creates a factory that cancels the source synchronously, then returns a completed result.</summary>
    /// <param name="source">The source to cancel while the factory runs.</param>
    /// <returns>The factory.</returns>
    private static Func<CancellationToken, Task<int>> CancelThenReturn(CancellationTokenSource source) =>
        _ =>
        {
            CancelSynchronously(source);
            return Task.FromResult(FactoryValue);
        };

    /// <summary>Cancels the source on the calling thread, running its registrations before returning.</summary>
    /// <param name="source">The source to cancel.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void CancelSynchronously(CancellationTokenSource source) => source.Cancel();

    /// <summary>Cancels the source synchronously through a registration that throws, capturing the callback failure.</summary>
    /// <param name="source">The source to cancel.</param>
    /// <param name="failure">The exception the registration throws.</param>
    /// <returns>The failure raised by the callback, or <see langword="null"/> when none surfaced.</returns>
    private static InvalidOperationException? CancelThroughThrowingCallback(CancellationTokenSource source, InvalidOperationException failure)
    {
        using var registration = source.Token.UnsafeRegister(static state => throw (Exception)state!, failure);
        try
        {
            source.Cancel(true);
            return null;
        }
        catch (InvalidOperationException error)
        {
            return error;
        }
    }
}
