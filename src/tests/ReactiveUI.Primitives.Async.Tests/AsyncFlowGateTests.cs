// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internal;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for <see cref="AsyncFlowGate"/>.</summary>
public sealed class AsyncFlowGateTests
{
    /// <summary>How long a test waits for an asynchronous hand-off before failing.</summary>
    private static readonly TimeSpan HandOffTimeout = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Verifies two independent async flows started from the same thread do not hold the gate together,
    /// which is the case a thread-ID owner check gets wrong.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task RunAsync_IndependentFlowsOnSameThread_RunOneAtATime()
    {
        AsyncFlowGate gate = new();
        TaskCompletionSource releaseFirst = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var secondEntered = false;

        var first = gate.RunAsync(releaseFirst, static release => new(release.Task), CancellationToken.None).AsTask();
        var second = gate.RunAsync(
            0,
            _ =>
            {
                secondEntered = true;
                return default;
            },
            CancellationToken.None).AsTask();

        await Assert.That(secondEntered).IsFalse();

        releaseFirst.SetResult();
        await Assert.That(first).CompletesWithin(HandOffTimeout);
        await Assert.That(second).CompletesWithin(HandOffTimeout);
        await Assert.That(secondEntered).IsTrue();
    }

    /// <summary>Verifies work running under the gate can re-enter it without waiting for itself.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task RunAsync_WorkReentersGate_RunsWithoutWaiting()
    {
        AsyncFlowGate gate = new();
        var innerRan = false;

        var outer = gate.RunAsync(
            gate,
            async owner =>
            {
                await Task.Yield();
                await owner.RunAsync(
                    0,
                    _ =>
                    {
                        innerRan = true;
                        return default;
                    },
                    CancellationToken.None);
            },
            CancellationToken.None).AsTask();

        await Assert.That(outer).CompletesWithin(HandOffTimeout);
        await Assert.That(innerRan).IsTrue();
    }

    /// <summary>Verifies work started inside a hold that detaches its flow waits for the hold to end.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task RunAsync_DetachedFlowStartedInsideHold_WaitsForRelease()
    {
        AsyncFlowGate gate = new();
        var childEntered = false;
        var childEnteredDuringHold = true;
        Task? child = null;

        async Task RunDetachedAsync()
        {
            gate.DetachCurrentFlow();
            await gate.RunAsync(
                0,
                _ =>
                {
                    childEntered = true;
                    return default;
                },
                CancellationToken.None);
        }

        await gate.RunAsync(
            0,
            _ =>
            {
                child = RunDetachedAsync();
                childEnteredDuringHold = childEntered;
                return default;
            },
            CancellationToken.None);

        await Assert.That(childEnteredDuringHold).IsFalse();
        await Assert.That(child!).CompletesWithin(HandOffTimeout);
        await Assert.That(childEntered).IsTrue();
    }

    /// <summary>Verifies an <c>EnterAsync</c> lease from another flow keeps a second flow out until the lease is disposed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task EnterAsync_GateHeldByAnotherFlow_WaitsForLeaseDisposal()
    {
        AsyncFlowGate gate = new();
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var holder = AsyncFlowGateHolder.HoldAsync(gate, release.Task);
        var entered = false;

        var waiter = EnterAndMarkAsync(gate, () => entered = true);
        await Assert.That(entered).IsFalse();

        release.SetResult();
        await Assert.That(holder).CompletesWithin(HandOffTimeout);
        await Assert.That(waiter).CompletesWithin(HandOffTimeout);
        await Assert.That(entered).IsTrue();
    }

    /// <summary>Verifies the flow that holds an <c>EnterAsync</c> lease can enter again, after an await, without waiting.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task EnterAsync_HolderEntersAgainAfterAwait_DoesNotWait()
    {
        AsyncFlowGate gate = new();
        var innerEntered = false;

        async Task HoldAndReenterAsync()
        {
            using (await gate.EnterAsync().ConfigureAwait(false))
            {
                await Task.Yield();
                await EnterAndMarkAsync(gate, () => innerEntered = true).ConfigureAwait(false);
            }
        }

        await Assert.That(HoldAndReenterAsync()).CompletesWithin(HandOffTimeout);
        await Assert.That(innerEntered).IsTrue();
    }

    /// <summary>Verifies a cancelled wait throws without taking the gate, so the next flow still gets in.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task EnterAsync_CancelledWhileWaiting_ThrowsAndLeavesGateUsable()
    {
        AsyncFlowGate gate = new();
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var holder = AsyncFlowGateHolder.HoldAsync(gate, release.Task);
        using CancellationTokenSource cancel = new();

        var cancelledWait = EnterWithTokenAsync(gate, cancel.Token);
        await cancel.CancelAsync();
        await Assert.That(() => cancelledWait).Throws<OperationCanceledException>();

        release.SetResult();
        await Assert.That(holder).CompletesWithin(HandOffTimeout);
        var entered = false;
        await Assert.That(EnterAndMarkAsync(gate, () => entered = true)).CompletesWithin(HandOffTimeout);
        await Assert.That(entered).IsTrue();
    }

    /// <summary>Verifies the entry's context-flowing <c>OnCompleted</c> runs the continuation once the gate is free.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ConfiguredEntryOnCompleted_GateFree_RunsContinuation()
    {
        AsyncFlowGate gate = new();
        TaskCompletionSource continued = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var entry = gate.EnterAsync().ConfigureAwait(false);

        entry.OnCompleted(() => continued.SetResult());

        await Assert.That(continued.Task).CompletesWithin(HandOffTimeout);
        entry.GetResult().Dispose();
    }

    /// <summary>Enters <paramref name="gate"/>, runs <paramref name="mark"/> while holding it, and leaves.</summary>
    /// <param name="gate">The gate to enter.</param>
    /// <param name="mark">Records that the gate was entered.</param>
    /// <returns>A task that completes after leaving the gate.</returns>
    private static async Task EnterAndMarkAsync(AsyncFlowGate gate, Action mark)
    {
        using (await gate.EnterAsync().ConfigureAwait(false))
        {
            mark();
        }
    }

    /// <summary>Enters <paramref name="gate"/> with <paramref name="cancellationToken"/>, records the entry, and leaves.</summary>
    /// <param name="gate">The gate to enter.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>A task that completes after leaving the gate.</returns>
    private static async Task EnterWithTokenAsync(AsyncFlowGate gate, CancellationToken cancellationToken)
    {
        var entered = false;
        using (await gate.EnterAsync(cancellationToken).ConfigureAwait(false))
        {
            entered = true;
        }

        await Assert.That(entered).IsTrue();
    }
}
