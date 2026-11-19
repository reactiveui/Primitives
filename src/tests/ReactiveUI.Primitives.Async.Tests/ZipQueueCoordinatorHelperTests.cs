// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Internal;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for <see cref="ZipQueueCoordinatorHelper"/> and <see cref="ZipDequeueOutcome{TCounterpart}"/>.</summary>
public sealed class ZipQueueCoordinatorHelperTests
{
    /// <summary>First paired value.</summary>
    private const int Ten = 10;

    /// <summary>Second paired value.</summary>
    private const int Twenty = 20;

    /// <summary>Ignored post-termination value.</summary>
    private const int LateValue = 99;

    /// <summary>
    /// Verifies <see cref="ZipQueueCoordinatorHelper.DequeueOrEnqueue{TSelf, TCounterpart}"/> buffers unmatched items,
    /// pairs waiting items, and marks terminal drain when the counterpart completed.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DequeueOrEnqueue_BuffersPairsAndCompletesWhenCounterpartDrains()
    {
        Lock gate = new();
        var done = false;
        var open = false;
        var completed = true;
        Queue<int> left = new();
        Queue<string> right = new();

        var buffered = ZipQueueCoordinatorHelper.DequeueOrEnqueue(gate, ref done, left, Ten, right, in open);
        await Assert.That(buffered.HasPair).IsFalse();
        await Assert.That(left.Count).IsEqualTo(1);

        var pairedOpen = ZipQueueCoordinatorHelper.DequeueOrEnqueue(gate, ref done, right, "a", left, in open);
        await Assert.That(pairedOpen).IsEqualTo(new(true, Ten, false));
        await Assert.That(done).IsFalse();

        left.Enqueue(Twenty);
        right.Enqueue("stale");
        var pairedTerminal = ZipQueueCoordinatorHelper.DequeueOrEnqueue(gate, ref done, right, "b", left, in completed);
        await Assert.That(pairedTerminal).IsEqualTo(new(true, Twenty, true));
        await Assert.That(done).IsTrue();
        await Assert.That(right.Count).IsEqualTo(0);

        var afterDone = ZipQueueCoordinatorHelper.DequeueOrEnqueue(gate, ref done, left, LateValue, right, in open);
        await Assert.That(afterDone.HasPair).IsFalse();
    }

    /// <summary>
    /// Verifies <see cref="ZipQueueCoordinatorHelper.DequeueOrEnqueue{TSelf, TCounterpart}"/> completes the zip when a
    /// pair drains the last value of a counterpart that completed while that value was still queued.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DequeueOrEnqueue_CompletesWhenPairDrainsCompletedCounterpart()
    {
        Lock gate = new();
        var done = false;
        var leftDone = false;
        var rightDone = false;
        Queue<int> left = new();
        Queue<int> right = new([Ten]);

        // The right side completes while its last value is still queued, so the zip stays open.
        var rightCompleted = ZipQueueCoordinatorHelper.MarkSideCompleted(
            gate,
            ref done,
            ref rightDone,
            in leftDone,
            right,
            left,
            Result.Success);
        await Assert.That(rightCompleted).IsFalse();

        var paired = ZipQueueCoordinatorHelper.DequeueOrEnqueue(gate, ref done, left, Twenty, right, in rightDone);
        await Assert.That(paired).IsEqualTo(new(true, Ten, true));
        await Assert.That(done).IsTrue();
    }

    /// <summary>Verifies <see cref="ZipQueueCoordinatorHelper.MarkSideCompleted{TSelf, TCounterpart}"/> waits while buffered items remain unless the result faulted or both sides completed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task MarkSideCompleted_DistinguishesBufferedActiveFromTerminalCompletion()
    {
        Lock gate = new();
        var done = false;
        var leftDone = false;
        var rightDone = false;
        Queue<int> left = new([1]);
        Queue<int> right = new();

        var leftCompletedEarly = ZipQueueCoordinatorHelper.MarkSideCompleted(
            gate,
            ref done,
            ref leftDone,
            in rightDone,
            left,
            right,
            Result.Success);
        await Assert.That(leftCompletedEarly).IsFalse();
        await Assert.That(leftDone).IsTrue();
        await Assert.That(done).IsFalse();

        var rightCompleted = ZipQueueCoordinatorHelper.MarkSideCompleted(
            gate,
            ref done,
            ref rightDone,
            in leftDone,
            right,
            left,
            Result.Success);
        await Assert.That(rightCompleted).IsTrue();
        await Assert.That(done).IsTrue();
        await Assert.That(left.Count).IsEqualTo(0);

        var duplicateCall = ZipQueueCoordinatorHelper.MarkSideCompleted(
            gate,
            ref done,
            ref rightDone,
            in leftDone,
            right,
            left,
            Result.Failure(new InvalidOperationException("ignored")));
        await Assert.That(duplicateCall).IsFalse();
    }
}
