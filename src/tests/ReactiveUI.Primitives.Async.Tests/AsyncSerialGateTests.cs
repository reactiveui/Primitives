// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests gate acquisition, reentry, contention and idempotent disposal.</summary>
public class AsyncSerialGateTests
{
    /// <summary>Verifies that an uncontended gate can be acquired again after release.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUncontendedLock_ThenAcquiresAndReleases()
    {
        using AsyncSerialGate gate = new();

        using (await gate.EnterAsync())
        {
            await Assert.That(gate).IsNotNull();
        }

        using (await gate.EnterAsync())
        {
            await Assert.That(gate).IsNotNull();
        }
    }

    /// <summary>Verifies that nested acquisitions on the owning thread are granted without blocking.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSameThreadReentry_ThenAllowedWithoutBlocking()
    {
        using AsyncSerialGate gate = new();

        var lease1 = await gate.EnterAsync();
        var lease2 = await gate.EnterAsync();
        var lease3 = await gate.EnterAsync();
        try
        {
            await Assert.That(gate).IsNotNull();
        }
        finally
        {
            lease3.Dispose();
            lease2.Dispose();
            lease1.Dispose();
        }

        using (await gate.EnterAsync())
        {
            await Assert.That(gate).IsNotNull();
        }
    }

    /// <summary>Verifies a waiting caller acquires the gate after its owner exits.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenContendedWaiter_ThenResumesAfterRelease()
    {
        using AsyncSerialGate gate = new();
        var first = await gate.EnterAsync(CancellationToken.None);
        var second = gate.WaitForEntryAsync(CancellationToken.None);
        await Assert.That(second.IsCompleted).IsFalse();
        await Assert.That(gate.WaitersCount).IsEqualTo(1);
        first.Dispose();
        using var lease = await second;
        await Assert.That(gate.WaitersCount).IsEqualTo(0);
    }

    /// <summary>Verifies that a second dispose of the gate has no effect.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposeCalledTwice_ThenIdempotent()
    {
        AsyncSerialGate gate = new();

        gate.Dispose();
        gate.Dispose();

        await Assert.That(gate).IsNotNull();
    }

    /// <summary>Verifies <see cref="AsyncSerialGate.AdvanceRetainAttempt"/> updates the observed count when the compare-exchange loses a race.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAdvanceRetainAttemptSeesStaleCount_ThenUpdatesCurrentAndReturnsFalse()
    {
        const int ActualRefs = 2;
        const int ExpectedAfterRetain = 3;
        var refs = ActualRefs;
        var stale = 1;

        var lost = AsyncSerialGate.AdvanceRetainAttempt(ref refs, ref stale);
        await Assert.That(lost).IsFalse();
        await Assert.That(stale).IsEqualTo(ActualRefs);

        var won = AsyncSerialGate.AdvanceRetainAttempt(ref refs, ref stale);
        await Assert.That(won).IsTrue();
        await Assert.That(refs).IsEqualTo(ExpectedAfterRetain);
    }

    /// <summary>Verifies that a waiter arriving after the gate was disposed, while another thread still holds it, gets <see cref="ObjectDisposedException"/>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitingAfterDisposeWhileHeld_ThenThrowsObjectDisposed()
    {
        const int OtherThreadId = -1;
        AsyncSerialGate gate = new();
        using var held = await gate.EnterForThreadAsync(OtherThreadId, CancellationToken.None);
        gate.Dispose();

        await Assert.That(async () => await gate.EnterAsync(CancellationToken.None)).ThrowsExactly<ObjectDisposedException>();
    }
}
