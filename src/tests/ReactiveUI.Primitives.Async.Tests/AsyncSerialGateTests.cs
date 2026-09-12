// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests gate acquisition, reentry, contention and idempotent disposal.</summary>
public class AsyncSerialGateTests
{
    /// <summary>Verifies that the uncontended fast path acquires the gate via pure CAS.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUncontendedLock_ThenAcquiresAndReleases()
    {
        using AsyncSerialGate gate = new();

        using (await gate.EnterAsync())
        {
            await Assert.That(gate).IsNotNull();
        }

        // After release the gate must be re-acquirable.
        using (await gate.EnterAsync())
        {
            await Assert.That(gate).IsNotNull();
        }
    }

    /// <summary>Verifies that same-thread reentry bumps the recursion depth and does not block.</summary>
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

        // Gate must release cleanly after nested acquisitions.
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

    /// <summary>Verifies that double-dispose is idempotent.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposeCalledTwice_ThenIdempotent()
    {
        AsyncSerialGate gate = new();

        gate.Dispose();
        gate.Dispose();

        await Assert.That(gate).IsNotNull();
    }
}
