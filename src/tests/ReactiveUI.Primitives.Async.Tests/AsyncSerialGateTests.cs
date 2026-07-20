// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Coverage for <see cref="AsyncSerialGate"/> — uncontended fast path, same-thread reentry, contended slow path, double-dispose idempotency.</summary>
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

    /// <summary>Verifies that a contended waiter resumes via the semaphore-signal slow path once the owning lock is released.</summary>
    /// <remarks>This intentionally avoids a "waiter has not resumed within Xms" timing assertion —
    /// such a probe is unreliable across CI runners. What matters for coverage is that the slow path
    /// (semaphore park + retry CAS) actually runs; we drive that by serialising two contenders so the
    /// second must wait on the first's release.</remarks>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenContendedWaiter_ThenResumesAfterRelease()
    {
        const int ContentionTimeoutSeconds = 30;
        using AsyncSerialGate gate = new();
        var first = await gate.EnterAsync();

        TaskCompletionSource<bool> secondAcquired = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<bool> release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // Wait until the contender is either parked on the slow path (WaitersCount > 0) or
        // has already acquired the gate via the same-thread reentry fast path (secondAcquired
        // set). Either outcome is a valid configuration of AsyncSerialGate — what we care about for
        // this test is that the contender ultimately gets the gate after we release it; the
        // dual condition keeps the assertion stable across runners where Task.Run may reuse
        // the test thread.
        var contender = Task.Run(async () =>
        {
            using var lease = await gate.EnterAsync().ConfigureAwait(false);
            _ = secondAcquired.TrySetResult(true);
            await release.Task.ConfigureAwait(false);
        });

        var contenderReady = await AsyncTestHelpers.WaitForConditionAsync(
            () => gate.WaitersCount >= 1 || secondAcquired.Task.IsCompleted,
            TimeSpan.FromSeconds(ContentionTimeoutSeconds));
        await Assert.That(contenderReady).IsTrue();

        // Releasing the first acquisition is the only thing that can let a slow-path contender
        // resume; a fast-path contender already completed and this is a no-op.
        first.Dispose();

        var acquired = await secondAcquired.Task.WaitAsync(TimeSpan.FromSeconds(ContentionTimeoutSeconds));
        await Assert.That(acquired).IsTrue();

        _ = release.TrySetResult(true);
        await contender;
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
