// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Internal;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="TimerSlotHelper"/>.</summary>
public sealed class TimerSlotHelperTests
{
    /// <summary>Verifies a live owner keeps the timer it just scheduled.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ReleaseIfDisposed_NotDisposed_KeepsTimer()
    {
        CountingDisposable scheduled = new();
        IDisposable? timer = scheduled;
        var disposed = 0;

        TimerSlotHelper.ReleaseIfDisposed(ref timer, ref disposed);

        await Assert.That(timer).IsSameReferenceAs(scheduled);
        await Assert.That(scheduled.DisposeCount).IsEqualTo(0);
    }

    /// <summary>Verifies disposal that raced the schedule releases the stored timer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ReleaseIfDisposed_DisposedWithStoredTimer_DisposesAndClearsTimer()
    {
        CountingDisposable scheduled = new();
        IDisposable? timer = scheduled;
        var disposed = 1;

        TimerSlotHelper.ReleaseIfDisposed(ref timer, ref disposed);

        await Assert.That(timer).IsNull();
        await Assert.That(scheduled.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies disposal that already cleared the slot leaves nothing to release.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ReleaseIfDisposed_DisposedWithEmptySlot_DoesNothing()
    {
        IDisposable? timer = null;
        var disposed = 1;

        TimerSlotHelper.ReleaseIfDisposed(ref timer, ref disposed);

        await Assert.That(timer).IsNull();
    }

    /// <summary>Counts how many times it is disposed.</summary>
    private sealed class CountingDisposable : IDisposable
    {
        /// <summary>Gets the number of dispose calls.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
    }
}
