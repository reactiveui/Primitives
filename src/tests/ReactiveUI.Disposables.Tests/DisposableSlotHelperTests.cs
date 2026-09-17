// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Disposables.Tests;

/// <summary>Tests slot assignment, replacement disposal, and idempotent disposal.</summary>
public class DisposableSlotHelperTests
{
    /// <summary>Verifies that an incoming value is disposed immediately when the slot is disposed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAssignWithoutDisposingPreviousIntoDisposedSlot_ThenIncomingDisposed()
    {
        IDisposable? slot = null;
        var disposed = DisposableSlotHelper.DisposedSentinel;
        CountingDisposable late = new();

        DisposableSlotHelper.AssignWithoutDisposingPrevious(ref slot, ref disposed, late);

        await Assert.That(late.DisposeCount).IsEqualTo(1);
        await Assert.That(slot).IsNull();
    }

    /// <summary>Verifies the steady-state assign - slot transitions to the new value without disposing the previous.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAssignWithoutDisposingPreviousOpen_ThenStoresAndLeavesPreviousAlone()
    {
        CountingDisposable first = new();
        IDisposable? slot = first;
        var disposed = 0;
        CountingDisposable second = new();

        DisposableSlotHelper.AssignWithoutDisposingPrevious(ref slot, ref disposed, second);

        await Assert.That(slot).IsSameReferenceAs(second);
        await Assert.That(first.DisposeCount).IsEqualTo(0);
    }

    /// <summary>Verifies that assigning a null value into an open slot stores null without throwing.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAssignWithoutDisposingPreviousNullValueOpen_ThenStoresNull()
    {
        CountingDisposable first = new();
        IDisposable? slot = first;
        var disposed = 0;

        DisposableSlotHelper.AssignWithoutDisposingPrevious(ref slot, ref disposed, null);

        await Assert.That(slot).IsNull();
        await Assert.That(first.DisposeCount).IsEqualTo(0);
    }

    /// <summary>Verifies the swap path disposes the previous value on each assignment.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwapAndDisposePreviousOpen_ThenPreviousDisposed()
    {
        CountingDisposable first = new();
        IDisposable? slot = first;
        var disposed = 0;
        CountingDisposable second = new();

        DisposableSlotHelper.SwapAndDisposePrevious(ref slot, ref disposed, second);

        await Assert.That(slot).IsSameReferenceAs(second);
        await Assert.That(first.DisposeCount).IsEqualTo(1);
        await Assert.That(second.DisposeCount).IsEqualTo(0);
    }

    /// <summary>Verifies the swap path disposes the incoming value when the slot is disposed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwapAndDisposePreviousIntoDisposedSlot_ThenIncomingDisposed()
    {
        IDisposable? slot = null;
        var disposed = DisposableSlotHelper.DisposedSentinel;
        CountingDisposable late = new();

        DisposableSlotHelper.SwapAndDisposePrevious(ref slot, ref disposed, late);

        await Assert.That(late.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies <c>TryDispose</c> latches and disposes the inner on the first call.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTryDisposeOpen_ThenLatchesAndDisposesInner()
    {
        CountingDisposable inner = new();
        IDisposable? slot = inner;
        var disposed = 0;

        var first = DisposableSlotHelper.TryDispose(ref slot, ref disposed);
        var second = DisposableSlotHelper.TryDispose(ref slot, ref disposed);

        await Assert.That(first).IsTrue();
        await Assert.That(second).IsFalse();
        await Assert.That(inner.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies assignment cleanup releases a value stored after holder disposal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenAssignmentOverlapsDisposal_ThenReleasesStoredValueOnce()
    {
        CountingDisposable incoming = new();
        IDisposable? slot = incoming;
        var disposed = DisposableSlotHelper.DisposedSentinel;

        DisposableSlotHelper.DisposeIfRaced(ref slot, ref disposed);
        DisposableSlotHelper.DisposeIfRaced(ref slot, ref disposed);

        await Assert.That(slot).IsNull();
        await Assert.That(incoming.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies assignment cleanup leaves a live holder's value installed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenAssignmentCompletesBeforeDisposal_ThenRetainsStoredValue()
    {
        CountingDisposable incoming = new();
        IDisposable? slot = incoming;
        var disposed = 0;

        DisposableSlotHelper.DisposeIfRaced(ref slot, ref disposed);

        await Assert.That(slot).IsSameReferenceAs(incoming);
        await Assert.That(incoming.DisposeCount).IsEqualTo(0);
    }

    /// <summary>Disposable that records its dispose count.</summary>
    private sealed class CountingDisposable : IDisposable
    {
        /// <summary>Gets the number of times <see cref="Dispose"/> has been invoked.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
    }
}
