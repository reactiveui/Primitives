// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for the <see cref="DisposableAsyncSlot"/> swap, single-assignment, and dispose helpers.</summary>
public class DisposableAsyncSlotTests
{
    /// <summary>Verifies a swap stores the new value and asynchronously disposes the previous occupant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwapReplacesOccupant_ThenPreviousDisposed()
    {
        IAsyncDisposable? slot = null;
        RecordingAsyncDisposable first = new();
        RecordingAsyncDisposable second = new();

        await DisposableAsyncSlot.SwapAsync(ref slot, first);
        await DisposableAsyncSlot.SwapAsync(ref slot, second);

        await Assert.That(first.DisposeCount).IsEqualTo(1);
        await Assert.That(second.DisposeCount).IsEqualTo(0);
        await Assert.That(DisposableAsyncSlot.IsDisposed(slot)).IsFalse();
    }

    /// <summary>Verifies swapping into an already-disposed slot disposes the incoming value immediately.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwapAfterDispose_ThenIncomingValueDisposed()
    {
        IAsyncDisposable? slot = null;
        await DisposableAsyncSlot.DisposeAsync(ref slot);

        RecordingAsyncDisposable late = new();
        await DisposableAsyncSlot.SwapAsync(ref slot, late);

        await Assert.That(late.DisposeCount).IsEqualTo(1);
        await Assert.That(DisposableAsyncSlot.IsDisposed(slot)).IsTrue();
    }

    /// <summary>Verifies a single assignment stores the value and disposing the slot disposes it once,
    /// with a second dispose being a no-op.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAssignedThenDisposedTwice_ThenDisposedExactlyOnce()
    {
        IAsyncDisposable? slot = null;
        RecordingAsyncDisposable disposable = new();

        await DisposableAsyncSlot.AssignAsync(ref slot, disposable);
        await DisposableAsyncSlot.DisposeAsync(ref slot);
        await DisposableAsyncSlot.DisposeAsync(ref slot);

        await Assert.That(disposable.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies assigning into an already-disposed slot disposes the incoming value immediately.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAssignAfterDispose_ThenIncomingValueDisposed()
    {
        IAsyncDisposable? slot = null;
        await DisposableAsyncSlot.DisposeAsync(ref slot);

        RecordingAsyncDisposable late = new();
        await DisposableAsyncSlot.AssignAsync(ref slot, late);

        await Assert.That(late.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies assigning a <see langword="null"/> value into an already-disposed slot is a silent
    /// no-op (exercises the null-value arm of the immediate-dispose path).</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAssignNullAfterDispose_ThenNoOp()
    {
        IAsyncDisposable? slot = null;
        await DisposableAsyncSlot.DisposeAsync(ref slot);

        await DisposableAsyncSlot.AssignAsync(ref slot, null);

        await Assert.That(DisposableAsyncSlot.IsDisposed(slot)).IsTrue();
    }

    /// <summary>Verifies a second assignment onto an occupied slot throws.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAssignedTwice_ThenThrowsInvalidOperation()
    {
        IAsyncDisposable? slot = null;
        await DisposableAsyncSlot.AssignAsync(ref slot, new RecordingAsyncDisposable());

        var threw = false;
        try
        {
            await DisposableAsyncSlot.AssignAsync(ref slot, new RecordingAsyncDisposable());
        }
        catch (InvalidOperationException)
        {
            threw = true;
        }

        await Assert.That(threw).IsTrue();
    }

    /// <summary>Verifies the shared disposed sentinel's no-op <see cref="IAsyncDisposable.DisposeAsync"/> completes silently.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposedSlotMarkerDisposed_ThenCompletesSilently() =>
        await Assert.That(static async () => await DisposableAsyncSlot.DisposedSentinel.DisposeAsync()).ThrowsNothing();

    /// <summary>Verifies a stale observation retries against the current slot state.</summary>
    /// <param name="closed">Whether the slot closed after the observation.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WhenSwapObservationIsStale_ThenUsesCurrentSlot(bool closed)
    {
        RecordingAsyncDisposable previous = new();
        RecordingAsyncDisposable incoming = new();
        var slot = closed ? DisposableAsyncSlot.DisposedSentinel : previous;
        await DisposableAsyncSlot.SwapObservedAsync(ref slot, incoming, null);
        await Assert.That(incoming.DisposeCount).IsEqualTo(closed ? 1 : 0);
        await Assert.That(previous.DisposeCount).IsEqualTo(closed ? 0 : 1);
        await Assert.That(slot).IsSameReferenceAs(closed ? DisposableAsyncSlot.DisposedSentinel : incoming);
        await DisposableAsyncSlot.DisposeAsync(ref slot);
    }

    /// <summary>Verifies <see cref="DisposableAsyncSlot.DisposeFromNotificationAsync"/> handles empty, closed, plain, and witness occupants.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WhenDisposedFromNotification_ThenPropagatesWitnessContextAndDisposesPlainOccupant()
    {
        IAsyncDisposable? emptySlot = null;
        await DisposableAsyncSlot.DisposeFromNotificationAsync(ref emptySlot);
        await Assert.That(DisposableAsyncSlot.IsDisposed(emptySlot)).IsTrue();
        await DisposableAsyncSlot.DisposeFromNotificationAsync(ref emptySlot);

        RecordingAsyncDisposable plain = new();
        IAsyncDisposable? plainSlot = plain;
        await DisposableAsyncSlot.DisposeFromNotificationAsync(ref plainSlot);
        await Assert.That(plain.DisposeCount).IsEqualTo(1);
        await Assert.That(DisposableAsyncSlot.IsDisposed(plainSlot)).IsTrue();

        NotificationSlotWitness witness = new();
        IAsyncDisposable? witnessSlot = witness;
        await DisposableAsyncSlot.DisposeCoreAsync(ref witnessSlot, fromNotification: true);
        await Assert.That(witness.DisposedFromNotification).IsTrue();
        await Assert.That(witness.HasDisposed).IsTrue();

        RecordingAsyncDisposable external = new();
        IAsyncDisposable? externalSlot = external;
        await DisposableAsyncSlot.DisposeCoreAsync(ref externalSlot, fromNotification: false);
        await Assert.That(external.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Minimal witness that records whether disposal arrived through notification context.</summary>
    private sealed class NotificationSlotWitness : IWitnessState
    {
        /// <summary>Embedded witness state.</summary>
        private WitnessAsyncState _witness;

        /// <summary>Gets a value indicating whether notification-context disposal was flagged before teardown.</summary>
        public bool DisposedFromNotification { get; private set; }

        /// <inheritdoc/>
        ref WitnessAsyncState IWitnessState.Witness => ref _witness;

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            DisposedFromNotification = _witness.IsDisposingFromNotification;
            return WitnessAsync.DisposeStateAsync(this);
        }
    }

    /// <summary>Recording async disposable that counts disposals.</summary>
    private sealed class RecordingAsyncDisposable : IAsyncDisposable
    {
        /// <summary>Gets the number of times this instance has been disposed.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public ValueTask DisposeAsync()
        {
            DisposeCount++;
            return default;
        }
    }
}
