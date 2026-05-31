// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for the zero-allocation <see cref="DisposableAsyncSlot"/> swap / single-assignment /
/// dispose helpers that operate against a caller-owned <see cref="IAsyncDisposable"/> field.</summary>
public class DisposableAsyncSlotTests
{
    /// <summary>Verifies a swap stores the new value and asynchronously disposes the previous occupant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwapReplacesOccupant_ThenPreviousDisposed()
    {
        IAsyncDisposable? slot = null;
        var first = new RecordingAsyncDisposable();
        var second = new RecordingAsyncDisposable();

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

        var late = new RecordingAsyncDisposable();
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
        var disposable = new RecordingAsyncDisposable();

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

        var late = new RecordingAsyncDisposable();
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

    /// <summary>Verifies the disposed sentinel's no-op <see cref="IAsyncDisposable.DisposeAsync"/> completes silently.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposedSentinelDisposed_ThenCompletesSilently() =>
        await ((IAsyncDisposable)DisposableAsyncSlot.DisposedSentinel.Instance).DisposeAsync();

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
