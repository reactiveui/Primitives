// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.IO;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Extensions.Internal;
using ReactiveUI.Primitives.Extensions.Operators;
using ReactiveUI.Primitives.Extensions.Tests;

namespace ReactiveUI.Primitives.Extensions.Tests.Internal;

/// <summary>Direct RxVoid tests for <see cref="DisposableSlotHelper"/>. Covers every reachable
/// branch — the already-disposed pre-check, the steady-state assign, the swap-disposes-previous
/// path, and the idempotent <c>TryDispose</c> latch. The single race-recheck step that fires
/// only under a real concurrent dispose is isolated in <c>DisposeIfRaced</c> and excluded from
/// coverage there.</summary>
public class DisposableSlotHelperTests
{
    /// <summary>Verifies that an incoming value is disposed immediately if the slot is already disposed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAssignWithoutDisposingPreviousIntoDisposedSlot_ThenIncomingDisposed()
    {
        IDisposable? slot = null;
        var disposed = DisposableSlotHelper.DisposedSentinel;
        var late = new CountingDisposable();

        DisposableSlotHelper.AssignWithoutDisposingPrevious(ref slot, ref disposed, late);

        await Assert.That(late.DisposeCount).IsEqualTo(1);
        await Assert.That(slot).IsNull();
    }

    /// <summary>Verifies the steady-state assign — slot transitions to the new value without disposing the previous.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAssignWithoutDisposingPreviousOpen_ThenStoresAndLeavesPreviousAlone()
    {
        var first = new CountingDisposable();
        IDisposable? slot = first;
        var disposed = 0;
        var second = new CountingDisposable();

        DisposableSlotHelper.AssignWithoutDisposingPrevious(ref slot, ref disposed, second);

        await Assert.That(slot).IsSameReferenceAs(second);
        await Assert.That(first.DisposeCount).IsEqualTo(0);
    }

    /// <summary>Verifies that assigning a null value into an open slot stores null without throwing.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAssignWithoutDisposingPreviousNullValueOpen_ThenStoresNull()
    {
        var first = new CountingDisposable();
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
        var first = new CountingDisposable();
        IDisposable? slot = first;
        var disposed = 0;
        var second = new CountingDisposable();

        DisposableSlotHelper.SwapAndDisposePrevious(ref slot, ref disposed, second);

        await Assert.That(slot).IsSameReferenceAs(second);
        await Assert.That(first.DisposeCount).IsEqualTo(1);
        await Assert.That(second.DisposeCount).IsEqualTo(0);
    }

    /// <summary>Verifies the swap path disposes the incoming value if the slot is already disposed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwapAndDisposePreviousIntoDisposedSlot_ThenIncomingDisposed()
    {
        IDisposable? slot = null;
        var disposed = DisposableSlotHelper.DisposedSentinel;
        var late = new CountingDisposable();

        DisposableSlotHelper.SwapAndDisposePrevious(ref slot, ref disposed, late);

        await Assert.That(late.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies <c>TryDispose</c> latches and disposes the inner on the first call.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTryDisposeOpen_ThenLatchesAndDisposesInner()
    {
        var inner = new CountingDisposable();
        IDisposable? slot = inner;
        var disposed = 0;

        var first = DisposableSlotHelper.TryDispose(ref slot, ref disposed);
        var second = DisposableSlotHelper.TryDispose(ref slot, ref disposed);

        await Assert.That(first).IsTrue();
        await Assert.That(second).IsFalse();
        await Assert.That(inner.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Disposable used to verify dispose counts.</summary>
    private sealed class CountingDisposable : IDisposable
    {
        /// <summary>Gets the number of times <see cref="Dispose"/> has been invoked.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
    }
}
