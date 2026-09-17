// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests delivery and release ordering for <see cref="Continuation"/>.</summary>
public class ContinuationTests
{
    /// <summary>An item offered while the current handoff holds the gate.</summary>
    private const int DroppedItem = 2;

    /// <summary>The emitted item carries the release handle, and its task remains pending until release.</summary>
    /// <param name="valueTask">True to acquire the handoff through the value-task overload.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Lock_FreeGate_EmitsAndCompletesOnUnlock(bool valueTask)
    {
        using Continuation continuation = new();
        List<int> values = [];
        IDisposable? handle = null;
        var observer = Observer.Create<(int Value, IDisposable Sync)>(value =>
        {
            values.Add(value.Value);
            handle = value.Sync;
        });

        var lockTask = valueTask
            ? continuation.LockValueTask(1, observer).AsTask()
            : continuation.Lock(1, observer);

        await Assert.That(lockTask.IsCompleted).IsFalse();
        await Assert.That(continuation.CompletedPhases).IsEqualTo(0);
        await Assert.That(handle).IsSameReferenceAs(continuation);

        var unlockTask = continuation.UnLock();

        await Assert.That(lockTask.IsCompletedSuccessfully).IsTrue();
        await Assert.That(unlockTask.IsCompletedSuccessfully).IsTrue();
        await lockTask;
        await unlockTask;

        await Assert.That(values).HasSingleItem();
        await Assert.That(values[0]).IsEqualTo(1);
        await Assert.That(continuation.CompletedPhases).IsEqualTo(1);
    }

    /// <summary>An item offered while the gate is held is dropped without releasing the pending handoff.</summary>
    /// <param name="valueTask">True to acquire handoffs through the value-task overload.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Lock_HeldGate_DropsItem(bool valueTask)
    {
        using Continuation continuation = new();
        List<int> values = [];
        var observer = Observer.Create<(int Value, IDisposable Sync)>(v => values.Add(v.Value));

        var first = valueTask
            ? continuation.LockValueTask(1, observer).AsTask()
            : continuation.Lock(1, observer);
        var second = valueTask
            ? continuation.LockValueTask(DroppedItem, observer).AsTask()
            : continuation.Lock(DroppedItem, observer);

        await Assert.That(first.IsCompleted).IsFalse();
        await Assert.That(second.IsCompletedSuccessfully).IsTrue();
        await second;
        await Assert.That(values).HasSingleItem();
        await Assert.That(values[0]).IsEqualTo(1);

        _ = continuation.UnLock();
        await Assert.That(first.IsCompletedSuccessfully).IsTrue();
        await first;
        await Assert.That(continuation.CompletedPhases).IsEqualTo(1);
    }

    /// <summary>A new lock after release starts a separate pending handoff.</summary>
    /// <param name="valueTask">True to acquire handoffs through the value-task overload.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Lock_AfterUnlock_StartsNextPhase(bool valueTask)
    {
        using Continuation continuation = new();
        var first = valueTask
            ? continuation.LockValueTask(1, null).AsTask()
            : continuation.Lock(1, null);
        _ = continuation.UnLock();
        await Assert.That(first.IsCompletedSuccessfully).IsTrue();
        await first;

        var second = valueTask
            ? continuation.LockValueTask(DroppedItem, null).AsTask()
            : continuation.Lock(DroppedItem, null);

        await Assert.That(second.IsCompleted).IsFalse();
        await Assert.That(continuation.CompletedPhases).IsEqualTo(1);

        _ = continuation.UnLock();
        await Assert.That(second.IsCompletedSuccessfully).IsTrue();
        await second;

        await Assert.That(continuation.CompletedPhases).IsEqualTo(DroppedItem);
    }

    /// <summary>A null observer still acquires a handoff that disposal releases once.</summary>
    /// <param name="valueTask">True to acquire the handoff through the value-task overload.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Dispose_PendingHandoffWithNullObserver_CompletesOnce(bool valueTask)
    {
        Continuation continuation = new();
        var handoff = valueTask
            ? continuation.LockValueTask(1, null).AsTask()
            : continuation.Lock(1, null);

        await Assert.That(handoff.IsCompleted).IsFalse();

        continuation.Dispose();
        continuation.Dispose();

        await Assert.That(handoff.IsCompletedSuccessfully).IsTrue();
        await handoff;
        await Assert.That(continuation.UnLock().IsCompletedSuccessfully).IsTrue();
        await Assert.That(continuation.CompletedPhases).IsEqualTo(1);
    }

    /// <summary>Disposal inside the observer releases the handoff without completing it before delivery returns.</summary>
    /// <param name="valueTask">True to acquire the handoff through the value-task overload.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Dispose_DuringDelivery_CompletesAfterDelivery(bool valueTask)
    {
        using Continuation continuation = new();
        long phasesDuringDelivery = -1;
        var observer = Observer.Create<(int Value, IDisposable Sync)>(value =>
        {
            value.Sync.Dispose();
            phasesDuringDelivery = continuation.CompletedPhases;
        });

        var handoff = valueTask
            ? continuation.LockValueTask(1, observer).AsTask()
            : continuation.Lock(1, observer);

        await Assert.That(phasesDuringDelivery).IsEqualTo(0);
        await Assert.That(handoff.IsCompletedSuccessfully).IsTrue();
        await handoff;
        await Assert.That(continuation.CompletedPhases).IsEqualTo(1);
    }

    /// <summary>An unlock inside delivery waits for that delivery to return.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task UnLock_DuringDelivery_CompletesAfterDelivery()
    {
        using Continuation continuation = new();
        Task? release = null;
        var completedDuringDelivery = true;
        var observer = Observer.Create<(int Value, IDisposable Sync)>(_ =>
        {
            release = continuation.UnLock();
            completedDuringDelivery = release.IsCompleted;
        });

        var handoff = continuation.Lock(1, observer);

        await Assert.That(completedDuringDelivery).IsFalse();
        await Assert.That(release!.IsCompletedSuccessfully).IsTrue();
        await Assert.That(handoff.IsCompletedSuccessfully).IsTrue();
        await handoff;
        await release;
        await Assert.That(continuation.CompletedPhases).IsEqualTo(1);
    }

    /// <summary>Finishing a newer handoff cannot complete an earlier delivery that has not returned.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CompleteDelivery_ReleasedPhasePrecedesNewLock_PreservesPairing()
    {
        using Continuation continuation = new();
        var first = continuation.TryBeginPhase()!;
        var firstRelease = continuation.UnLock();
        var second = continuation.TryBeginPhase()!;
        continuation.CompleteDelivery(second);

        await Assert.That(firstRelease.IsCompleted).IsFalse();
        await Assert.That(second.Completion.Task.IsCompleted).IsFalse();
        await Assert.That(continuation.CompletedPhases).IsEqualTo(0);

        var secondRelease = continuation.UnLock();

        await Assert.That(secondRelease.IsCompletedSuccessfully).IsTrue();
        await Assert.That(firstRelease.IsCompleted).IsFalse();
        await Assert.That(continuation.CompletedPhases).IsEqualTo(1);

        continuation.CompleteDelivery(first);
        continuation.CompleteDelivery(first);

        await Assert.That(firstRelease.IsCompletedSuccessfully).IsTrue();
        await firstRelease;
        await secondRelease;
        await Assert.That(continuation.CompletedPhases).IsEqualTo(DroppedItem);
    }

    /// <summary>Releasing and disposing a free gate never advances the completed phase count.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task UnLockAndDispose_FreeGate_RemainNoOps()
    {
        Continuation continuation = new();

        await Assert.That(continuation.UnLock().IsCompletedSuccessfully).IsTrue();
        await Assert.That(continuation.UnLock().IsCompletedSuccessfully).IsTrue();
        continuation.Dispose();
        continuation.Dispose();
        await Assert.That(continuation.UnLock().IsCompletedSuccessfully).IsTrue();

        await Assert.That(continuation.CompletedPhases).IsEqualTo(0);
    }

    /// <summary>A handoff started after disposal emits its item and reports disposal through its task.</summary>
    /// <param name="valueTask">True to acquire the handoff through the value-task overload.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Lock_AfterDispose_EmitsAndFaultsTask(bool valueTask)
    {
        Continuation continuation = new();
        List<int> values = [];
        var observer = Observer.Create<(int Value, IDisposable Sync)>(value => values.Add(value.Value));
        continuation.Dispose();

        var handoff = valueTask
            ? continuation.LockValueTask(1, observer).AsTask()
            : continuation.Lock(1, observer);
        var dropped = valueTask
            ? continuation.LockValueTask(DroppedItem, observer).AsTask()
            : continuation.Lock(DroppedItem, observer);

        await Assert.That(handoff.IsFaulted).IsTrue();
        await Assert.That(async () => await handoff).Throws<ObjectDisposedException>();
        await Assert.That(dropped.IsCompletedSuccessfully).IsTrue();
        await dropped;
        await Assert.That(values).HasSingleItem();
        await Assert.That(values[0]).IsEqualTo(1);
        await Assert.That(continuation.CompletedPhases).IsEqualTo(0);

        var release = continuation.UnLock();
        await Assert.That(release.IsFaulted).IsTrue();
        await Assert.That(async () => await release).Throws<ObjectDisposedException>();
    }

    /// <summary>Unmanaged disposal leaves the pending handoff available for an explicit release.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_UnmanagedOnly_DoesNotReleaseHandoff()
    {
        DerivedContinuation continuation = new();
        var handoff = continuation.Lock(1, null);

        continuation.DisposeUnmanaged();
        continuation.Dispose();

        await Assert.That(handoff.IsCompleted).IsFalse();
        _ = continuation.UnLock();
        await Assert.That(handoff.IsCompletedSuccessfully).IsTrue();
        await handoff;
        await Assert.That(continuation.CompletedPhases).IsEqualTo(1);
    }

    /// <summary>Exposes unmanaged disposal without changing the public disposal path.</summary>
    private sealed class DerivedContinuation : Continuation
    {
        /// <summary>Requests unmanaged disposal through the protected overload.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void DisposeUnmanaged() => Dispose(false);
    }
}
