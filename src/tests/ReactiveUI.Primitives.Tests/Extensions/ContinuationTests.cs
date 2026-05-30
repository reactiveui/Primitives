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
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Extensions.Internal;
using ReactiveUI.Primitives.Extensions.Operators;
using ReactiveUI.Primitives.Extensions.Tests;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for <see cref="Continuation"/> — the phase-barrier lock used to serialise emissions,
/// covering both the <see cref="Continuation.Lock{T}"/> (Task) and <see cref="Continuation.LockValueTask{T}"/>
/// (ValueTask) entry points plus the already-locked short-circuit.</summary>
public class ContinuationTests
{
    /// <summary>Guard timeout to keep barrier rendezvous from hanging the test run.</summary>
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies <see cref="Continuation.LockValueTask{T}"/> pushes the item downstream, locks,
    /// and completes once the phase is signalled by an unlock.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLockValueTaskNotLocked_ThenEmitsAndCompletesOnUnlock()
    {
        using var continuation = new Continuation();
        var values = new List<int>();
        var observer = Observer.Create<(int Value, IDisposable Sync)>(v => values.Add(v.Value));

        var lockTask = continuation.LockValueTask(1, observer);
        var unlockTask = continuation.UnLock();

        await lockTask.AsTask().WaitAsync(Timeout);
        await unlockTask.WaitAsync(Timeout);

        await Assert.That(values.Count).IsEqualTo(1);
        await Assert.That(values[0]).IsEqualTo(1);
        await Assert.That(continuation.CompletedPhases).IsGreaterThanOrEqualTo(1);
    }

    /// <summary>Verifies a second <see cref="Continuation.LockValueTask{T}"/> while already locked returns a
    /// completed default value task and does not push the item downstream.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLockValueTaskAlreadyLocked_ThenReturnsDefaultAndDropsItem()
    {
        using var continuation = new Continuation();
        var values = new List<int>();
        var observer = Observer.Create<(int Value, IDisposable Sync)>(v => values.Add(v.Value));

        var first = continuation.LockValueTask(1, observer);
        var second = continuation.LockValueTask(2, observer);

        await Assert.That(second.IsCompleted).IsTrue();
        await second;

        var unlockTask = continuation.UnLock();
        await first.AsTask().WaitAsync(Timeout);
        await unlockTask.WaitAsync(Timeout);

        await Assert.That(values.Count).IsEqualTo(1);
        await Assert.That(values[0]).IsEqualTo(1);
    }

    /// <summary>Verifies <see cref="Continuation.Lock{T}"/> (the Task overload) emits and completes on unlock.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLockNotLocked_ThenEmitsAndCompletesOnUnlock()
    {
        using var continuation = new Continuation();
        var values = new List<int>();
        var observer = Observer.Create<(int Value, IDisposable Sync)>(v => values.Add(v.Value));

        var lockTask = continuation.Lock(1, observer);
        var unlockTask = continuation.UnLock();

        await lockTask.WaitAsync(Timeout);
        await unlockTask.WaitAsync(Timeout);

        await Assert.That(values.Count).IsEqualTo(1);
    }

    /// <summary>Verifies a second <see cref="Continuation.Lock{T}"/> while already locked returns a completed
    /// task and drops the item.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLockAlreadyLocked_ThenReturnsCompletedAndDropsItem()
    {
        using var continuation = new Continuation();
        var values = new List<int>();
        var observer = Observer.Create<(int Value, IDisposable Sync)>(v => values.Add(v.Value));

        var first = continuation.Lock(1, observer);
        var second = continuation.Lock(2, observer);

        await Assert.That(second.IsCompleted).IsTrue();

        var unlockTask = continuation.UnLock();
        await first.WaitAsync(Timeout);
        await unlockTask.WaitAsync(Timeout);

        await Assert.That(values.Count).IsEqualTo(1);
    }

    /// <summary>Verifies that unlocking a continuation that was never locked completes immediately.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenUnlockNotLocked_ThenCompletesImmediately()
    {
        using var continuation = new Continuation();

        await continuation.UnLock().WaitAsync(Timeout);

        await Assert.That(continuation.CompletedPhases).IsEqualTo(0);
    }

    /// <summary>Verifies disposing twice is an idempotent no-op on the second call.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposeTwice_ThenSecondDisposeIsNoOp()
    {
        var continuation = new Continuation();
        var disposedTwice = false;

        continuation.Dispose();
        continuation.Dispose();
        disposedTwice = true;

        await Assert.That(disposedTwice).IsTrue();
    }
}
