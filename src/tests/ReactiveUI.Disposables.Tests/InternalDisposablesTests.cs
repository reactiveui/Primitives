// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Disposables.Tests;

/// <summary>Tests for the internal disposable holders (<see cref = "SwapDisposable"/>, <see cref = "MutableDisposable"/>, <see cref = "OnceDisposable"/>) that back the sync-side sinks.</summary>
public class InternalDisposablesTests
{
    /// <summary>Verifies that assigning a new inner disposes the previous one.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwapDisposableReplaced_ThenPreviousDisposed()
    {
        SwapDisposable holder = new();
        CountingDisposable first = new();
        CountingDisposable second = new();
        holder.Disposable = first;
        await Assert.That(holder.Disposable).IsSameReferenceAs(first);
        holder.Disposable = second;
        await Assert.That(first.DisposeCount).IsEqualTo(1);
        await Assert.That(holder.Disposable).IsSameReferenceAs(second);
        holder.Dispose();
        await Assert.That(second.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that once disposed, subsequent assignments dispose the supplied value immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwapDisposableAfterDispose_ThenAssignmentDisposesValue()
    {
        SwapDisposable holder = new();
        holder.Dispose();
        CountingDisposable late = new();
        holder.Disposable = late;
        await Assert.That(late.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that double-dispose is a no-op on <see cref = "SwapDisposable"/>.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwapDisposableDisposedTwice_ThenNoOp()
    {
        CountingDisposable inner = new();
        SwapDisposable holder = new() { Disposable = inner };
        holder.Dispose();
        holder.Dispose();
        await Assert.That(inner.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that <see cref = "MutableDisposable"/> replacement does NOT dispose the previous inner.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMutableDisposableReplaced_ThenPreviousNotDisposed()
    {
        MutableDisposable holder = new();
        CountingDisposable first = new();
        CountingDisposable second = new();
        holder.Disposable = first;
        holder.Disposable = second;
        await Assert.That(first.DisposeCount).IsEqualTo(0);
        holder.Dispose();
        await Assert.That(second.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that assigning after dispose immediately disposes the value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMutableDisposableAfterDispose_ThenAssignmentDisposesValue()
    {
        MutableDisposable holder = new();
        holder.Dispose();
        CountingDisposable late = new();
        holder.Disposable = late;
        await Assert.That(late.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that <see cref = "MutableDisposable"/> double-dispose is a no-op.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMutableDisposableDisposedTwice_ThenNoOp()
    {
        CountingDisposable inner = new();
        MutableDisposable holder = new() { Disposable = inner };
        holder.Dispose();
        holder.Dispose();
        await Assert.That(inner.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies single-assignment succeeds and <see cref = "OnceDisposable.IsAssigned"/> reflects state.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnceDisposableAssignedOnce_ThenIsAssignedTrueAndDisposed()
    {
        OnceDisposable holder = new();
        CountingDisposable inner = new();
        await Assert.That(holder.IsAssigned).IsFalse();
        await Assert.That(holder.IsDisposed).IsFalse();
        holder.Disposable = inner;
        await Assert.That(holder.IsAssigned).IsTrue();
        await Assert.That(holder.Disposable).IsSameReferenceAs(inner);
        holder.Dispose();
        await Assert.That(holder.IsDisposed).IsTrue();
        await Assert.That(inner.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that a second non-null assignment throws.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnceDisposableAssignedTwice_ThenThrows()
    {
        OnceDisposable holder = new() { Disposable = new CountingDisposable() };
        var ex = Assert.Throws<InvalidOperationException>(() => holder.Disposable = new CountingDisposable());
        await Assert.That(ex).IsNotNull();
    }

    /// <summary>Verifies assigning <see cref="EmptyDisposable.Instance"/> does not mark disposed and does not drop a later real assignment.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnceDisposableAssignedEmptyDisposable_ThenNotDisposedAndLaterRealAssignmentNotDropped()
    {
        OnceDisposable holder = new();
        holder.Disposable = EmptyDisposable.Instance;

        await Assert.That(holder.IsDisposed).IsFalse();
        await Assert.That(holder.Disposable).IsSameReferenceAs(EmptyDisposable.Instance);

        CountingDisposable late = new();
        var ex = Assert.Throws<InvalidOperationException>(() => holder.Disposable = late);
        await Assert.That(ex).IsNotNull();
        await Assert.That(late.DisposeCount).IsEqualTo(0);
    }

    /// <summary>Verifies that assigning after dispose disposes the supplied value and reports null via the getter.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnceDisposableAfterDispose_ThenAssignmentDisposesValueAndGetterReportsNull()
    {
        OnceDisposable holder = new();
        holder.Dispose();
        CountingDisposable late = new();
        holder.Disposable = late;
        await Assert.That(late.DisposeCount).IsEqualTo(1);
        await Assert.That(holder.Disposable).IsNull();
    }

    /// <summary>Verifies that disposing without ever assigning is a no-op.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnceDisposableDisposedUnassigned_ThenNoOp()
    {
        OnceDisposable holder = new();
        holder.Dispose();
        await Assert.That(holder.Disposable).IsNull();
    }

    /// <summary>Counts how many times <see cref = "IDisposable.Dispose"/> is invoked.</summary>
    private sealed class CountingDisposable : IDisposable
    {
        /// <summary>Gets the number of times <see cref = "Dispose"/> has been invoked.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
    }
}
