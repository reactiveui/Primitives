// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Tests.Internal;

/// <summary>Tests for the internal disposable holders
/// (<see cref="SwapDisposable"/>, <see cref="MutableDisposable"/>, <see cref="OnceDisposable"/>)
/// that back the sync-side sinks.</summary>
public class InternalDisposablesTests
{
    /// <summary>Verifies that assigning a new inner disposes the previous one.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwapDisposableReplaced_ThenPreviousDisposed()
    {
        var holder = new SwapDisposable();
        var first = new CountingDisposable();
        var second = new CountingDisposable();

        holder.Disposable = first;
        await Assert.That(holder.Disposable).IsSameReferenceAs(first);

        holder.Disposable = second;
        await Assert.That(first.DisposeCount).IsEqualTo(1);
        await Assert.That(holder.Disposable).IsSameReferenceAs(second);

        holder.Dispose();
        await Assert.That(second.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that once disposed, subsequent assignments dispose the supplied value immediately.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwapDisposableAfterDispose_ThenAssignmentDisposesValue()
    {
        var holder = new SwapDisposable();
        holder.Dispose();

        var late = new CountingDisposable();
        holder.Disposable = late;

        await Assert.That(late.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that double-dispose is a no-op on <see cref="SwapDisposable"/>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSwapDisposableDisposedTwice_ThenNoOp()
    {
        var inner = new CountingDisposable();
        var holder = new SwapDisposable { Disposable = inner };

        holder.Dispose();
        holder.Dispose();

        await Assert.That(inner.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that <see cref="MutableDisposable"/> replacement does NOT dispose the previous inner.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMutableDisposableReplaced_ThenPreviousNotDisposed()
    {
        var holder = new MutableDisposable();
        var first = new CountingDisposable();
        var second = new CountingDisposable();

        holder.Disposable = first;
        holder.Disposable = second;

        await Assert.That(first.DisposeCount).IsEqualTo(0);

        holder.Dispose();
        await Assert.That(second.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that assigning after dispose immediately disposes the value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMutableDisposableAfterDispose_ThenAssignmentDisposesValue()
    {
        var holder = new MutableDisposable();
        holder.Dispose();

        var late = new CountingDisposable();
        holder.Disposable = late;

        await Assert.That(late.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies that <see cref="MutableDisposable"/> double-dispose is a no-op.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMutableDisposableDisposedTwice_ThenNoOp()
    {
        var inner = new CountingDisposable();
        var holder = new MutableDisposable { Disposable = inner };

        holder.Dispose();
        holder.Dispose();

        await Assert.That(inner.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies single-assignment succeeds and <see cref="OnceDisposable.IsAssigned"/> reflects state.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnceDisposableAssignedOnce_ThenIsAssignedTrueAndDisposed()
    {
        var holder = new OnceDisposable();
        var inner = new CountingDisposable();

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
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnceDisposableAssignedTwice_ThenThrows()
    {
        var holder = new OnceDisposable { Disposable = new CountingDisposable() };

        var ex = Assert.Throws<InvalidOperationException>(() => holder.Disposable = new CountingDisposable());
        await Assert.That(ex).IsNotNull();
    }

    /// <summary>Verifies that assigning after dispose disposes the supplied value and reports null via the getter.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnceDisposableAfterDispose_ThenAssignmentDisposesValueAndGetterReportsNull()
    {
        var holder = new OnceDisposable();
        holder.Dispose();

        var late = new CountingDisposable();
        holder.Disposable = late;

        await Assert.That(late.DisposeCount).IsEqualTo(1);
        await Assert.That(holder.Disposable).IsNull();
    }

    /// <summary>Verifies that disposing without ever assigning is a no-op.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnceDisposableDisposedUnassigned_ThenNoOp()
    {
        var holder = new OnceDisposable();
        holder.Dispose();

        await Assert.That(holder.Disposable).IsNull();
    }

    /// <summary>Counts how many times <see cref="IDisposable.Dispose"/> is invoked.</summary>
    private sealed class CountingDisposable : IDisposable
    {
        /// <summary>Gets the number of times <see cref="Dispose"/> has been invoked.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
    }
}
