// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Disposables.Tests;

/// <summary>Tests for <see cref="MutableDisposable"/> — verifies that reassigning the inner does
/// NOT dispose the previous, that assigning after disposal immediately disposes the incoming
/// value, and that <c>Dispose</c> is idempotent.</summary>
public class MutableDisposableTests
{
    /// <summary>Verifies replacement leaves the previous inner alone.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenInnerReplaced_ThenPreviousIsNotDisposed()
    {
        using var holder = new MutableDisposable();
        var firstDisposed = 0;
        var secondDisposed = 0;
        var first = new ActionDisposable(() => firstDisposed++);
        var second = new ActionDisposable(() => secondDisposed++);

        holder.Disposable = first;
        holder.Disposable = second;

        await Assert.That(firstDisposed).IsEqualTo(0);
        await Assert.That(holder.Disposable).IsSameReferenceAs(second);
        await Assert.That(secondDisposed).IsEqualTo(0);
    }

    /// <summary>Verifies that assigning after disposal immediately disposes the incoming value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSetAfterDispose_ThenIncomingIsDisposedImmediately()
    {
        var holder = new MutableDisposable();
        holder.Dispose();
        var late = 0;

        holder.Disposable = new ActionDisposable(() => late++);

        await Assert.That(late).IsEqualTo(1);
    }

    /// <summary>Verifies that assigning <see langword="null"/> after disposal is a no-op.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSetNullAfterDispose_ThenNoThrow()
    {
        var holder = new MutableDisposable();
        holder.Dispose();

        holder.Disposable = null;

        await Assert.That(holder.Disposable).IsNull();
    }

    /// <summary>Verifies <c>Dispose</c> disposes the inner and is idempotent across repeated calls.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposedTwice_ThenInnerDisposedOnce()
    {
        var holder = new MutableDisposable();
        var disposed = 0;
        holder.Disposable = new ActionDisposable(() => disposed++);

        await Assert.That(holder.IsDisposed).IsFalse();

        holder.Dispose();
        holder.Dispose();

        await Assert.That(holder.IsDisposed).IsTrue();
        await Assert.That(disposed).IsEqualTo(1);
    }
}
