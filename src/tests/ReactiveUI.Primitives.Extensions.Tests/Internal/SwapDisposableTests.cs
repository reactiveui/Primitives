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

/// <summary>Tests for <see cref="SwapDisposable"/> — verifies replacement disposes the previous
/// inner, assigning after disposal immediately disposes the incoming value, and <c>Dispose</c>
/// is idempotent.</summary>
public class SwapDisposableTests
{
    /// <summary>Verifies that replacement disposes the previous inner.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenInnerReplaced_ThenPreviousIsDisposed()
    {
        using var holder = new SwapDisposable();
        var firstDisposed = 0;
        var secondDisposed = 0;
        holder.Disposable = new ActionDisposable(() => firstDisposed++);
        holder.Disposable = new ActionDisposable(() => secondDisposed++);

        await Assert.That(firstDisposed).IsEqualTo(1);
        await Assert.That(secondDisposed).IsEqualTo(0);
    }

    /// <summary>Verifies that the getter returns the currently-assigned inner.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGetCurrent_ThenReturnsCurrent()
    {
        using var holder = new SwapDisposable();
        var current = new ActionDisposable(static () => { });

        holder.Disposable = current;

        await Assert.That(holder.Disposable).IsSameReferenceAs(current);
    }

    /// <summary>Verifies that assigning after disposal immediately disposes the incoming value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSetAfterDispose_ThenIncomingIsDisposedImmediately()
    {
        var holder = new SwapDisposable();
        holder.Dispose();
        var late = 0;

        holder.Disposable = new ActionDisposable(() => late++);

        await Assert.That(late).IsEqualTo(1);
    }

    /// <summary>Verifies <c>Dispose</c> is idempotent across repeated calls.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDisposedTwice_ThenInnerDisposedOnce()
    {
        var holder = new SwapDisposable();
        var disposed = 0;
        holder.Disposable = new ActionDisposable(() => disposed++);

        await Assert.That(holder.IsDisposed).IsFalse();

        holder.Dispose();
        holder.Dispose();

        await Assert.That(holder.IsDisposed).IsTrue();
        await Assert.That(disposed).IsEqualTo(1);
    }
}
