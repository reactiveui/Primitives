// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Disposables.Tests;

/// <summary>Tests the Primitives alias for a single-assignment disposable slot.</summary>
public class AssignmentSlotTests
{
    /// <summary>The number of callbacks run by the action and the assigned disposable.</summary>
    private const int ActionAndDisposal = 2;

    /// <summary>Every constructor shape assigns once, and disposal runs the action then disposes the assignment once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructors_AssignOnceAndDisposeOnce()
    {
        var callbacks = 0;
        BooleanDisposable assigned = new();
        AssignmentSlot empty = new();
        AssignmentSlot withAction = new(() => callbacks++);
        AssignmentSlot withDisposable = new(assigned);
        AssignmentSlot withBoth = new(new ActionDisposable(() => callbacks++), () => callbacks++);

        empty.Create(new BooleanDisposable());
        withBoth.Dispose();
        withBoth.Dispose();
        withAction.Dispose();

        await Assert.That(() => withDisposable.Create(new BooleanDisposable())).Throws<InvalidOperationException>();
        await Assert.That(() => empty.Create(null!)).Throws<ArgumentNullException>();
        await Assert.That(callbacks).IsEqualTo(ActionAndDisposal);
        await Assert.That(withBoth.IsDisposed).IsTrue();
        await Assert.That(withDisposable.IsDisposed).IsFalse();
    }

    /// <summary>A value assigned after disposal is disposed immediately.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Create_AfterDispose_DisposesTheIncomingValue()
    {
        BooleanDisposable late = new();
        AssignmentSlot slot = new();
        slot.Dispose();

        slot.Create(late);

        await Assert.That(late.IsDisposed).IsTrue();
    }
}
