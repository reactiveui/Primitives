// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Disposables.Tests;

/// <summary>Tests the single-assignment disposable slot.</summary>
public class SingleDisposableTests
{
    /// <summary>The callbacks the sequence runs: an action and its assignment, plus an action with nothing assigned.</summary>
    private const int ActionAndDisposal = 3;

    /// <summary>Every constructor shape assigns once, and disposal runs the action then disposes the assignment once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructors_AssignOnceAndDisposeOnce()
    {
        var callbacks = 0;
        SingleDisposable empty = new();
        SingleDisposable withAction = new(() => callbacks++);
        SingleDisposable withDisposable = new(new BooleanDisposable());
        SingleDisposable withBoth = new(new ActionDisposable(() => callbacks++), () => callbacks++);
        BooleanDisposable late = new();

        empty.Dispose();
        empty.Create(late);
        withBoth.Dispose();
        withBoth.Dispose();
        withAction.Dispose();

        await Assert.That(() => withDisposable.Create(new BooleanDisposable())).Throws<InvalidOperationException>();
        await Assert.That(() => withAction.Create(null!)).Throws<ArgumentNullException>();
        await Assert.That(late.IsDisposed).IsTrue();
        await Assert.That(callbacks).IsEqualTo(ActionAndDisposal);
        await Assert.That(withBoth.IsDisposed).IsTrue();
    }

    /// <summary>Disposal runs the action even when the slot never took an assignment.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_WithNothingAssigned_RunsTheAction()
    {
        var callbacks = 0;
        SingleDisposable slot = new(() => callbacks++);

        slot.Dispose();

        await Assert.That(callbacks).IsEqualTo(1);
        await Assert.That(slot.IsDisposed).IsTrue();
    }

    /// <summary>Repeated disposal runs the action once, whether or not a value was assigned.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_Repeated_RunsTheActionOnce()
    {
        var callbacks = 0;
        SingleDisposable slot = new(() => callbacks++);

        slot.Dispose();
        slot.Dispose();
        slot.Create(new BooleanDisposable());

        await Assert.That(callbacks).IsEqualTo(1);
    }
}
