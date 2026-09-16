// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Disposables.Tests;

/// <summary>Tests the Primitives alias for a replaceable disposable slot.</summary>
public class SlotTests
{
    /// <summary>The number of callbacks run when a value is assigned after disposal.</summary>
    private const int DisposeAndLateAssignment = 2;

    /// <summary>Replacing the value disposes the displaced one, and disposal releases the current value and runs the action once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Create_ReplacesAndDisposeReleasesTheCurrentValue()
    {
        var callbacks = 0;
        BooleanDisposable initial = new();
        BooleanDisposable replacement = new();
        Slot slot = new(initial, () => callbacks++);
        Slot fromDisposable = new(new BooleanDisposable());

        slot.Create(replacement);
        slot.Dispose();
        slot.Dispose();
        fromDisposable.Dispose();

        await Assert.That(initial.IsDisposed).IsTrue();
        await Assert.That(replacement.IsDisposed).IsTrue();
        await Assert.That(callbacks).IsEqualTo(1);
        await Assert.That(slot.IsDisposed).IsTrue();
        await Assert.That(() => slot.Create(null!)).Throws<ArgumentNullException>();
    }

    /// <summary>A slot with no value runs its action on disposal, and a value assigned afterwards is disposed and runs the action again.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Create_AfterDispose_DisposesTheIncomingValue()
    {
        var callbacks = 0;
        BooleanDisposable late = new();
        Slot empty = new();
        Slot slot = new(() => callbacks++);

        empty.Dispose();
        slot.Dispose();
        slot.Create(late);

        await Assert.That(late.IsDisposed).IsTrue();
        await Assert.That(callbacks).IsEqualTo(DisposeAndLateAssignment);
        await Assert.That(empty.IsDisposed).IsTrue();
    }
}
