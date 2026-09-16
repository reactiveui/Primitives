// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Disposables.Tests;

/// <summary>Tests the Primitives alias for a group of disposables that are disposed together.</summary>
public class PocketTests
{
    /// <summary>The number of disposables held by the two-item constructor.</summary>
    private const int TwoItems = 2;

    /// <summary>The number of disposables held by the three-item constructor.</summary>
    private const int ThreeItems = 3;

    /// <summary>Every constructor shape holds its disposables and the pocket forwards the collection members.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructors_HoldDisposablesAndForwardCollectionMembers()
    {
        BooleanDisposable first = new();
        BooleanDisposable second = new();
        BooleanDisposable third = new();
        Pocket empty = new();
        Pocket pair = new(first, second);
        Pocket triple = new(first, second, third);
        Pocket array = new([first]);
        var target = new IDisposable[ThreeItems];

        triple.CopyTo(target, 0);
        List<IDisposable> enumerated = [];
        foreach (var item in (IEnumerable)triple)
        {
            enumerated.Add((IDisposable)item);
        }

        await Assert.That(empty.Count).IsEqualTo(0);
        await Assert.That(pair.Count).IsEqualTo(TwoItems);
        await Assert.That(triple.Count).IsEqualTo(ThreeItems);
        await Assert.That(array.Contains(first)).IsTrue();
        await Assert.That(triple.IsReadOnly).IsFalse();
        await Assert.That(target.SequenceEqual([first, second, third])).IsTrue();
        await Assert.That(enumerated.SequenceEqual([first, second, third])).IsTrue();
        await Assert.That(static () => new Pocket(null!)).Throws<ArgumentNullException>();
    }

    /// <summary>Add, Remove, Clear and Dispose release the held disposables.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Mutations_ReleaseTheHeldDisposables()
    {
        BooleanDisposable removed = new();
        BooleanDisposable cleared = new();
        BooleanDisposable disposed = new();
        Pocket pocket = [removed];

        var wasRemoved = pocket.Remove(removed);
        pocket.Add(cleared);
        pocket.Clear();
        pocket.Add(disposed);
        pocket.Dispose();

        await Assert.That(wasRemoved).IsTrue();
        await Assert.That(removed.IsDisposed).IsTrue();
        await Assert.That(cleared.IsDisposed).IsTrue();
        await Assert.That(disposed.IsDisposed).IsTrue();
        await Assert.That(pocket.IsDisposed).IsTrue();
    }
}
