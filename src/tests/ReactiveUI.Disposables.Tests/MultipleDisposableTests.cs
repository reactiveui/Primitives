// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Collections;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Disposables.Tests;

/// <summary>Tests the group of disposables that are disposed together.</summary>
public class MultipleDisposableTests
{
    /// <summary>The number of disposables held by the two-item constructor.</summary>
    private const int TwoItems = 2;

    /// <summary>The number of disposables held by the three-item constructor.</summary>
    private const int ThreeItems = 3;

    /// <summary>Every constructor shape holds its disposables and the group forwards the collection members.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Constructors_HoldDisposablesAndForwardCollectionMembers()
    {
        BooleanDisposable first = new();
        BooleanDisposable second = new();
        BooleanDisposable third = new();
        MultipleDisposable empty = new();
        MultipleDisposable pair = new(first, second);
        MultipleDisposable triple = new(first, second, third);
        MultipleDisposable array = new([first]);
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
        await Assert.That(static () => new MultipleDisposable(null!)).Throws<ArgumentNullException>();
    }

    /// <summary>Add, Remove, Clear and Dispose release the held disposables.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Mutations_ReleaseTheHeldDisposables()
    {
        BooleanDisposable removed = new();
        BooleanDisposable cleared = new();
        BooleanDisposable disposed = new();
        MultipleDisposable group = [removed];

        var wasRemoved = group.Remove(removed);
        group.Add(cleared);
        group.Clear();
        group.Add(disposed);
        group.Dispose();

        await Assert.That(wasRemoved).IsTrue();
        await Assert.That(removed.IsDisposed).IsTrue();
        await Assert.That(cleared.IsDisposed).IsTrue();
        await Assert.That(disposed.IsDisposed).IsTrue();
        await Assert.That(group.IsDisposed).IsTrue();
    }

    /// <summary>The factory disposes its non-null disposables once and rejects a null array.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Create_DisposesNonNullEntriesOnce()
    {
        var disposals = 0;
        var group = MultipleDisposable.Create(new ActionDisposable(() => disposals++), null!);

        group.Dispose();
        group.Dispose();

        await Assert.That(disposals).IsEqualTo(1);
        await Assert.That(static () => MultipleDisposable.Create(null!)).Throws<ArgumentNullException>();
    }
}
