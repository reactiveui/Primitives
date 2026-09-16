// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Disposables.Tests;

/// <summary>Tests the embeddable group of disposables that are disposed together.</summary>
public class DisposableSetTests
{
    /// <summary>The number of disposables that forces the overflow storage to grow.</summary>
    private const int GrowthCount = 5;

    /// <summary>The number of disposables held by the two-item constructor.</summary>
    private const int TwoItems = 2;

    /// <summary>The number of disposables held by the three-item constructor.</summary>
    private const int ThreeItems = 3;

    /// <summary>The default value is an empty set that accepts, counts and disposes disposables.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DefaultSet_AddsCountsAndDisposes()
    {
        CountingDisposable first = new();
        CountingDisposable late = new();
        DisposableSet set = default;

        set.Add(first);
        var countBeforeDispose = set.Count;
        set.Dispose();
        set.Dispose();
        set.Add(late);

        await Assert.That(countBeforeDispose).IsEqualTo(1);
        await Assert.That(first.DisposeCount).IsEqualTo(1);
        await Assert.That(late.DisposeCount).IsEqualTo(1);
        await Assert.That(set.IsDisposed).IsTrue();
        await Assert.That(set.Count).IsEqualTo(0);
        await Assert.That(set.Contains(first)).IsFalse();
        await Assert.That(set.Remove(first)).IsFalse();
        await Assert.That(set.Snapshot().Count).IsEqualTo(0);
    }

    /// <summary>Clearing a disposed set changes nothing.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Clear_AfterDispose_DoesNothing()
    {
        DisposableSet set = default;
        set.Dispose();

        set.Clear();

        await Assert.That(set.IsDisposed).IsTrue();
    }

    /// <summary>The positional constructors keep registration order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task PositionalConstructors_KeepRegistrationOrder()
    {
        CountingDisposable first = new();
        CountingDisposable second = new();
        CountingDisposable third = new();

        DisposableSet pair = new(first, second);
        DisposableSet triple = new(first, second, third);

        await Assert.That(pair.Count).IsEqualTo(TwoItems);
        await Assert.That(triple.Count).IsEqualTo(ThreeItems);
        await Assert.That(triple.Snapshot().SequenceEqual([first, second, third])).IsTrue();
    }

    /// <summary>The array constructor skips null entries and rejects a null array.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ArrayConstructor_SkipsNullEntriesAndRejectsNull()
    {
        CountingDisposable first = new();
        DisposableSet set = new([first, null!]);

        await Assert.That(set.Count).IsEqualTo(1);
        await Assert.That(static () => new DisposableSet(null!)).Throws<ArgumentNullException>();
    }

    /// <summary>Adding past the inline slots grows the overflow storage and disposes everything in order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Add_PastInlineSlots_GrowsOverflowAndDisposesInOrder()
    {
        List<int> order = [];
        DisposableSet set = default;
        for (var i = 0; i < GrowthCount; i++)
        {
            var index = i;
            set.Add(new ActionDisposable(() => order.Add(index)));
        }

        var count = set.Count;
        set.Dispose();

        await Assert.That(count).IsEqualTo(GrowthCount);
        await Assert.That(string.Join(",", order)).IsEqualTo("0,1,2,3,4");
        await Assert.That(() => set.Add(null!)).Throws<ArgumentNullException>();
    }

    /// <summary>Contains finds items in either inline slot and in the overflow, and not unknown or null items.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Contains_FindsInlineAndOverflowItems()
    {
        CountingDisposable first = new();
        CountingDisposable second = new();
        CountingDisposable third = new();
        CountingDisposable unknown = new();
        DisposableSet inlineOnly = new(first, second);
        DisposableSet set = new(first, second, third);

        await Assert.That(set.Contains(first)).IsTrue();
        await Assert.That(set.Contains(second)).IsTrue();
        await Assert.That(set.Contains(third)).IsTrue();
        await Assert.That(set.Contains(unknown)).IsFalse();
        await Assert.That(set.Contains(null)).IsFalse();
        await Assert.That(inlineOnly.Contains(unknown)).IsFalse();

        _ = set.Remove(first);
        _ = set.Remove(second);

        await Assert.That(set.Contains(third)).IsTrue();
        await Assert.That(set.Contains(unknown)).IsFalse();
    }

    /// <summary>Remove disposes items from each inline slot and the overflow, shifting later overflow items down.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Remove_DisposesItemsFromEverySlot()
    {
        CountingDisposable first = new();
        CountingDisposable second = new();
        CountingDisposable third = new();
        CountingDisposable fourth = new();
        CountingDisposable unknown = new();
        DisposableSet inlineOnly = new(first, second);
        DisposableSet set = new(first, second, third);
        set.Add(fourth);

        var removedThird = set.Remove(third);
        var removedFirst = set.Remove(first);
        var removedSecond = set.Remove(second);
        var removedUnknown = set.Remove(unknown);
        var inlineRemovedUnknown = inlineOnly.Remove(unknown);

        await Assert.That(removedThird).IsTrue();
        await Assert.That(removedFirst).IsTrue();
        await Assert.That(removedSecond).IsTrue();
        await Assert.That(removedUnknown).IsFalse();
        await Assert.That(inlineRemovedUnknown).IsFalse();
        await Assert.That(third.DisposeCount).IsEqualTo(1);
        await Assert.That(set.Snapshot().SequenceEqual([fourth])).IsTrue();
        await Assert.That(() => set.Remove(null)).Throws<ArgumentNullException>();
    }

    /// <summary>Clear disposes every item and leaves the set usable.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Clear_DisposesEveryItemAndKeepsTheSetUsable()
    {
        CountingDisposable first = new();
        CountingDisposable second = new();
        CountingDisposable third = new();
        CountingDisposable later = new();
        DisposableSet set = new(first, second, third);

        set.Clear();
        set.Add(later);

        await Assert.That(first.DisposeCount + second.DisposeCount + third.DisposeCount).IsEqualTo(ThreeItems);
        await Assert.That(set.IsDisposed).IsFalse();
        await Assert.That(set.Snapshot().SequenceEqual([later])).IsTrue();
    }

    /// <summary>CopyTo copies the held items and validates its arguments.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CopyTo_CopiesItemsAndValidatesArguments()
    {
        CountingDisposable first = new();
        CountingDisposable second = new();
        DisposableSet set = new(first, second);
        var target = new IDisposable[ThreeItems];

        set.CopyTo(target, 1);

        await Assert.That(target[1]).IsSameReferenceAs(first);
        await Assert.That(target[TwoItems]).IsSameReferenceAs(second);
        await Assert.That(() => set.CopyTo(null!, 0)).Throws<ArgumentNullException>();
        await Assert.That(() => set.CopyTo(target, -1)).Throws<ArgumentOutOfRangeException>();
    }

    /// <summary>Counts every disposal invocation.</summary>
    private sealed class CountingDisposable : IDisposable
    {
        /// <summary>Gets the number of disposal invocations.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Dispose() => DisposeCount++;
    }
}
