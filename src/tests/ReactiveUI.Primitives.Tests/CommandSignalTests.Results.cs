// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests result observer updates against explicit current and stale snapshots.</summary>
public sealed partial class CommandSignalTests
{
    /// <summary>Stale additions leave the published observer set unchanged.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ResultAdditionsRetryStaleSnapshots()
    {
        RecordingWitness<int> first = new();
        RecordingWitness<int> second = new();
        RecordingWitness<int> third = new();
        object? storage = null;
        await Assert.That(CommandSignal<int>.TryAddResult(ref storage, null, first)).IsTrue();
        await Assert.That(CommandSignal<int>.TryAddResult(ref storage, null, second)).IsFalse();
        await Assert.That(storage).IsSameReferenceAs(first);

        await Assert.That(CommandSignal<int>.TryAddResult(ref storage, first, second)).IsTrue();
        var pair = storage;
        await Assert.That(CommandSignal<int>.TryAddResult(ref storage, first, third)).IsFalse();
        await Assert.That(storage).IsSameReferenceAs(pair);
        await Assert.That(CommandSignal<int>.TryAddResult(ref storage, pair, third)).IsTrue();
        await Assert.That(((IObserver<int>[])storage!).SequenceEqual([first, second, third])).IsTrue();
        await Assert.That(CommandSignal<int>.TryAddResult(ref storage, pair, first)).IsFalse();
    }

    /// <summary>Removal retries changed snapshots and preserves absent observers without an update.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ResultRemovalsDistinguishStaleAndAbsentObservers()
    {
        RecordingWitness<int> first = new();
        RecordingWitness<int> second = new();
        RecordingWitness<int> third = new();
        RecordingWitness<int> absent = new();
        IObserver<int>[] original = [first, second];
        IObserver<int>[] current = [first, second, third];
        object? storage = current;
        await Assert.That(CommandSignal<int>.TryRemoveResult(ref storage, original, first)).IsFalse();
        await Assert.That(storage).IsSameReferenceAs(current);
        await Assert.That(CommandSignal<int>.TryRemoveResult(ref storage, current, absent)).IsTrue();
        await Assert.That(storage).IsSameReferenceAs(current);

        await Assert.That(CommandSignal<int>.TryRemoveResult(ref storage, current, first)).IsTrue();
        await Assert.That(((IObserver<int>[])storage!).SequenceEqual([second, third])).IsTrue();
        await Assert.That(CommandSignal<int>.TryRemoveResult(ref storage, storage, second)).IsTrue();
        await Assert.That(storage).IsSameReferenceAs(third);
        await Assert.That(CommandSignal<int>.TryRemoveResult(ref storage, storage, third)).IsTrue();
        await Assert.That(storage).IsNull();
        await Assert.That(CommandSignal<int>.TryRemoveResult(ref storage, null, absent)).IsTrue();
        await Assert.That(storage).IsNull();
    }
}
