// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Coverage for the public <see cref="Broadcaster{T}"/> equality and copy-on-write surface.</summary>
public class BroadcasterTests
{
    /// <summary>The literal one.</summary>
    private const int One = 1;

    /// <summary>The literal two.</summary>
    private const int Two = 2;

    /// <summary>The literal three.</summary>
    private const int Three = 3;

    /// <summary>The equality operators compare the underlying observer set by reference.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task BroadcasterEqualityOperatorsCompareTheObserverSet()
    {
        Broadcaster<int> left = default;
        Broadcaster<int> right = default;

        // Both empty -> same (null) observer set.
        await Assert.That(left == right).IsTrue();
        await Assert.That(left != right).IsFalse();
        left.Add(new DelegateWitness<int>(_ => { }));

        // Left now references an observer set; right is still empty.
        await Assert.That(left != right).IsTrue();
        await Assert.That(left == right).IsFalse();
    }

    /// <summary>Covers broadcaster copy-on-write, signal late-terminal, and buffer disposal/error branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task BroadcasterCopyOnWriteSignalAndBufferCoverTerminalEdges()
    {
        Broadcaster<int> broadcaster = default;
        RecordingWitness<int> first = new();
        RecordingWitness<int> second = new();
        RecordingWitness<int> third = new();
        RecordingWitness<int> fourth = new();
        RecordingWitness<int> missing = new();
        broadcaster.Add(first);
        broadcaster.Add(second);
        broadcaster.Add(third);
        broadcaster.Add(fourth);
        await Assert.That(broadcaster.HasObservers).IsTrue();
        broadcaster.Remove(missing);
        broadcaster.Remove(second);
        broadcaster.Next(One);
        broadcaster.Error(new InvalidOperationException("broadcast"));
        broadcaster.Completed();
        var copy = broadcaster;
        await Assert.That(broadcaster.Equals(copy)).IsTrue();
        await Assert.That(broadcaster.Equals((object)copy)).IsTrue();
        await Assert.That(broadcaster.Equals("not a broadcaster")).IsFalse();
        await Assert.That(broadcaster.GetHashCode()).IsNotEqualTo(0);
        broadcaster.Clear();
        await Assert.That(broadcaster.HasObservers).IsFalse();
        await Assert.That(first.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(second.Values.Count).IsEqualTo(0);
        await Assert.That(third.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(fourth.Values.SequenceEqual([One])).IsTrue();
        await Assert.That(first.Errors.Count).IsEqualTo(1);
        await Assert.That(third.Completed).IsEqualTo(1);
        await Assert.That(fourth.Completed).IsEqualTo(1);
        Signal<int> completedSignal = new();
        completedSignal.OnCompleted();
        completedSignal.Subscribe(_ => { }).Dispose();
        Signal<int> failedSignal = new();
        failedSignal.OnError(new InvalidOperationException("late action"));
        _ = Assert.Throws<InvalidOperationException>(() => failedSignal.Subscribe(_ => { }).Dispose());
        Signal<int> source = new();
        List<IList<int>> buffers = [];
        using (source.Buffer(Three, Two).Subscribe(buffers.Add))
        {
            source.OnNext(One);
            source.OnNext(Two);

            // The window (size 3) is incomplete; completion flushes the partial trailing window.
            source.OnCompleted();
        }

        await Assert.That(buffers.Count).IsEqualTo(1);
        await Assert.That(buffers[0].SequenceEqual([One, Two])).IsTrue();
        Signal<int> errorSource = new();
        var bufferError = false;
        using (errorSource.Buffer(Two, One).Subscribe(_ => { }, _ => bufferError = true, () => { }))
        {
            errorSource.OnError(new InvalidOperationException("buffer-error"));
        }

        await Assert.That(bufferError).IsTrue();
    }
}
