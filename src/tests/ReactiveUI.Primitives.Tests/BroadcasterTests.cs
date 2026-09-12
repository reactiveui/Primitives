// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for the public <see cref="Broadcaster{T}"/> equality and copy-on-write surface.</summary>
public class BroadcasterTests
{
    /// <summary>The literal one.</summary>
    private const int One = 1;

    /// <summary>The literal two.</summary>
    private const int Two = 2;

    /// <summary>The literal three.</summary>
    private const int Three = 3;

    /// <summary>A stale observer snapshot cannot replace a newer addition.</summary>
    /// <param name="observerCount">The number of observers in the original snapshot.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(One)]
    [Arguments(Two)]
    public async Task TryAdd_StaleSnapshot_PreservesCompetingAddition(int observerCount)
    {
        RecordingWitness<int> first = new();
        RecordingWitness<int> second = new();
        RecordingWitness<int> competing = new();
        RecordingWitness<int> incoming = new();
        IObserver<int>[] initial = observerCount switch
        {
            0 => [],
            One => [first],
            _ => [first, second],
        };
        object? observers = observerCount switch
        {
            0 => null,
            One => first,
            _ => initial,
        };
        var stale = observers;
        await Assert.That(Broadcaster<int>.TryAdd(ref observers, stale, competing)).IsTrue();
        var current = observers;

        await Assert.That(Broadcaster<int>.TryAdd(ref observers, stale, incoming)).IsFalse();
        await Assert.That(observers).IsSameReferenceAs(current);

        await Assert.That(Broadcaster<int>.TryAdd(ref observers, current, incoming)).IsTrue();
        var actual = await Assert.That(observers).IsTypeOf<IObserver<int>[]>().And.IsNotNull();
        await Assert.That(actual.SequenceEqual(initial.Append(competing).Append(incoming))).IsTrue();
    }

    /// <summary>Removal retries against the current snapshot without losing a competing addition.</summary>
    /// <param name="multiple">True when the original snapshot contains two observers.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task TryRemove_StaleSnapshot_PreservesCompetingAddition(bool multiple)
    {
        RecordingWitness<int> removed = new();
        RecordingWitness<int> retained = new();
        RecordingWitness<int> competing = new();
        object? observers = multiple ? new IObserver<int>[] { removed, retained } : removed;
        var stale = observers;
        await Assert.That(Broadcaster<int>.TryAdd(ref observers, stale, competing)).IsTrue();
        var current = observers;

        await Assert.That(Broadcaster<int>.TryRemove(ref observers, stale, removed)).IsFalse();
        await Assert.That(observers).IsSameReferenceAs(current);
        await Assert.That(Broadcaster<int>.TryRemove(ref observers, current, removed)).IsTrue();

        if (multiple)
        {
            var actual = await Assert.That(observers).IsTypeOf<IObserver<int>[]>().And.IsNotNull();
            await Assert.That(actual.SequenceEqual([retained, competing])).IsTrue();
        }
        else
        {
            await Assert.That(observers).IsSameReferenceAs(competing);
        }
    }

    /// <summary>Removing an absent observer does not change an empty or single-observer slot.</summary>
    /// <param name="empty">True when no observer is registered.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task TryRemove_AbsentObserver_LeavesSlotUnchanged(bool empty)
    {
        RecordingWitness<int> retained = new();
        RecordingWitness<int> missing = new();
        object? observers = empty ? null : retained;
        var current = observers;

        await Assert.That(Broadcaster<int>.TryRemove(ref observers, current, missing)).IsTrue();
        await Assert.That(ReferenceEquals(observers, current)).IsTrue();
    }

    /// <summary>The equality operators compare the underlying observer set by reference.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task BroadcasterEqualityOperatorsCompareTheObserverSet()
    {
        Broadcaster<int> left = default;
        Broadcaster<int> right = default;

        await Assert.That(left == right).IsTrue();
        await Assert.That(left != right).IsFalse();
        left.Add(new DelegateWitness<int>(static _ => { }));

        // Left references an observer set; right is empty.
        await Assert.That(left != right).IsTrue();
        await Assert.That(left == right).IsFalse();
    }

    /// <summary>An empty broadcaster hashes to zero and a single-observer broadcaster hashes to that observer's identity.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task BroadcasterHashesToZeroWhenEmptyAndToTheObserverIdentityWhenSingle()
    {
        Broadcaster<int> empty = default;
        await Assert.That(empty.GetHashCode()).IsEqualTo(0);

        RecordingWitness<int> only = new();
        Broadcaster<int> single = default;
        Broadcaster<int> alsoSingle = default;
        single.Add(only);
        alsoSingle.Add(only);

        await Assert.That(single.GetHashCode()).IsEqualTo(RuntimeHelpers.GetHashCode(only));
        await Assert.That(single.GetHashCode()).IsEqualTo(alsoSingle.GetHashCode());
        await Assert.That(single.GetHashCode()).IsNotEqualTo(empty.GetHashCode());
        await Assert.That(single.Equals(alsoSingle)).IsTrue();
    }

    /// <summary>Verifies broadcaster copy-on-write, late terminal notifications on a signal, and buffer error handling.</summary>
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
        completedSignal.Subscribe(static _ => { }).Dispose();
        Signal<int> failedSignal = new();
        failedSignal.OnError(new InvalidOperationException("late action"));
        _ = Assert.Throws<InvalidOperationException>(() => failedSignal.Subscribe(static _ => { }).Dispose());
        Signal<int> source = new();
        List<IList<int>> buffers = [];
        using (source.Buffer(Three, Two).Subscribe(buffers.Add))
        {
            source.OnNext(One);
            source.OnNext(Two);

            // The size-3 window is incomplete, so completion flushes the partial window.
            source.OnCompleted();
        }

        await Assert.That(buffers.Count).IsEqualTo(1);
        await Assert.That(buffers[0].SequenceEqual([One, Two])).IsTrue();
        Signal<int> errorSource = new();
        var bufferError = false;
        using (errorSource.Buffer(Two, One).Subscribe(static _ => { }, _ => bufferError = true, static () => { }))
        {
            errorSource.OnError(new InvalidOperationException("buffer-error"));
        }

        await Assert.That(bufferError).IsTrue();
    }
}
