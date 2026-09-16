// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="SerializedBroadcaster{T}"/> and <see cref="SerializedBroadcast{T}"/>.</summary>
public sealed class SerializedBroadcasterTests
{
    /// <summary>The first posted value.</summary>
    private const int First = 1;

    /// <summary>The second posted value.</summary>
    private const int Second = 2;

    /// <summary>Posting to an empty broadcaster reaches nothing, and every batch it returns, like the default batch, flushes quietly.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task EmptyBroadcasterPostsNothingAndItsBatchesFlushQuietly()
    {
        SerializedBroadcaster<int> broadcaster = default;

        broadcaster.PostNext(First).Flush();
        broadcaster.PostError(new InvalidOperationException("empty")).Flush();
        broadcaster.PostCompleted().Flush();
        default(SerializedBroadcast<int>).Flush();

        await Assert.That(broadcaster.HasObservers).IsFalse();
    }

    /// <summary>A single witness receives posted values only when their batches are flushed, in posted order.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SingleWitnessReceivesPostedValuesWhenFlushed()
    {
        RecordingWitness<int> observer = new();
        SerializedBroadcaster<int> broadcaster = default;
        broadcaster.Add(new(observer));

        var first = broadcaster.PostNext(First);
        var second = broadcaster.PostNext(Second);
        var deliveredBeforeFlush = observer.Values.Count;
        first.Flush();
        second.Flush();

        await Assert.That(broadcaster.HasObservers).IsTrue();
        await Assert.That(deliveredBeforeFlush).IsEqualTo(0);
        await Assert.That(observer.Values.SequenceEqual([First, Second])).IsTrue();
    }

    /// <summary>A single witness receives a posted error or completion when the batch is flushed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SingleWitnessReceivesAPostedTerminalWhenFlushed()
    {
        RecordingWitness<int> failing = new();
        RecordingWitness<int> completing = new();
        SerializedBroadcaster<int> errors = default;
        SerializedBroadcaster<int> completions = default;
        errors.Add(new(failing));
        completions.Add(new(completing));
        InvalidOperationException expected = new("single");

        errors.PostError(expected).Flush();
        completions.PostCompleted().Flush();

        await Assert.That(failing.Errors.Single()).IsSameReferenceAs(expected);
        await Assert.That(completing.Completed).IsEqualTo(1);
    }

    /// <summary>Every witness in a larger set receives posted values and terminals when the batch is flushed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task EveryWitnessReceivesPostedValuesAndTerminalsWhenFlushed()
    {
        RecordingWitness<int>[] failing = [new(), new(), new()];
        RecordingWitness<int>[] completing = [new(), new()];
        SerializedBroadcaster<int> errors = default;
        SerializedBroadcaster<int> completions = default;
        foreach (var observer in failing)
        {
            errors.Add(new(observer));
        }

        foreach (var observer in completing)
        {
            completions.Add(new(observer));
        }

        InvalidOperationException expected = new("many");

        errors.PostNext(First).Flush();
        errors.PostError(expected).Flush();
        completions.PostCompleted().Flush();

        await Assert.That(Array.TrueForAll(failing, static observer => observer.Values.SequenceEqual([First]))).IsTrue();
        await Assert.That(Array.TrueForAll(failing, observer => ReferenceEquals(observer.Errors.Single(), expected))).IsTrue();
        await Assert.That(Array.TrueForAll(completing, static observer => observer.Completed == 1)).IsTrue();
    }

    /// <summary>Removing witnesses shrinks the set through each shape, ignores unknown witnesses, and clearing empties it.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task RemovingWitnessesShrinksTheSetAndIgnoresUnknownWitnesses()
    {
        RecordingWitness<int> a = new();
        RecordingWitness<int> b = new();
        RecordingWitness<int> c = new();
        SerializedWitness<int> witnessA = new(a);
        SerializedWitness<int> witnessB = new(b);
        SerializedWitness<int> witnessC = new(c);
        SerializedWitness<int> unknown = new(new RecordingWitness<int>());
        SerializedBroadcaster<int> broadcaster = default;

        broadcaster.Remove(unknown);
        broadcaster.Add(witnessA);
        broadcaster.Remove(unknown);
        broadcaster.Add(witnessB);
        broadcaster.Add(witnessC);
        broadcaster.Remove(unknown);
        broadcaster.Remove(witnessB);
        broadcaster.PostNext(First).Flush();
        broadcaster.Remove(witnessC);
        broadcaster.Add(witnessB);
        broadcaster.Remove(witnessA);
        broadcaster.PostNext(Second).Flush();
        broadcaster.Remove(witnessB);
        var emptyAfterRemovals = !broadcaster.HasObservers;
        broadcaster.Add(witnessA);
        broadcaster.Clear();

        await Assert.That(emptyAfterRemovals).IsTrue();
        await Assert.That(broadcaster.HasObservers).IsFalse();
        await Assert.That(a.Values.SequenceEqual([First])).IsTrue();
        await Assert.That(b.Values.SequenceEqual([Second])).IsTrue();
        await Assert.That(c.Values.SequenceEqual([First])).IsTrue();
    }

    /// <summary>A value posted to a witness whose delivery is running is queued behind that delivery, for one witness and for several.</summary>
    /// <param name="witnessCount">The number of registered witnesses.</param>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    [Arguments(1)]
    [Arguments(Second)]
    public async Task ValuePostedDuringADeliveryIsQueuedBehindIt(int witnessCount)
    {
        SerializedBroadcaster<int> broadcaster = default;
        List<int>[] received = [.. Enumerable.Range(0, witnessCount).Select(static _ => new List<int>())];
        for (var i = 0; i < witnessCount; i++)
        {
            var values = received[i];
            var postsNested = i == 0;
            broadcaster.Add(new(new CallbackRecordingWitness<int>(value =>
            {
                values.Add(value);
                if (postsNested && value == First)
                {
                    broadcaster.PostNext(Second).Flush();
                }
            })));
        }

        broadcaster.PostNext(First).Flush();

        await Assert.That(Array.TrueForAll(received, static values => values.SequenceEqual([First, Second]))).IsTrue();
    }

    /// <summary>Witnesses beyond the claim capacity still receive every posted value.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WitnessesBeyondTheClaimCapacityReceiveTheValue()
    {
        const int WitnessCount = SerializedBroadcast<int>.ClaimCapacity + 1;
        SerializedBroadcaster<int> broadcaster = default;
        RecordingWitness<int>[] observers = [.. Enumerable.Range(0, WitnessCount).Select(static _ => new RecordingWitness<int>())];
        foreach (var observer in observers)
        {
            broadcaster.Add(new(observer));
        }

        broadcaster.PostNext(First).Flush();

        await Assert.That(Array.TrueForAll(observers, static observer => observer.Values.SequenceEqual([First]))).IsTrue();
    }

    /// <summary>An observer that throws does not stop the other witnesses, its failure is rethrown, and it still receives later values.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ThrowingObserverDoesNotStopTheOtherWitnessesAndIsRethrown()
    {
        SerializedBroadcaster<int> broadcaster = default;
        List<int> failingValues = [];
        RecordingWitness<int> sibling = new();
        broadcaster.Add(new(new CallbackRecordingWitness<int>(value =>
        {
            failingValues.Add(value);
            if (value == First)
            {
                throw new InvalidOperationException("observer");
            }
        })));
        broadcaster.Add(new(sibling));

        var first = broadcaster.PostNext(First);
        _ = Assert.Throws<InvalidOperationException>(() => first.Flush());
        broadcaster.PostNext(Second).Flush();

        await Assert.That(failingValues.SequenceEqual([First, Second])).IsTrue();
        await Assert.That(sibling.Values.SequenceEqual([First, Second])).IsTrue();
    }

    /// <summary>A value posted to a witness that already has queued values is queued behind them.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ValuePostedBehindQueuedValuesIsDeliveredAfterThem()
    {
        RecordingWitness<int> observer = new();
        SerializedWitness<int> witness = new(observer);
        SerializedBroadcaster<int> broadcaster = default;
        broadcaster.Add(witness);

        _ = witness.Post(First);
        broadcaster.PostNext(Second).Flush();

        await Assert.That(observer.Values.SequenceEqual([First, Second])).IsTrue();
    }

    /// <summary>Adding a null witness is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task AddingANullWitnessThrows()
    {
        SerializedBroadcaster<int> broadcaster = default;

        await Assert.That(() => broadcaster.Add(null!)).Throws<ArgumentNullException>();
    }
}
