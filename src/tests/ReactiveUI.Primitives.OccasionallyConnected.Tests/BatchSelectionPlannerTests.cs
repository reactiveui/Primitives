// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="BatchSelectionPlanner"/>.</summary>
public sealed partial class BatchSelectionPlannerTests
{
    /// <summary>The first sequence.</summary>
    private const long Sequence = 10;

    /// <summary>The next sequence.</summary>
    private const long NextSequence = 11;

    /// <summary>A single encoded byte.</summary>
    private const long OneByte = 1;

    /// <summary>The first payload contribution.</summary>
    private const long ThreeBytes = 3;

    /// <summary>The second payload contribution.</summary>
    private const long FourBytes = 4;

    /// <summary>The exact remaining payload capacity.</summary>
    private const long TwelveBytes = 12;

    /// <summary>A payload exceeding remaining capacity.</summary>
    private const long ThirteenBytes = 13;

    /// <summary>The batch capacity.</summary>
    private const long Bytes = 20;

    /// <summary>The envelope byte count.</summary>
    private const long Envelope = 8;

    /// <summary>The first prefix byte count.</summary>
    private const long Prefix = 11;

    /// <summary>The dwell duration in milliseconds.</summary>
    private const long Dwell = 50;

    /// <summary>A single operation.</summary>
    private const int One = 1;

    /// <summary>The smaller count ceiling.</summary>
    private const int Two = 2;

    /// <summary>The default count ceiling.</summary>
    private const int Three = 3;

    /// <summary>Verifies effective count flushes FIFO.</summary>
    /// <returns>A task.</returns>
    [Test]
    public async Task PlanReturnsReadyPrefixAtEffectiveCountCeiling()
    {
        var r = BatchSelectionPlanner.Plan([new(Sequence, ThreeBytes), new(NextSequence, FourBytes)], Options(Three, Two));
        await Assert.That(r.Kind).IsEqualTo(BatchSelectionResultKind.Ready);
        await Assert.That(r.PrefixCount).IsEqualTo(Two);
        await Assert.That(r.EncodedBytes).IsEqualTo(Envelope + ThreeBytes + FourBytes);
    }

    /// <summary>Verifies envelope is included in smaller byte limit.</summary>
    /// <returns>A task.</returns>
    [Test]
    public async Task PlanUsesSmallerByteCeilingIncludingEnvelope()
    {
        var r = BatchSelectionPlanner.Plan([new(Sequence, ThreeBytes), new(NextSequence, FourBytes)], Options(bytes: Prefix));
        await Assert.That(r.Kind).IsEqualTo(BatchSelectionResultKind.Ready);
        await Assert.That(r.EncodedBytes).IsEqualTo(Prefix);
        await Assert.That(r.PrefixCount).IsEqualTo(One);
    }

    /// <summary>Verifies dwell boundaries.</summary>
    /// <returns>A task.</returns>
    [Test]
    public async Task PlanUsesDwellBoundary()
    {
        var waiting = BatchSelectionPlanner.Plan([new(Sequence, ThreeBytes)], Options(elapsed: TimeSpan.FromMilliseconds(Dwell - One)));
        var ready = BatchSelectionPlanner.Plan([new(Sequence, ThreeBytes)], Options(elapsed: TimeSpan.FromMilliseconds(Dwell)));
        await Assert.That(waiting.Kind).IsEqualTo(BatchSelectionResultKind.WaitForDwell);
        await Assert.That(waiting.PrefixCount).IsEqualTo(One);
        await Assert.That(waiting.EncodedBytes).IsEqualTo(Prefix);
        await Assert.That(ready.Kind).IsEqualTo(BatchSelectionResultKind.Ready);
        await Assert.That(ready.PrefixCount).IsEqualTo(One);
        await Assert.That(ready.EncodedBytes).IsEqualTo(Prefix);
    }

    /// <summary>Verifies exact byte and oversized head outcomes.</summary>
    /// <returns>A task.</returns>
    [Test]
    public async Task PlanHandlesByteBoundaries()
    {
        var exact = BatchSelectionPlanner.Plan([new(Sequence, TwelveBytes)], Options());
        var oversized = BatchSelectionPlanner.Plan([new(Sequence, ThirteenBytes), new(NextSequence, OneByte)], Options());
        await Assert.That(exact.Kind).IsEqualTo(BatchSelectionResultKind.Ready);
        await Assert.That(exact.PrefixCount).IsEqualTo(One);
        await Assert.That(exact.EncodedBytes).IsEqualTo(Bytes);
        await Assert.That(oversized.Kind).IsEqualTo(BatchSelectionResultKind.OversizedHead);
        await Assert.That(oversized.PrefixCount).IsEqualTo(0);
        await Assert.That(oversized.EncodedBytes).IsEqualTo(Envelope);
    }

    /// <summary>Verifies empty, tail, overflow, and invalid inputs.</summary>
    /// <returns>A task.</returns>
    [Test]
    public async Task PlanHandlesBoundedAndInvalidInputs()
    {
        var empty = BatchSelectionPlanner.Plan([], Options());
        var tail = BatchSelectionPlanner.Plan([new(Sequence, OneByte), new(0, 0)], Options(One, One));
        var overflow = BatchSelectionPlanner.Plan([new(Sequence, long.MaxValue)], Options());
        await Assert.That(empty.Kind).IsEqualTo(BatchSelectionResultKind.WaitForDwell);
        await Assert.That(tail.Kind).IsEqualTo(BatchSelectionResultKind.Ready);
        await Assert.That(overflow.Kind).IsEqualTo(BatchSelectionResultKind.OversizedHead);
        await Assert.That(static () => BatchSelectionPlanner.Plan([new(Sequence, OneByte), new(Sequence, OneByte)], Options())).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(static () => BatchSelectionPlanner.Plan([new(Sequence, OneByte)], Options(envelope: Bytes + One))).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Creates planner input.</summary>
    /// <param name="maximum">The local count ceiling.</param>
    /// <param name="negotiated">The server count ceiling.</param>
    /// <param name="bytes">The byte ceiling.</param>
    /// <param name="envelope">The envelope bytes.</param>
    /// <param name="elapsed">The elapsed dwell.</param>
    /// <returns>The planner options.</returns>
    private static BatchSelectionOptions Options(int maximum = Three, int negotiated = Three, long bytes = Bytes, long envelope = Envelope, TimeSpan? elapsed = null) =>
        new()
        {
            Batching = new() { MaximumOperations = maximum, MaximumBytes = bytes, MaximumDwellTime = TimeSpan.FromMilliseconds(Dwell) },
            NegotiatedMaximumOperations = negotiated,
            NegotiatedMaximumBytes = bytes,
            EnvelopeBytes = envelope,
            FirstEligibleElapsed = elapsed ?? TimeSpan.Zero,
        };
}
