// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests batching limits and FIFO boundary behavior.</summary>
public sealed partial class BatchSelectionPlannerTests
{
    /// <summary>Verifies a non-fitting successor flushes the prefix without including or skipping that successor.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task PlanFlushesBeforeNonFittingSuccessor()
    {
        var options = Options() with { NegotiatedMaximumBytes = Prefix + One };
        var result = BatchSelectionPlanner.Plan([new(Sequence, ThreeBytes), new(NextSequence, FourBytes)], options);

        await Assert.That(result.Kind).IsEqualTo(BatchSelectionResultKind.Ready);
        await Assert.That(result.PrefixCount).IsEqualTo(One);
        await Assert.That(result.EncodedBytes).IsEqualTo(Prefix);
    }

    /// <summary>Verifies a smaller local byte ceiling overrides the negotiated ceiling.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task PlanUsesLocalByteCeiling()
    {
        var options = Options(bytes: Prefix) with { NegotiatedMaximumBytes = Bytes };
        var result = BatchSelectionPlanner.Plan([new(Sequence, ThreeBytes), new(NextSequence, FourBytes)], options);

        await Assert.That(result.Kind).IsEqualTo(BatchSelectionResultKind.Ready);
        await Assert.That(result.PrefixCount).IsEqualTo(One);
        await Assert.That(result.EncodedBytes).IsEqualTo(Prefix);
    }

    /// <summary>Verifies a smaller local count ceiling prevents inspection of later invalid metadata.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task PlanUsesLocalCountCeiling()
    {
        var result = BatchSelectionPlanner.Plan([new(Sequence, OneByte), new(0, 0)], Options(maximum: One));

        await Assert.That(result.Kind).IsEqualTo(BatchSelectionResultKind.Ready);
        await Assert.That(result.PrefixCount).IsEqualTo(One);
        await Assert.That(result.EncodedBytes).IsEqualTo(Envelope + OneByte);
    }

    /// <summary>Verifies gaps from previously resolved operations do not invalidate FIFO order.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task PlanAcceptsIncreasingSequenceGaps()
    {
        var result = BatchSelectionPlanner.Plan([new(Sequence, OneByte), new(Sequence + Three, OneByte)], Options(elapsed: TimeSpan.MaxValue));

        await Assert.That(result.Kind).IsEqualTo(BatchSelectionResultKind.Ready);
        await Assert.That(result.PrefixCount).IsEqualTo(Two);
        await Assert.That(result.EncodedBytes).IsEqualTo(Envelope + Two);
    }

    /// <summary>Verifies no empty batch is emitted after the dwell deadline.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task PlanDoesNotEmitEmptyBatchAfterDwell()
    {
        var result = BatchSelectionPlanner.Plan([], Options(elapsed: TimeSpan.MaxValue));

        await Assert.That(result.Kind).IsEqualTo(BatchSelectionResultKind.WaitForDwell);
        await Assert.That(result.PrefixCount).IsEqualTo(0);
        await Assert.That(result.EncodedBytes).IsEqualTo(Envelope);
    }

    /// <summary>Verifies arithmetic remains exact at the largest representable batch size.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task PlanHandlesMaximumRepresentableBatch()
    {
        var result = BatchSelectionPlanner.Plan([new(Sequence, long.MaxValue - Envelope)], Options(bytes: long.MaxValue));

        await Assert.That(result.Kind).IsEqualTo(BatchSelectionResultKind.Ready);
        await Assert.That(result.PrefixCount).IsEqualTo(One);
        await Assert.That(result.EncodedBytes).IsEqualTo(long.MaxValue);
    }

    /// <summary>Verifies empty framing and a framing-only ceiling have deterministic outcomes.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task PlanHandlesEnvelopeBoundaries()
    {
        var unframed = BatchSelectionPlanner.Plan([new(Sequence, Bytes)], Options(envelope: 0));
        var framingOnly = BatchSelectionPlanner.Plan([new(Sequence, OneByte)], Options(envelope: Bytes));

        await Assert.That(unframed.Kind).IsEqualTo(BatchSelectionResultKind.Ready);
        await Assert.That(unframed.EncodedBytes).IsEqualTo(Bytes);
        await Assert.That(framingOnly.Kind).IsEqualTo(BatchSelectionResultKind.OversizedHead);
        await Assert.That(framingOnly.PrefixCount).IsEqualTo(0);
        await Assert.That(framingOnly.EncodedBytes).IsEqualTo(Bytes);
    }

    /// <summary>Verifies inspected metadata cannot carry non-positive sequences or encoded sizes.</summary>
    /// <param name="sequence">The candidate sequence.</param>
    /// <param name="bytes">The candidate encoded size.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [Arguments(0, OneByte)]
    [Arguments(-1, OneByte)]
    [Arguments(Sequence, 0)]
    [Arguments(Sequence, -1)]
    public async Task PlanRejectsInvalidCandidate(long sequence, long bytes) =>
        await Assert.That(() => BatchSelectionPlanner.Plan([new(sequence, bytes)], Options())).ThrowsExactly<ArgumentOutOfRangeException>();

    /// <summary>Verifies invalid negotiated operation ceilings are rejected before planning.</summary>
    /// <param name="maximum">The negotiated ceiling.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public async Task PlanRejectsInvalidNegotiatedCount(int maximum) =>
        await Assert.That(() => BatchSelectionPlanner.Plan([], Options() with { NegotiatedMaximumOperations = maximum })).ThrowsExactly<ArgumentOutOfRangeException>();

    /// <summary>Verifies invalid negotiated byte ceilings are rejected before planning.</summary>
    /// <param name="maximum">The negotiated ceiling.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [Arguments(0)]
    [Arguments(-1)]
    public async Task PlanRejectsInvalidNegotiatedBytes(long maximum) =>
        await Assert.That(() => BatchSelectionPlanner.Plan([], Options() with { NegotiatedMaximumBytes = maximum })).ThrowsExactly<ArgumentOutOfRangeException>();

    /// <summary>Verifies framing cannot be negative or exceed available capacity.</summary>
    /// <param name="envelope">The framing byte count.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [Arguments(-1)]
    [Arguments(Bytes + One)]
    public async Task PlanRejectsInvalidEnvelope(long envelope) =>
        await Assert.That(() => BatchSelectionPlanner.Plan([], Options(envelope: envelope))).ThrowsExactly<ArgumentOutOfRangeException>();

    /// <summary>Verifies invalid local configuration and negative elapsed time are rejected.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task PlanRejectsInvalidLocalLimitsAndElapsedTime()
    {
        await Assert.That(static () => BatchSelectionPlanner.Plan([], Options(maximum: 0))).ThrowsExactly<InvalidOperationException>();
        await Assert.That(static () => BatchSelectionPlanner.Plan([], Options(elapsed: TimeSpan.FromTicks(-1)))).ThrowsExactly<ArgumentOutOfRangeException>();
    }
}
