// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Tests for <see cref="CrdtLoopbackAckProbeVerifier"/>.</summary>
public sealed class CrdtLoopbackAckProbeVerifierTests
{
    /// <summary>The test client identifier.</summary>
    private const string ClientId = "device-a";

    /// <summary>The expected event count.</summary>
    private const int ExpectedEventCount = 1;

    /// <summary>The expected first counter value.</summary>
    private const int InitialCounter = 1;

    /// <summary>The expected resumed counter value.</summary>
    private const int ResumedCounter = 2;

    /// <summary>The first cursor.</summary>
    private const string FirstCursor = "cursor-1";

    /// <summary>The second cursor.</summary>
    private const string SecondCursor = "cursor-2";

    /// <summary>The previous cursor mismatch.</summary>
    private const string OtherCursor = "cursor-other";

    /// <summary>The rewind diagnostic.</summary>
    private const string RewindDiagnostic = "rewind";

    /// <summary>The first event identifier.</summary>
    private static readonly Guid FirstEventId = Guid.Parse("00000000-0000-0000-0000-000000000901");

    /// <summary>The second event identifier.</summary>
    private static readonly Guid SecondEventId = Guid.Parse("00000000-0000-0000-0000-000000000902");

    /// <summary>Verifies ACK page shape fails when counts are not the expected single operation group.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task IsExpectedAckProbePageRejectsUnexpectedCounts()
    {
        var page = CreatePage(InitialCounter, null, FirstCursor, FirstEventId, eventCount: 2, completedOperationCount: 1);

        var actual = CrdtLoopbackAckProbeVerifier.IsExpectedAckProbePage(
            page,
            InitialCounter,
            ExpectedEventCount);

        await Assert.That(actual).IsFalse();
    }

    /// <summary>Verifies ACK page shape fails when the authoritative counter value does not match.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task IsExpectedAckProbePageRejectsUnexpectedValue()
    {
        var page = CreatePage(ResumedCounter, null, FirstCursor, FirstEventId, ExpectedEventCount, ExpectedEventCount);

        var actual = CrdtLoopbackAckProbeVerifier.IsExpectedAckProbePage(
            page,
            InitialCounter,
            ExpectedEventCount);

        await Assert.That(actual).IsFalse();
    }

    /// <summary>Verifies resume proof fails when the resumed page does not start from the acknowledged cursor.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task IsExpectedResumePageRejectsPreviousCursorMismatch()
    {
        var first = CreatePage(InitialCounter, null, FirstCursor, FirstEventId, ExpectedEventCount, ExpectedEventCount);
        var second = CreatePage(ResumedCounter, OtherCursor, SecondCursor, SecondEventId, ExpectedEventCount, ExpectedEventCount);

        var actual = CrdtLoopbackAckProbeVerifier.IsExpectedResumePage(
            first,
            second,
            ResumedCounter,
            ExpectedEventCount);

        await Assert.That(actual).IsFalse();
    }

    /// <summary>Verifies resume proof fails when a previously acknowledged event id is replayed.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task IsExpectedResumePageRejectsReplayedEventId()
    {
        var first = CreatePage(InitialCounter, null, FirstCursor, FirstEventId, ExpectedEventCount, ExpectedEventCount);
        var second = CreatePage(ResumedCounter, FirstCursor, SecondCursor, FirstEventId, ExpectedEventCount, ExpectedEventCount);

        var actual = CrdtLoopbackAckProbeVerifier.IsExpectedResumePage(
            first,
            second,
            ResumedCounter,
            ExpectedEventCount);

        await Assert.That(actual).IsFalse();
    }

    /// <summary>Verifies a stale same-subscription read is detected only when the public receive path rejects rewind.</summary>
    /// <param name="throwsRewind">Whether the receive attempt throws the expected rewind diagnostic.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task IsStaleInitialReadRejectedReportsRewindOutcome(bool throwsRewind)
    {
        var actual = await CrdtLoopbackAckProbeVerifier.IsStaleInitialReadRejectedAsync(
            () => ReceiveOrThrowAsync(throwsRewind),
            RewindDiagnostic);

        await Assert.That(actual).IsEqualTo(throwsRewind);
    }

    /// <summary>Returns a page or throws the rewind diagnostic.</summary>
    /// <param name="throwsRewind">Whether to throw.</param>
    /// <returns>The receive result.</returns>
    /// <exception cref="InvalidOperationException">The requested rewind diagnostic.</exception>
    private static ValueTask<CrdtLoopbackReceivedStream> ReceiveOrThrowAsync(bool throwsRewind)
    {
        if (throwsRewind)
        {
            throw new InvalidOperationException("rewind rejected");
        }

        var page = CreatePage(InitialCounter, null, FirstCursor, FirstEventId, ExpectedEventCount, ExpectedEventCount);
        return new(page);
    }

    /// <summary>Creates a received ACK probe page.</summary>
    /// <param name="value">The counter value.</param>
    /// <param name="previousCursor">The previous cursor.</param>
    /// <param name="cursor">The next cursor.</param>
    /// <param name="eventId">The event identifier.</param>
    /// <param name="eventCount">The event count.</param>
    /// <param name="completedOperationCount">The completion count.</param>
    /// <returns>The received page.</returns>
    private static CrdtLoopbackReceivedStream CreatePage(
        int value,
        string? previousCursor,
        string cursor,
        Guid eventId,
        int eventCount,
        int completedOperationCount) =>
        new(
            [CreateCounterState(value)],
            previousCursor,
            cursor,
            [eventId],
            eventCount,
            completedOperationCount);

    /// <summary>Creates a G-counter state.</summary>
    /// <param name="value">The counter value.</param>
    /// <returns>The CRDT state.</returns>
    private static CrdtState CreateCounterState(int value) =>
        new() { Kind = CrdtKind.GCounter, GCounterComponents = new Dictionary<string, long> { [ClientId] = value } };
}
