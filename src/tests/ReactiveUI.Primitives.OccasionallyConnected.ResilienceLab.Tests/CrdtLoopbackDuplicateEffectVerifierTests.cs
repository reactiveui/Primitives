// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.Tests;

/// <summary>Tests for <see cref="CrdtLoopbackDuplicateEffectVerifier"/>.</summary>
public sealed class CrdtLoopbackDuplicateEffectVerifierTests
{
    /// <summary>The expected duplicate effect delta.</summary>
    private const int ExpectedDuplicateEffectDelta = 0;

    /// <summary>The unexpected duplicate effect delta.</summary>
    private const int UnexpectedDuplicateEffectDelta = 1;

    /// <summary>The shared cursor.</summary>
    private const string SharedCursor = "cursor-shared";

    /// <summary>The previous cursor.</summary>
    private const string PreviousCursor = "cursor-previous";

    /// <summary>The other cursor.</summary>
    private const string OtherCursor = "cursor-other";

    /// <summary>The first event identifier.</summary>
    private static readonly Guid FirstEventId = Guid.Parse("00000000-0000-0000-0000-000000000951");

    /// <summary>The second event identifier.</summary>
    private static readonly Guid SecondEventId = Guid.Parse("00000000-0000-0000-0000-000000000952");

    /// <summary>Verifies duplicate effect succeeds only when event count, completion count, and frontier all match.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetEffectDeltaReturnsExpectedWhenDuplicateAddsNoEffectGroup()
    {
        var before = CreatePage(SharedCursor, eventCount: 2, completedOperationCount: 2);
        var after = CreatePage(SharedCursor, eventCount: 2, completedOperationCount: 2);

        var actual = CrdtLoopbackDuplicateEffectVerifier.GetEffectDelta(before, after, ExpectedDuplicateEffectDelta);

        await Assert.That(actual).IsEqualTo(ExpectedDuplicateEffectDelta);
    }

    /// <summary>Verifies an added duplicate event fails the duplicate effect proof.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetEffectDeltaRejectsUnexpectedEventDelta()
    {
        var before = CreatePage(SharedCursor, eventCount: 2, completedOperationCount: 2);
        var after = CreatePage(SharedCursor, eventCount: 3, completedOperationCount: 2);

        var actual = CrdtLoopbackDuplicateEffectVerifier.GetEffectDelta(before, after, ExpectedDuplicateEffectDelta);

        await Assert.That(actual).IsEqualTo(int.MinValue);
    }

    /// <summary>Verifies an added duplicate completion group fails the duplicate effect proof.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetEffectDeltaRejectsUnexpectedCompletedOperationDelta()
    {
        var before = CreatePage(SharedCursor, eventCount: 2, completedOperationCount: 2);
        var after = CreatePage(SharedCursor, eventCount: 2, completedOperationCount: 3);

        var actual = CrdtLoopbackDuplicateEffectVerifier.GetEffectDelta(before, after, ExpectedDuplicateEffectDelta);

        await Assert.That(actual).IsEqualTo(int.MinValue);
    }

    /// <summary>Verifies a frontier change fails the duplicate effect proof.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetEffectDeltaRejectsUnexpectedFrontierChange()
    {
        var before = CreatePage(SharedCursor, eventCount: 2, completedOperationCount: 2);
        var after = CreatePage(OtherCursor, eventCount: 2, completedOperationCount: 2);

        var actual = CrdtLoopbackDuplicateEffectVerifier.GetEffectDelta(before, after, ExpectedDuplicateEffectDelta);

        await Assert.That(actual).IsEqualTo(int.MinValue);
    }

    /// <summary>Verifies the expected delta is enforced rather than hard-coded.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task GetEffectDeltaReturnsConfiguredExpectedDelta()
    {
        var before = CreatePage(SharedCursor, eventCount: 2, completedOperationCount: 2);
        var after = CreatePage(SharedCursor, eventCount: 3, completedOperationCount: 3);

        var actual = CrdtLoopbackDuplicateEffectVerifier.GetEffectDelta(before, after, UnexpectedDuplicateEffectDelta);

        await Assert.That(actual).IsEqualTo(UnexpectedDuplicateEffectDelta);
    }

    /// <summary>Creates a received stream page.</summary>
    /// <param name="cursor">The cursor.</param>
    /// <param name="eventCount">The event count.</param>
    /// <param name="completedOperationCount">The completion count.</param>
    /// <returns>The received page.</returns>
    private static CrdtLoopbackReceivedStream CreatePage(
        string cursor,
        int eventCount,
        int completedOperationCount) =>
        new(
            [new() { Kind = CrdtKind.GCounter }],
            PreviousCursor,
            cursor,
            [FirstEventId, SecondEventId],
            eventCount,
            completedOperationCount);
}
