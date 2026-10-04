// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests bounded deferred subscription anchors.</summary>
public sealed partial class InMemoryServerCommitJournalTests
{
    /// <summary>Verifies resolving a previously unknown cursor cannot exceed retained storage bounds.</summary>
    /// <returns>The asynchronous test operation.</returns>
    [Test]
    public async Task DeferredCursorAnchorRespectsRetainedByteLimit()
    {
        var calibration = new InMemoryServerCommitJournal(new() { TimeProvider = new ManualTimeProvider(Start) });
        PopulateDeferredCursorJournal(calibration);
        var byteLimit = calibration.LogicalBytes;
        var journal = new InMemoryServerCommitJournal(new() { MaximumLogicalBytes = byteLimit, TimeProvider = new ManualTimeProvider(Start) });
        PopulateDeferredCursorJournal(journal);
        var identity = SubscriptionIdentity(FirstSubscription);

        await Assert.That(() => journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes)))
            .ThrowsExactly<QueueCapacityExceededException>();
        await Assert.That(journal.LogicalBytes).IsEqualTo(byteLimit);
        await Assert.That(journal.SubscriptionOfferCount).IsEqualTo(0);
    }

    /// <summary>Registers a future cursor before its operation arrives.</summary>
    /// <param name="journal">The journal under test.</param>
    private static void PopulateDeferredCursorJournal(InMemoryServerCommitJournal journal)
    {
        _ = journal.RegisterSubscription(new ServerSubscriptionRegistrationRequest(SubscriptionIdentity(FirstSubscription), StartPosition.FromCursor(SecondCursor)));
        var operation = OperationKey(SecondOperationSeed);
        _ = journal.TryCommit(Plan(0, State(FirstVersion), Stamp(operation), Entry(operation, OperationResultKind.Accepted, SecondOperationSeed)));
    }
}
