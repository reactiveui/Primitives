// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="InMemoryServerCommitJournal"/>.</summary>
public sealed partial class InMemoryServerCommitJournalTests
{
    /// <summary>Verifies an active page cannot be offered under a replacement subscription generation.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task ReceiveOfferRejectsReplacedBindingGeneration()
    {
        var clock = new ManualTimeProvider(Start);
        var journal = CreateSubscriptionJournal(clock, subscriptionRetention: TimeSpan.FromTicks(SingleEntryCount));
        var identity = SubscriptionIdentity(FirstSubscription);
        var original = journal.RegisterSubscription(identity);
        clock.SetUtcNow(Start.AddTicks(DoubleEntryCount));
        _ = journal.Compact();
        var replacement = journal.RegisterSubscription(identity);
        var page = journal.OfferReceivePage(new(identity, null, SingleEntryCount, DefaultMaximumEvents, DefaultMaximumLogicalBytes)
        { ExpectedGeneration = original.Generation });

        await Assert.That(page.Status).IsEqualTo(ServerReceivePageStatus.RetentionGap);
        await Assert.That(replacement.Generation).IsGreaterThan(original.Generation);
    }
}
