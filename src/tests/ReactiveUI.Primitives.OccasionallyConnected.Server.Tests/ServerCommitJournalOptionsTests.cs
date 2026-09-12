// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="ServerCommitJournalOptions"/>.</summary>
public sealed class ServerCommitJournalOptionsTests
{
    /// <summary>Verifies invalid logical byte limits are rejected.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ValidateRejectsNonPositiveLogicalBytes()
    {
        var options = new ServerCommitJournalOptions { MaximumLogicalBytes = 0 };

        await Assert.That(options.Validate).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies retention must be positive and finite.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ValidateRejectsNonPositiveAndInfiniteRetention()
    {
        var zero = new ServerCommitJournalOptions { OperationRetention = TimeSpan.Zero };
        var infinite = new ServerCommitJournalOptions { OperationRetention = TimeSpan.MaxValue };

        await Assert.That(zero.Validate).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(infinite.Validate).ThrowsExactly<ArgumentOutOfRangeException>();
    }
}
