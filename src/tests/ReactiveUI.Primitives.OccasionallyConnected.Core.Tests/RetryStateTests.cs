// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Core.Tests;

/// <summary>Tests persisted retry state initialization.</summary>
public sealed class RetryStateTests
{
    /// <summary>Verifies a newly tracked operation has no attempt or renewal history.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task FreshStatePreservesStartWithoutInventingHistory()
    {
        var started = DateTimeOffset.UnixEpoch;
        var state = RetryState.Start(started);
        await Assert.That(state.StartedUtc).IsEqualTo(started);
        await Assert.That(state.DueUtc).IsNull();
        await Assert.That(state.PreviousDelay).IsNull();
        await Assert.That(state.TransientAttemptCount).IsEqualTo(0);
        await Assert.That(state.AuthenticationState).IsEqualTo(RetryAuthenticationState.None);
        await Assert.That(state.CredentialsVersion).IsNull();
    }
}
