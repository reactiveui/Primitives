// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Core.Tests;

/// <summary>Tests retry decisions as immutable persistence handoffs.</summary>
public sealed class RetryDecisionTests
{
    /// <summary>Verifies a persisted decision keeps its due time and original state when copied.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task DecisionCopyPreservesTheOriginalPersistenceHandoff()
    {
        var state = RetryState.Start(DateTimeOffset.UnixEpoch);
        var due = state.StartedUtc.AddTicks(1);
        var next = state with { DueUtc = due, PreviousDelay = TimeSpan.FromTicks(1), TransientAttemptCount = 1 };
        var decision = new RetryDecision(RetryDecisionKind.Retry, RetryStopReason.None, next.PreviousDelay, due, next);
        var stopped = decision with { Kind = RetryDecisionKind.Stop, StopReason = RetryStopReason.AttemptsExhausted, Delay = null, DueUtc = null };
        await Assert.That(decision.Kind).IsEqualTo(RetryDecisionKind.Retry);
        await Assert.That(decision.StopReason).IsEqualTo(RetryStopReason.None);
        await Assert.That(decision.Delay).IsEqualTo(TimeSpan.FromTicks(1));
        await Assert.That(decision.DueUtc).IsEqualTo(due);
        await Assert.That(decision.NextState).IsEqualTo(next);
        await Assert.That(stopped.Kind).IsEqualTo(RetryDecisionKind.Stop);
        await Assert.That(stopped.StopReason).IsEqualTo(RetryStopReason.AttemptsExhausted);
        await Assert.That(stopped.Delay).IsNull();
        await Assert.That(stopped.DueUtc).IsNull();
        await Assert.That(stopped.NextState).IsEqualTo(next);
        await Assert.That(state.DueUtc).IsNull();
    }
}
