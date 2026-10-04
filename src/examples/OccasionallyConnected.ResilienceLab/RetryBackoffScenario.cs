// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Demonstrates deterministic bounded retry scheduling and attempt persistence.</summary>
internal static class RetryBackoffScenario
{
    /// <summary>The command-line scenario name.</summary>
    internal const string ScenarioName = "retry-backoff";

    /// <summary>The minimum retry delay used by the demonstration.</summary>
    private const int MinimumDelayMilliseconds = 100;

    /// <summary>The server retry hint used by the demonstration.</summary>
    private const int RetryHintDelayMilliseconds = 500;

    /// <summary>Runs the retry policy against a transient failure and a persisted next state.</summary>
    /// <param name="timeProvider">The time provider used by the policy.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The scenario invariants.</returns>
    internal static ValueTask<IReadOnlyList<ResilienceLabCaseResult>> RunAsync(TimeProvider timeProvider, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(timeProvider);
        var options = new RetryOptions
        {
            MinimumDelay = TimeSpan.FromMilliseconds(MinimumDelayMilliseconds),
            MaximumDelay = TimeSpan.FromSeconds(1),
            MaximumRetryAttempts = 1,
            MaximumRetryAge = TimeSpan.FromMinutes(1),
        };
        var policy = new RetryPolicy(options, timeProvider, new FixedRetryRandomSource());
        var first = policy.GetDecision(RetryFailure.Transient(TimeSpan.FromMilliseconds(RetryHintDelayMilliseconds)), RetryState.Start(timeProvider.GetUtcNow()));
        var second = policy.GetDecision(RetryFailure.Transient(), first.NextState);
        var delay = first.Delay ?? TimeSpan.Zero;

        IReadOnlyList<ResilienceLabCaseResult> cases =
        [
            new($"{ScenarioName}.server-hint-applied", TimeSpan.FromMilliseconds(RetryHintDelayMilliseconds), delay, delay == TimeSpan.FromMilliseconds(RetryHintDelayMilliseconds)),
            new($"{ScenarioName}.attempt-persisted", 1, first.NextState.TransientAttemptCount, first.NextState.TransientAttemptCount == 1),
            new($"{ScenarioName}.delay-bounded", true, delay <= options.MaximumDelay, delay <= options.MaximumDelay),
            new($"{ScenarioName}.attempts-exhausted", RetryDecisionKind.Stop, second.Kind, second.Kind == RetryDecisionKind.Stop),
        ];

        return ValueTask.FromResult(cases);
    }

    /// <summary>Supplies a deterministic zero jitter sample for the runnable demonstration.</summary>
    private sealed class FixedRetryRandomSource : IRetryRandomSource
    {
        /// <inheritdoc />
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public double NextDouble() => 0;
    }
}
