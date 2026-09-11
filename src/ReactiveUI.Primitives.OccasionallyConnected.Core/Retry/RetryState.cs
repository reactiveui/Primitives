// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Persisted retry state for a durable operation.</summary>
/// <param name="StartedUtc">The UTC time at which retry tracking started.</param>
/// <param name="DueUtc">The next UTC retry due time.</param>
/// <param name="PreviousDelay">The previous delay used as decorrelated jitter input.</param>
/// <param name="TransientAttemptCount">The number of transient retry attempts already scheduled.</param>
/// <param name="AuthenticationState">The authentication renewal retry state.</param>
/// <param name="CredentialsVersion">The credential version associated with the authentication retry state.</param>
[System.Diagnostics.DebuggerDisplay("Attempts = {TransientAttemptCount}, DueUtc = {DueUtc}")]
public sealed record RetryState(
    DateTimeOffset StartedUtc,
    DateTimeOffset? DueUtc,
    TimeSpan? PreviousDelay,
    int TransientAttemptCount,
    RetryAuthenticationState AuthenticationState,
    string? CredentialsVersion)
{
    /// <summary>Creates a fresh retry state for an operation.</summary>
    /// <param name="startedUtc">The UTC time at which retry tracking starts.</param>
    /// <returns>A new retry state.</returns>
    public static RetryState Start(DateTimeOffset startedUtc) =>
        new(startedUtc, null, null, 0, RetryAuthenticationState.None, null);
}
