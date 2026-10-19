// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Describes a classified retry failure and any server or credential retry hints.</summary>
[System.Diagnostics.DebuggerDisplay("{Kind,nq}")]
public sealed record RetryFailure
{
    /// <summary>Initializes a new instance of the <see cref="RetryFailure"/> class.</summary>
    /// <param name="kind">The failure classification.</param>
    public RetryFailure(RetryFailureKind kind)
        : this(kind, null, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="RetryFailure"/> class.</summary>
    /// <param name="kind">The failure classification.</param>
    /// <param name="retryAfter">The optional server retry lower bound.</param>
    /// <param name="credentialsVersion">The optional credential version observed after token renewal.</param>
    public RetryFailure(RetryFailureKind kind, TimeSpan? retryAfter, string? credentialsVersion)
    {
        Kind = kind;
        RetryAfter = retryAfter;
        CredentialsVersion = credentialsVersion;
    }

    /// <summary>Gets the failure classification.</summary>
    public RetryFailureKind Kind { get; init; }

    /// <summary>Gets the optional server retry lower bound.</summary>
    public TimeSpan? RetryAfter { get; init; }

    /// <summary>Gets the optional credential version observed after token renewal.</summary>
    public string? CredentialsVersion { get; init; }

    /// <summary>Creates a transient failure.</summary>
    /// <returns>A transient retry failure.</returns>
    public static RetryFailure Transient() =>
        new(RetryFailureKind.Transient);

    /// <summary>Creates a transient failure with a server retry lower bound.</summary>
    /// <param name="retryAfter">The server retry lower bound.</param>
    /// <returns>A transient retry failure.</returns>
    public static RetryFailure Transient(TimeSpan retryAfter) =>
        new(RetryFailureKind.Transient, retryAfter, null);

    /// <summary>Creates an authentication failure observed after token renewal.</summary>
    /// <param name="credentialsVersion">The renewed credential version.</param>
    /// <returns>An authentication retry failure.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="credentialsVersion"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="credentialsVersion"/> is empty.</exception>
    public static RetryFailure AuthenticationTokenRenewed(string credentialsVersion)
    {
#if NETFRAMEWORK
        ArgumentExceptionHelper.ThrowIfNull(credentialsVersion);
        if (credentialsVersion.Length == 0)
        {
            throw new ArgumentException("Credentials version cannot be null or empty.", nameof(credentialsVersion));
        }
#else
        ArgumentExceptionHelper.ThrowIfNullOrEmpty(credentialsVersion);
#endif

        return new(RetryFailureKind.Authentication, null, credentialsVersion);
    }
}
