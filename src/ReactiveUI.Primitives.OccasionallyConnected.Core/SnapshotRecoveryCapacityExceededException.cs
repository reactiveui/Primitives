// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Reports a bounded snapshot recovery capture limit failure.</summary>
[System.Diagnostics.DebuggerDisplay("{LimitName,nq}: {Observed} > {Maximum}")]
public sealed class SnapshotRecoveryCapacityExceededException : InvalidOperationException
{
    /// <summary>Initializes a new instance of the <see cref="SnapshotRecoveryCapacityExceededException"/> class.</summary>
    public SnapshotRecoveryCapacityExceededException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SnapshotRecoveryCapacityExceededException"/> class.</summary>
    /// <param name="message">The exception message.</param>
    public SnapshotRecoveryCapacityExceededException(string message)
        : base(message ?? throw new ArgumentNullException(nameof(message)))
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SnapshotRecoveryCapacityExceededException"/> class.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    public SnapshotRecoveryCapacityExceededException(string message, Exception innerException)
        : base(message ?? throw new ArgumentNullException(nameof(message)), innerException)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SnapshotRecoveryCapacityExceededException"/> class.</summary>
    /// <param name="limitName">The exceeded limit name.</param>
    /// <param name="maximum">The configured maximum.</param>
    /// <param name="observed">The observed value.</param>
    public SnapshotRecoveryCapacityExceededException(string limitName, long maximum, long observed)
        : base($"Snapshot recovery capture exceeded {limitName}. Maximum: {maximum}; observed: {observed}.")
    {
        ArgumentExceptionHelper.ThrowIfNull(limitName);

        LimitName = limitName;
        Maximum = maximum;
        Observed = observed;
    }

    /// <summary>Gets the exceeded limit name.</summary>
    public string LimitName { get; } = string.Empty;

    /// <summary>Gets the configured maximum.</summary>
    public long Maximum { get; }

    /// <summary>Gets the observed value.</summary>
    public long Observed { get; }
}
