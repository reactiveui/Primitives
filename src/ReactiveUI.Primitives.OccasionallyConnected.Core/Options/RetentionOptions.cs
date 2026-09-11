// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures finite retention periods for terminal and reconstructable durable data.</summary>
[System.Diagnostics.DebuggerDisplay("Outbox={OutboxTerminalRetention}; Inbox={InboxDeduplicationRetention}; Compaction={CompactionInterval}")]
public sealed record RetentionOptions
{
    /// <summary>The implementation default terminal outbox retention in days.</summary>
    private const int DefaultOutboxTerminalRetentionDays = 1;

    /// <summary>The implementation default common retention period in days.</summary>
    private const int DefaultCommonRetentionDays = 7;

    /// <summary>The implementation default dead-letter retention period in days.</summary>
    private const int DefaultDeadLetterRetentionDays = 30;

    /// <summary>The implementation default compaction interval in hours.</summary>
    private const int DefaultCompactionIntervalHours = 1;

    /// <summary>Gets the terminal outbox record retention period.</summary>
    public TimeSpan OutboxTerminalRetention { get; init; } = TimeSpan.FromDays(DefaultOutboxTerminalRetentionDays);

    /// <summary>Gets the inbox deduplication record retention period.</summary>
    public TimeSpan InboxDeduplicationRetention { get; init; } = TimeSpan.FromDays(DefaultCommonRetentionDays);

    /// <summary>Gets the dead-letter record retention period.</summary>
    public TimeSpan DeadLetterRetention { get; init; } = TimeSpan.FromDays(DefaultDeadLetterRetentionDays);

    /// <summary>Gets the server idempotency record retention period.</summary>
    public TimeSpan ServerIdempotencyRetention { get; init; } = TimeSpan.FromDays(DefaultCommonRetentionDays);

    /// <summary>Gets the local snapshot retention period.</summary>
    public TimeSpan SnapshotRetention { get; init; } = TimeSpan.FromDays(DefaultCommonRetentionDays);

    /// <summary>Gets the interval between compaction attempts; retention never authorizes removal of data needed to rebuild the current snapshot or resolve pending conflicts.</summary>
    public TimeSpan CompactionInterval { get; init; } = TimeSpan.FromHours(DefaultCompactionIntervalHours);

    /// <summary>Validates the configured retention periods.</summary>
    /// <exception cref="ArgumentOutOfRangeException">A retention period is not positive and finite.</exception>
    public void Validate()
    {
        ValidateFinitePositive(OutboxTerminalRetention, nameof(OutboxTerminalRetention));
        ValidateFinitePositive(InboxDeduplicationRetention, nameof(InboxDeduplicationRetention));
        ValidateFinitePositive(DeadLetterRetention, nameof(DeadLetterRetention));
        ValidateFinitePositive(ServerIdempotencyRetention, nameof(ServerIdempotencyRetention));
        ValidateFinitePositive(SnapshotRetention, nameof(SnapshotRetention));
        ValidateFinitePositive(CompactionInterval, nameof(CompactionInterval));
    }

    /// <summary>Validates one finite positive interval.</summary>
    /// <param name="value">The interval to validate.</param>
    /// <param name="parameterName">The public option name.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="value"/> is not positive and finite.</exception>
    private static void ValidateFinitePositive(TimeSpan value, string parameterName)
    {
        if (value > TimeSpan.Zero && value != TimeSpan.MaxValue)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(parameterName, value, "Retention periods must be positive and finite.");
    }
}
