// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures an occasionally connected context before it is started.</summary>
[System.Diagnostics.DebuggerDisplay("AutoStart={AutoStart}; Streams={MaxConcurrentStreams}; Priority={MinimumPriority}..{MaximumPriority}")]
public sealed record OccasionallyConnectedOptions
{
    /// <summary>Gets the shared valid default configuration.</summary>
    public static OccasionallyConnectedOptions Default { get; } = new()
    {
        Outbox = new(),
        Inbox = new(),
        Batching = new(),
        Retry = RetryOptions.Default,
        CircuitBreaker = new(),
        Retention = new(),
        Security = new(),
        Diagnostics = new(),
    };

    /// <summary>Gets a value indicating whether the context starts automatically.</summary>
    public bool AutoStart { get; init; }

    /// <summary>Gets the maximum number of streams that synchronize concurrently.</summary>
    public int MaxConcurrentStreams { get; init; } = 4;

    /// <summary>Gets the maximum conflict-resolution rounds for one operation.</summary>
    public int MaxConflictResolutionRounds { get; init; } = 3;

    /// <summary>Gets the inclusive minimum scheduling priority.</summary>
    public int MinimumPriority { get; init; } = OccasionallyConnectedOptionsValidation.MinimumPriority;

    /// <summary>Gets the inclusive maximum scheduling priority.</summary>
    public int MaximumPriority { get; init; } = OccasionallyConnectedOptionsValidation.MaximumPriority;

    /// <summary>Gets how exactly-once work behaves after its negotiated window expires.</summary>
    public ExactlyOnceExpiryBehavior ExactlyOnceExpiryBehavior { get; init; } = ExactlyOnceExpiryBehavior.StopAndReport;

    /// <summary>Gets the durable outgoing capacity configuration.</summary>
    public required OutboxOptions Outbox { get; init; }

    /// <summary>Gets the durable incoming capacity configuration.</summary>
    public required InboxOptions Inbox { get; init; }

    /// <summary>Gets the outgoing batching configuration.</summary>
    public required BatchingOptions Batching { get; init; }

    /// <summary>Gets the retry configuration.</summary>
    public required RetryOptions Retry { get; init; }

    /// <summary>Gets the endpoint circuit-breaker configuration.</summary>
    public required CircuitBreakerOptions CircuitBreaker { get; init; }

    /// <summary>Gets the durable data retention configuration.</summary>
    public required RetentionOptions Retention { get; init; }

    /// <summary>Gets the bounded security configuration.</summary>
    public required SecurityOptions Security { get; init; }

    /// <summary>Gets the diagnostic configuration.</summary>
    public required DiagnosticsOptions Diagnostics { get; init; }

    /// <summary>Validates this configuration and all nested option records.</summary>
    /// <exception cref="ArgumentNullException">A required nested configuration is missing.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A structural configuration value is invalid.</exception>
    public void Validate()
    {
        if (MaxConcurrentStreams <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxConcurrentStreams), MaxConcurrentStreams, "MaxConcurrentStreams must be positive.");
        }

        if (MaxConflictResolutionRounds <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxConflictResolutionRounds), MaxConflictResolutionRounds, "MaxConflictResolutionRounds must be positive.");
        }

        if (MinimumPriority > MaximumPriority)
        {
            throw new ArgumentOutOfRangeException(nameof(MinimumPriority), MinimumPriority, "MinimumPriority cannot exceed MaximumPriority.");
        }

        if (ExactlyOnceExpiryBehavior is not (ExactlyOnceExpiryBehavior.StopAndReport or ExactlyOnceExpiryBehavior.FallbackToAtLeastOnce))
        {
            throw new ArgumentOutOfRangeException(nameof(ExactlyOnceExpiryBehavior), ExactlyOnceExpiryBehavior, "ExactlyOnceExpiryBehavior must be a defined value.");
        }

        ValidateNestedOptions();
        ValidateBatchSize();
    }

    /// <summary>Validates required nested configuration records.</summary>
    /// <exception cref="ArgumentNullException">A required nested configuration is missing.</exception>
    private void ValidateNestedOptions()
    {
        ArgumentExceptionHelper.ThrowIfNull(Outbox);
        ArgumentExceptionHelper.ThrowIfNull(Inbox);
        ArgumentExceptionHelper.ThrowIfNull(Batching);
        ArgumentExceptionHelper.ThrowIfNull(Retry);
        ArgumentExceptionHelper.ThrowIfNull(CircuitBreaker);
        ArgumentExceptionHelper.ThrowIfNull(Retention);
        ArgumentExceptionHelper.ThrowIfNull(Security);
        ArgumentExceptionHelper.ThrowIfNull(Diagnostics);

        Outbox.Validate();
        Inbox.Validate();
        Batching.Validate();
        Retry.Validate();
        CircuitBreaker.Validate();
        Retention.Validate();
        Security.Validate();
        Diagnostics.Validate();
    }

    /// <summary>Validates that a locally constructed batch fits within a permitted message.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The configured batch exceeds the permitted message size.</exception>
    private void ValidateBatchSize()
    {
        if (Batching.MaximumBytes <= Security.MaximumMessageBytes)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(Batching), Batching.MaximumBytes, "Batching.MaximumBytes cannot exceed Security.MaximumMessageBytes.");
    }
}
