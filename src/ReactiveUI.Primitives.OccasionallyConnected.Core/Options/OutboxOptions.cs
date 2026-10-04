// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures durable outgoing capacity; implementation defaults are 10,000 operations, 64 MiB, and 1,000 blocked publishers.</summary>
[DebuggerDisplay("Operations={MaxOperations,nq}; Bytes={MaxBytes,nq}; BlockedPublishers={MaximumBlockedPublishers,nq}")]
public sealed record OutboxOptions
{
    /// <summary>The default number of durable operations.</summary>
    private const int DefaultMaxOperations = 10_000;

    /// <summary>The number of bytes in one mebibyte.</summary>
    private const int BytesPerMebibyte = 1024 * 1024;

    /// <summary>The default encoded byte capacity for durable operations.</summary>
    private const long DefaultMaxBytes = 64L * BytesPerMebibyte;

    /// <summary>The default number of publishers allowed to wait for outbox capacity.</summary>
    private const int DefaultMaximumBlockedPublishers = 1_000;

    /// <summary>Gets the maximum number of durable operations.</summary>
    public int MaxOperations { get; init; } = DefaultMaxOperations;

    /// <summary>Gets the maximum encoded bytes held by durable operations.</summary>
    public long MaxBytes { get; init; } = DefaultMaxBytes;

    /// <summary>Gets the maximum number of publishers waiting for outbox capacity.</summary>
    public int MaximumBlockedPublishers { get; init; } = DefaultMaximumBlockedPublishers;

    /// <summary>Validates the durable outgoing capacity limits.</summary>
    /// <exception cref="ArgumentOutOfRangeException">An outbox capacity limit is not positive.</exception>
    public void Validate()
    {
        var exception = CreateValidationException();

        if (exception is null)
        {
            return;
        }

        throw exception;
    }

    /// <summary>Creates the validation exception for the first invalid outbox capacity limit.</summary>
    /// <returns>The validation exception when a limit is invalid; otherwise null.</returns>
    private ArgumentOutOfRangeException? CreateValidationException()
    {
        if (MaxOperations <= 0)
        {
            return new(nameof(MaxOperations), MaxOperations, "MaxOperations must be positive.");
        }

        if (MaxBytes <= 0)
        {
            return new(nameof(MaxBytes), MaxBytes, "MaxBytes must be positive.");
        }

        return MaximumBlockedPublishers <= 0
            ? new(nameof(MaximumBlockedPublishers), MaximumBlockedPublishers, "MaximumBlockedPublishers must be positive.")
            : null;
    }
}
