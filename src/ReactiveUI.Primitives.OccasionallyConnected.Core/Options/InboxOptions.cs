// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures durable incoming capacity; implementation defaults are 10,000 events and 64 MiB.</summary>
[DebuggerDisplay("Events={MaxEvents,nq}; Bytes={MaxBytes,nq}")]
public sealed record InboxOptions
{
    /// <summary>The default number of durable events.</summary>
    private const int DefaultMaxEvents = 10_000;

    /// <summary>The number of bytes in one mebibyte.</summary>
    private const int BytesPerMebibyte = 1024 * 1024;

    /// <summary>The default encoded byte capacity for durable events.</summary>
    private const long DefaultMaxBytes = 64L * BytesPerMebibyte;

    /// <summary>Gets the maximum number of durable events.</summary>
    public int MaxEvents { get; init; } = DefaultMaxEvents;

    /// <summary>Gets the maximum encoded bytes held by durable events.</summary>
    public long MaxBytes { get; init; } = DefaultMaxBytes;

    /// <summary>Validates the durable incoming capacity limits.</summary>
    /// <exception cref="ArgumentOutOfRangeException">An inbox capacity limit is not positive.</exception>
    public void Validate()
    {
        var exception = CreateValidationException();

        if (exception is null)
        {
            return;
        }

        throw exception;
    }

    /// <summary>Creates the validation exception for the first invalid inbox capacity limit.</summary>
    /// <returns>The validation exception when a limit is invalid; otherwise null.</returns>
    private ArgumentOutOfRangeException? CreateValidationException()
    {
        if (MaxEvents <= 0)
        {
            return new(nameof(MaxEvents), MaxEvents, "MaxEvents must be positive.");
        }

        return MaxBytes <= 0
            ? new(nameof(MaxBytes), MaxBytes, "MaxBytes must be positive.")
            : null;
    }
}
