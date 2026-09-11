// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Configures bounded payload, metadata, and replay-protection limits.</summary>
[System.Diagnostics.DebuggerDisplay("Payload={MaximumPayloadBytes,nq}; Message={MaximumMessageBytes,nq}; Decompressed={MaximumDecompressedBytes,nq}")]
public sealed record SecurityOptions
{
    /// <summary>The number of bytes in one mebibyte.</summary>
    private const int BytesPerMebibyte = 1024 * 1024;

    /// <summary>The default maximum payload size.</summary>
    private const int DefaultMaximumPayloadBytes = BytesPerMebibyte;

    /// <summary>The default maximum message size.</summary>
    private const int DefaultMaximumMessageBytes = 2 * BytesPerMebibyte;

    /// <summary>The default maximum decompressed message size.</summary>
    private const int DefaultMaximumDecompressedBytes = 4 * BytesPerMebibyte;

    /// <summary>The default maximum metadata entry count.</summary>
    private const int DefaultMaximumMetadataEntries = 32;

    /// <summary>The default maximum metadata value size.</summary>
    private const int DefaultMaximumMetadataValueBytes = 4096;

    /// <summary>The default maximum JSON depth.</summary>
    private const int DefaultMaximumJsonDepth = 64;

    /// <summary>The default replay window in minutes.</summary>
    private const int DefaultReplayWindowMinutes = 5;

    /// <summary>The default nonce retention in minutes.</summary>
    private const int DefaultNonceRetentionMinutes = 10;

    /// <summary>Gets a value indicating whether authenticated encryption at rest is required.</summary>
    public bool RequireAuthenticatedEncryptionAtRest { get; init; }

    /// <summary>Gets the largest allowed serialized payload in bytes.</summary>
    public int MaximumPayloadBytes { get; init; } = DefaultMaximumPayloadBytes;

    /// <summary>Gets the largest allowed protocol message in bytes.</summary>
    public int MaximumMessageBytes { get; init; } = DefaultMaximumMessageBytes;

    /// <summary>Gets the largest allowed decompressed message in bytes.</summary>
    public int MaximumDecompressedBytes { get; init; } = DefaultMaximumDecompressedBytes;

    /// <summary>Gets the largest number of metadata entries allowed on one message.</summary>
    public int MaximumMetadataEntries { get; init; } = DefaultMaximumMetadataEntries;

    /// <summary>Gets the largest metadata value in bytes.</summary>
    public int MaximumMetadataValueBytes { get; init; } = DefaultMaximumMetadataValueBytes;

    /// <summary>Gets the largest allowed JSON nesting depth.</summary>
    public int MaximumJsonDepth { get; init; } = DefaultMaximumJsonDepth;

    /// <summary>Gets the interval in which a replayed message is rejected.</summary>
    public TimeSpan ReplayWindow { get; init; } = TimeSpan.FromMinutes(DefaultReplayWindowMinutes);

    /// <summary>Gets the period for which used nonces are retained.</summary>
    public TimeSpan NonceRetention { get; init; } = TimeSpan.FromMinutes(DefaultNonceRetentionMinutes);

    /// <summary>Validates the configured security limits.</summary>
    /// <exception cref="ArgumentOutOfRangeException">A security limit is invalid or contradictory.</exception>
    public void Validate()
    {
        ValidatePositive(MaximumPayloadBytes, nameof(MaximumPayloadBytes));
        ValidatePositive(MaximumMessageBytes, nameof(MaximumMessageBytes));
        ValidatePositive(MaximumDecompressedBytes, nameof(MaximumDecompressedBytes));
        ValidatePositive(MaximumMetadataEntries, nameof(MaximumMetadataEntries));
        ValidatePositive(MaximumMetadataValueBytes, nameof(MaximumMetadataValueBytes));
        ValidatePositive(MaximumJsonDepth, nameof(MaximumJsonDepth));
        ValidateFinitePositive(ReplayWindow, nameof(ReplayWindow));
        ValidateFinitePositive(NonceRetention, nameof(NonceRetention));
        ValidateOrderedLimits();

        static void ValidatePositive(int value, string parameterName)
        {
            if (value > 0)
            {
                return;
            }

            throw new ArgumentOutOfRangeException(parameterName, value, "Security limits must be positive.");
        }

        static void ValidateFinitePositive(TimeSpan value, string parameterName)
        {
            if (value > TimeSpan.Zero && value != TimeSpan.MaxValue)
            {
                return;
            }

            throw new ArgumentOutOfRangeException(parameterName, value, "Security intervals must be positive and finite.");
        }
    }

    /// <summary>Validates relationships between security limits.</summary>
    /// <exception cref="ArgumentOutOfRangeException">One limit exceeds the limit that contains it.</exception>
    private void ValidateOrderedLimits()
    {
        ValidateMaximumPayloadBytes();
        ValidateMaximumMessageBytes();
        ValidateNonceRetention();
    }

    /// <summary>Validates that payloads fit within messages.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The payload size exceeds the message size.</exception>
    private void ValidateMaximumPayloadBytes()
    {
        if (MaximumPayloadBytes <= MaximumMessageBytes)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(MaximumPayloadBytes), MaximumPayloadBytes, "MaximumPayloadBytes cannot exceed MaximumMessageBytes.");
    }

    /// <summary>Validates that messages fit within decompressed-message limits.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The message size exceeds the decompressed-message size.</exception>
    private void ValidateMaximumMessageBytes()
    {
        if (MaximumMessageBytes <= MaximumDecompressedBytes)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(MaximumMessageBytes), MaximumMessageBytes, "MaximumMessageBytes cannot exceed MaximumDecompressedBytes.");
    }

    /// <summary>Validates that nonce retention covers the replay window.</summary>
    /// <exception cref="ArgumentOutOfRangeException">Nonce retention does not cover the replay window.</exception>
    private void ValidateNonceRetention()
    {
        if (NonceRetention >= ReplayWindow)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(nameof(NonceRetention), NonceRetention, "NonceRetention cannot be less than ReplayWindow.");
    }
}
