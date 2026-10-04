// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Defines immutable HTTP protocol codec limits independent of transport client state.</summary>
internal sealed record HttpProtocolLimits
{
    /// <summary>The default maximum protocol string byte count.</summary>
    private const int DefaultMaximumProtocolStringBytes = 4096;

    /// <summary>Gets the maximum encoded request body bytes.</summary>
    public required int MaximumRequestBytes { get; init; }

    /// <summary>Gets the maximum encoded response body bytes.</summary>
    public required int MaximumResponseBytes { get; init; }

    /// <summary>Gets the maximum decoded payload bytes.</summary>
    public required int MaximumPayloadBytes { get; init; }

    /// <summary>Gets the maximum metadata entries.</summary>
    public required int MaximumMetadataEntries { get; init; }

    /// <summary>Gets the maximum metadata key bytes.</summary>
    public required int MaximumMetadataKeyBytes { get; init; }

    /// <summary>Gets the maximum metadata value bytes.</summary>
    public required int MaximumMetadataValueBytes { get; init; }

    /// <summary>Gets the maximum operations or response batch groups.</summary>
    public required int MaximumBatchOperations { get; init; }

    /// <summary>Gets the maximum events per receive batch.</summary>
    public required int MaximumEventsPerBatch { get; init; }

    /// <summary>Gets the maximum completed operation groups per receive batch.</summary>
    public required int MaximumCompletedOperationsPerBatch { get; init; }

    /// <summary>Gets the maximum JSON depth.</summary>
    public required int MaximumJsonDepth { get; init; }

    /// <summary>Gets the maximum encoded query bytes.</summary>
    public int MaximumQueryBytes { get; init; }

    /// <summary>Gets the maximum query key count.</summary>
    public int MaximumQueryKeys { get; init; } = 7;

    /// <summary>Gets the maximum protocol string bytes.</summary>
    public int MaximumProtocolStringBytes { get; init; }

    /// <summary>Creates limits from adapter options.</summary>
    /// <param name="options">The adapter options.</param>
    /// <returns>The protocol limits.</returns>
    internal static HttpProtocolLimits FromOptions(HttpRemoteTransportOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        return new()
        {
            MaximumRequestBytes = options.MaximumRequestBytes,
            MaximumResponseBytes = options.MaximumResponseBytes,
            MaximumPayloadBytes = options.MaximumPayloadBytes,
            MaximumMetadataEntries = options.MaximumMetadataEntries,
            MaximumMetadataKeyBytes = options.MaximumMetadataKeyBytes,
            MaximumMetadataValueBytes = options.MaximumMetadataValueBytes,
            MaximumBatchOperations = options.MaximumBatchOperations,
            MaximumEventsPerBatch = options.MaximumEventsPerBatch,
            MaximumCompletedOperationsPerBatch = options.MaximumCompletedOperationsPerBatch,
            MaximumJsonDepth = options.MaximumJsonDepth,
        };
    }

    /// <summary>Validates and completes derived limit values.</summary>
    /// <returns>The completed limits.</returns>
    internal HttpProtocolLimits Complete()
    {
        ValidatePositive(MaximumRequestBytes, nameof(MaximumRequestBytes));
        ValidatePositive(MaximumResponseBytes, nameof(MaximumResponseBytes));
        ValidatePositive(MaximumPayloadBytes, nameof(MaximumPayloadBytes));
        ValidatePositive(MaximumMetadataEntries, nameof(MaximumMetadataEntries));
        ValidatePositive(MaximumMetadataKeyBytes, nameof(MaximumMetadataKeyBytes));
        ValidatePositive(MaximumMetadataValueBytes, nameof(MaximumMetadataValueBytes));
        ValidatePositive(MaximumBatchOperations, nameof(MaximumBatchOperations));
        ValidatePositive(MaximumEventsPerBatch, nameof(MaximumEventsPerBatch));
        ValidatePositive(MaximumCompletedOperationsPerBatch, nameof(MaximumCompletedOperationsPerBatch));
        ValidatePositive(MaximumJsonDepth, nameof(MaximumJsonDepth));
        ValidatePositive(MaximumQueryKeys, nameof(MaximumQueryKeys));

        return this with
        {
            MaximumQueryBytes = MaximumQueryBytes > 0 ? MaximumQueryBytes : MaximumRequestBytes,
            MaximumProtocolStringBytes = MaximumProtocolStringBytes > 0
                ? MaximumProtocolStringBytes
                : Math.Min(Math.Max(MaximumRequestBytes, MaximumResponseBytes), DefaultMaximumProtocolStringBytes),
        };
    }

    /// <summary>Validates a positive limit.</summary>
    /// <param name="value">The limit value.</param>
    /// <param name="name">The limit name.</param>
    /// <exception cref="ArgumentOutOfRangeException">The value is not positive.</exception>
    private static void ValidatePositive(int value, string name)
    {
        if (value > 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(name, value, "HTTP protocol limits must be positive.");
    }
}
