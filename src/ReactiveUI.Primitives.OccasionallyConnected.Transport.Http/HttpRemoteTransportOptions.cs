// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Net.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Configures the HTTP reference transport adapter.</summary>
[System.Diagnostics.DebuggerDisplay("{BaseAddress,nq}")]
public sealed record HttpRemoteTransportOptions
{
    /// <summary>The byte count in one kibibyte.</summary>
    private const int BytesPerKilobyte = 1024;

    /// <summary>The default byte limit for request, response, and payload bodies.</summary>
    private const int DefaultBodyByteLimit = BytesPerKilobyte * BytesPerKilobyte;

    /// <summary>The default metadata entry count.</summary>
    private const int DefaultMetadataEntries = 64;

    /// <summary>The default metadata key byte limit.</summary>
    private const int DefaultMetadataKeyBytes = 128;

    /// <summary>The default metadata value byte limit.</summary>
    private const int DefaultMetadataValueBytes = BytesPerKilobyte;

    /// <summary>The default batch item limit.</summary>
    private const int DefaultBatchItemLimit = 100;

    /// <summary>The default JSON reader depth limit.</summary>
    private const int DefaultJsonDepth = 32;

    /// <summary>The default non-acknowledgement HTTP request capacity.</summary>
    private const int DefaultConcurrentRequests = 4;

    /// <summary>The default acknowledgement request capacity.</summary>
    private const int DefaultConcurrentAcknowledgements = 1;

    /// <summary>The default active subscription capacity.</summary>
    private const int DefaultConcurrentSubscriptions = 4;

    /// <summary>Gets the default relative connect endpoint.</summary>
    public static string DefaultConnectPath { get; } = "connect";

    /// <summary>Gets the default relative push endpoint.</summary>
    public static string DefaultPushPath { get; } = "push";

    /// <summary>Gets the default relative long-poll subscription endpoint.</summary>
    public static string DefaultSubscribePath { get; } = "subscribe";

    /// <summary>Gets the default relative acknowledgement endpoint.</summary>
    public static string DefaultAcknowledgePath { get; } = "ack";

    /// <summary>Gets the default relative snapshot recovery endpoint.</summary>
    public static string DefaultSnapshotRecoveryPath { get; } = "snapshot-recovery";

    /// <summary>Gets the caller-owned HTTP client.</summary>
    public required HttpClient HttpClient { get; init; }

    /// <summary>Gets the trusted base URI under which all protocol endpoints are resolved.</summary>
    public required Uri BaseAddress { get; init; }

    /// <summary>Gets the relative connect route.</summary>
    public string ConnectPath { get; init; } = DefaultConnectPath;

    /// <summary>Gets the relative push route.</summary>
    public string PushPath { get; init; } = DefaultPushPath;

    /// <summary>Gets the relative long-poll subscribe route.</summary>
    public string SubscribePath { get; init; } = DefaultSubscribePath;

    /// <summary>Gets the relative acknowledgement route.</summary>
    public string AcknowledgePath { get; init; } = DefaultAcknowledgePath;

    /// <summary>Gets the relative snapshot recovery route.</summary>
    public string SnapshotRecoveryPath { get; init; } = DefaultSnapshotRecoveryPath;

    /// <summary>Gets whether plain HTTP is permitted for loopback-only local development endpoints.</summary>
    public bool AllowInsecureLoopbackHttp { get; init; }

    /// <summary>Gets the maximum accepted response bytes before JSON decoding.</summary>
    public int MaximumResponseBytes { get; init; } = DefaultBodyByteLimit;

    /// <summary>Gets the maximum serialized request bytes allowed by the adapter.</summary>
    public int MaximumRequestBytes { get; init; } = DefaultBodyByteLimit;

    /// <summary>Gets the maximum payload bytes allowed for each payload envelope.</summary>
    public int MaximumPayloadBytes { get; init; } = DefaultBodyByteLimit;

    /// <summary>Gets the maximum metadata entries allowed on each operation or remote event.</summary>
    public int MaximumMetadataEntries { get; init; } = DefaultMetadataEntries;

    /// <summary>Gets the maximum UTF-8 bytes allowed for each metadata key.</summary>
    public int MaximumMetadataKeyBytes { get; init; } = DefaultMetadataKeyBytes;

    /// <summary>Gets the maximum UTF-8 bytes allowed for each metadata value.</summary>
    public int MaximumMetadataValueBytes { get; init; } = DefaultMetadataValueBytes;

    /// <summary>Gets the maximum operation results accepted in a push response.</summary>
    public int MaximumBatchOperations { get; init; } = DefaultBatchItemLimit;

    /// <summary>Gets the maximum remote events accepted in one receive batch.</summary>
    public int MaximumEventsPerBatch { get; init; } = DefaultBatchItemLimit;

    /// <summary>Gets the maximum completed operation groups accepted in one receive batch.</summary>
    public int MaximumCompletedOperationsPerBatch { get; init; } = DefaultBatchItemLimit;

    /// <summary>Gets the maximum JSON reader depth accepted by the protocol codec.</summary>
    public int MaximumJsonDepth { get; init; } = DefaultJsonDepth;

    /// <summary>Gets the clock used to observe HTTP retry hints.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;

    /// <summary>Gets the replay protection settings used by this adapter.</summary>
    public HttpReplayProtectionOptions ReplayProtection { get; init; } = new();

    /// <summary>Gets the finite validation limits used for snapshot recovery request and response bodies.</summary>
    public SnapshotRecoveryLimits SnapshotRecoveryLimits { get; init; } = new();

    /// <summary>Gets the maximum concurrent non-acknowledgement HTTP requests.</summary>
    public int MaximumConcurrentRequests { get; init; } = DefaultConcurrentRequests;

    /// <summary>Gets the independently reserved acknowledgement request capacity.</summary>
    public int MaximumConcurrentAcknowledgements { get; init; } = DefaultConcurrentAcknowledgements;

    /// <summary>Gets the maximum active HTTP receive subscriptions.</summary>
    public int MaximumConcurrentSubscriptions { get; init; } = DefaultConcurrentSubscriptions;

    /// <summary>Validates this option set.</summary>
    /// <exception cref="ArgumentException">An endpoint, URI, or limit is invalid.</exception>
    public void Validate()
    {
        ArgumentExceptionHelper.ThrowIfNull(HttpClient);
        ArgumentExceptionHelper.ThrowIfNull(BaseAddress);
        if (!BaseAddress.IsAbsoluteUri)
        {
            throw new ArgumentException("The HTTP transport base address must be absolute.", nameof(BaseAddress));
        }

        ValidateScheme();
        HttpRemoteTransportOptionsValidation.ValidateRelativePath(ConnectPath, nameof(ConnectPath));
        HttpRemoteTransportOptionsValidation.ValidateRelativePath(PushPath, nameof(PushPath));
        HttpRemoteTransportOptionsValidation.ValidateRelativePath(SubscribePath, nameof(SubscribePath));
        HttpRemoteTransportOptionsValidation.ValidateRelativePath(AcknowledgePath, nameof(AcknowledgePath));
        HttpRemoteTransportOptionsValidation.ValidateRelativePath(SnapshotRecoveryPath, nameof(SnapshotRecoveryPath));
        HttpRemoteTransportOptionsValidation.ValidatePositive(MaximumResponseBytes, nameof(MaximumResponseBytes));
        HttpRemoteTransportOptionsValidation.ValidatePositive(MaximumRequestBytes, nameof(MaximumRequestBytes));
        HttpRemoteTransportOptionsValidation.ValidatePositive(MaximumPayloadBytes, nameof(MaximumPayloadBytes));
        HttpRemoteTransportOptionsValidation.ValidatePositive(MaximumMetadataEntries, nameof(MaximumMetadataEntries));
        HttpRemoteTransportOptionsValidation.ValidatePositive(MaximumMetadataKeyBytes, nameof(MaximumMetadataKeyBytes));
        HttpRemoteTransportOptionsValidation.ValidatePositive(MaximumMetadataValueBytes, nameof(MaximumMetadataValueBytes));
        HttpRemoteTransportOptionsValidation.ValidatePositive(MaximumBatchOperations, nameof(MaximumBatchOperations));
        HttpRemoteTransportOptionsValidation.ValidatePositive(MaximumEventsPerBatch, nameof(MaximumEventsPerBatch));
        HttpRemoteTransportOptionsValidation.ValidatePositive(MaximumCompletedOperationsPerBatch, nameof(MaximumCompletedOperationsPerBatch));
        HttpRemoteTransportOptionsValidation.ValidatePositive(MaximumJsonDepth, nameof(MaximumJsonDepth));
        HttpRemoteTransportOptionsValidation.ValidatePositive(MaximumConcurrentRequests, nameof(MaximumConcurrentRequests));
        HttpRemoteTransportOptionsValidation.ValidatePositive(MaximumConcurrentAcknowledgements, nameof(MaximumConcurrentAcknowledgements));
        HttpRemoteTransportOptionsValidation.ValidatePositive(MaximumConcurrentSubscriptions, nameof(MaximumConcurrentSubscriptions));
        ArgumentExceptionHelper.ThrowIfNull(TimeProvider);
        ArgumentExceptionHelper.ThrowIfNull(ReplayProtection);
        ArgumentExceptionHelper.ThrowIfNull(SnapshotRecoveryLimits);
        ReplayProtection.Validate();
        SnapshotRecoveryLimits.Validate();
    }

    /// <summary>Validates the endpoint transport scheme.</summary>
    /// <exception cref="ArgumentException">The base address does not use HTTPS or an explicitly allowed loopback HTTP URI.</exception>
    private void ValidateScheme()
    {
        if (string.Equals(BaseAddress.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        if (AllowInsecureLoopbackHttp
            && string.Equals(BaseAddress.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase)
            && BaseAddress.IsLoopback)
        {
            return;
        }

        throw new ArgumentException("The HTTP transport requires HTTPS unless loopback HTTP is explicitly enabled.", nameof(BaseAddress));
    }
}
