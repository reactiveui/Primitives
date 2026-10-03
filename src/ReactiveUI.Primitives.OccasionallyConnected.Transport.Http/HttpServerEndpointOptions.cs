// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Configures the portable HTTP server endpoint.</summary>
[System.Diagnostics.DebuggerDisplay("{PathBase,nq}")]
public sealed record HttpServerEndpointOptions
{
    /// <summary>The byte count in one kibibyte.</summary>
    private const int BytesPerKilobyte = 1024;

    /// <summary>The default body byte limit.</summary>
    private const int DefaultBodyByteLimit = BytesPerKilobyte * BytesPerKilobyte;

    /// <summary>The default long-poll timeout.</summary>
    private static readonly TimeSpan DefaultLongPollTimeout = TimeSpan.FromSeconds(30);

    /// <summary>Gets the borrowed server hub.</summary>
    public required IServerStreamHub Hub { get; init; }

    /// <summary>Gets the optional borrowed snapshot recovery hub.</summary>
    public IServerSnapshotRecoveryHub? SnapshotRecoveryHub { get; init; }

    /// <summary>Gets the capabilities declared by this endpoint.</summary>
    public required NegotiatedCapabilities DeclaredCapabilities { get; init; }

    /// <summary>Gets the host authorization callback used before admitting fresh and duplicate replay requests.</summary>
    public required IHttpReplayAuthorizer ReplayAuthorizer { get; init; }

    /// <summary>Gets the replay protection settings used by this endpoint.</summary>
    public HttpReplayProtectionOptions ReplayProtection { get; init; } = new();

    /// <summary>Gets the finite validation limits used for snapshot recovery request and response bodies.</summary>
    public SnapshotRecoveryLimits SnapshotRecoveryLimits { get; init; } = new();

    /// <summary>Gets the optional route path base.</summary>
    public string PathBase { get; init; } = string.Empty;

    /// <summary>Gets the relative connect route.</summary>
    public string ConnectPath { get; init; } = HttpRemoteTransportOptions.DefaultConnectPath;

    /// <summary>Gets the relative push route.</summary>
    public string PushPath { get; init; } = HttpRemoteTransportOptions.DefaultPushPath;

    /// <summary>Gets the relative long-poll subscribe route.</summary>
    public string SubscribePath { get; init; } = HttpRemoteTransportOptions.DefaultSubscribePath;

    /// <summary>Gets the relative acknowledgement route.</summary>
    public string AcknowledgePath { get; init; } = HttpRemoteTransportOptions.DefaultAcknowledgePath;

    /// <summary>Gets the relative snapshot recovery route.</summary>
    public string SnapshotRecoveryPath { get; init; } = HttpRemoteTransportOptions.DefaultSnapshotRecoveryPath;

    /// <summary>Gets the maximum concurrent non-acknowledgement requests.</summary>
    public int MaximumConcurrentRequests { get; init; } = 4;

    /// <summary>Gets the independently reserved acknowledgement request capacity.</summary>
    public int MaximumConcurrentAcknowledgements { get; init; } = 1;

    /// <summary>Gets the maximum active subscription polls.</summary>
    public int MaximumConcurrentSubscriptions { get; init; } = 4;

    /// <summary>Gets the maximum encoded request body bytes.</summary>
    public int MaximumRequestBytes { get; init; } = DefaultBodyByteLimit;

    /// <summary>Gets the maximum encoded response body bytes.</summary>
    public int MaximumResponseBytes { get; init; } = DefaultBodyByteLimit;

    /// <summary>Gets the maximum decoded payload bytes.</summary>
    public int MaximumPayloadBytes { get; init; } = DefaultBodyByteLimit;

    /// <summary>Gets the maximum metadata entries per protocol object.</summary>
    public int MaximumMetadataEntries { get; init; } = 64;

    /// <summary>Gets the maximum metadata key bytes.</summary>
    public int MaximumMetadataKeyBytes { get; init; } = 128;

    /// <summary>Gets the maximum metadata value bytes.</summary>
    public int MaximumMetadataValueBytes { get; init; } = BytesPerKilobyte;

    /// <summary>Gets the maximum operations per pushed batch or returned response batch set.</summary>
    public int MaximumBatchOperations { get; init; } = 100;

    /// <summary>Gets the maximum events in one returned receive batch.</summary>
    public int MaximumEventsPerBatch { get; init; } = 100;

    /// <summary>Gets the maximum completed operation groups in one returned receive batch.</summary>
    public int MaximumCompletedOperationsPerBatch { get; init; } = 100;

    /// <summary>Gets the maximum JSON reader depth.</summary>
    public int MaximumJsonDepth { get; init; } = 32;

    /// <summary>Gets the maximum encoded query bytes.</summary>
    public int MaximumQueryBytes { get; init; } = 4096;

    /// <summary>Gets the maximum subscribe query key count.</summary>
    public int MaximumQueryKeys { get; init; } = 7;

    /// <summary>Gets the maximum UTF-8 bytes in one protocol string.</summary>
    public int MaximumProtocolStringBytes { get; init; } = 4096;

    /// <summary>Gets the finite owned long-poll timeout.</summary>
    public TimeSpan LongPollTimeout { get; init; } = DefaultLongPollTimeout;

    /// <summary>Gets the clock used for endpoint-owned deadlines.</summary>
    public TimeProvider TimeProvider { get; init; } = TimeProvider.System;
}
