// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

/// <summary>Converts snapshot recovery HTTP DTOs and enums.</summary>
internal sealed partial class HttpProtocolCodec
{
    /// <summary>The recovered status wire value.</summary>
    private const int SnapshotStatusRecoveredWire = 0;

    /// <summary>The unsupported projection status wire value.</summary>
    private const int SnapshotStatusUnsupportedProjectionWire = 1;

    /// <summary>The retention expired status wire value.</summary>
    private const int SnapshotStatusRetentionExpiredWire = 2;

    /// <summary>The ambiguous pending operation status wire value.</summary>
    private const int SnapshotStatusAmbiguousPendingOperationWire = 3;

    /// <summary>The validation rejected status wire value.</summary>
    private const int SnapshotStatusValidationRejectedWire = 4;

    /// <summary>The capacity exceeded status wire value.</summary>
    private const int SnapshotStatusCapacityExceededWire = 5;

    /// <summary>The retryable concurrent change status wire value.</summary>
    private const int SnapshotStatusRetryableConcurrentChangeWire = 6;

    /// <summary>The included accepted disposition wire value.</summary>
    private const int SnapshotDispositionIncludedAcceptedWire = 0;

    /// <summary>The terminal rejected disposition wire value.</summary>
    private const int SnapshotDispositionTerminalRejectedWire = 1;

    /// <summary>The unknown disposition wire value.</summary>
    private const int SnapshotDispositionUnknownWire = 2;

    /// <summary>Converts a snapshot recovery status integer.</summary>
    /// <param name="value">The encoded value.</param>
    /// <returns>The status.</returns>
    /// <exception cref="HttpRemoteTransportException"><paramref name="value"/> is not a known status.</exception>
    private static RemoteSnapshotRecoveryStatus ToSnapshotStatus(int value) => value switch
    {
        SnapshotStatusRecoveredWire => RemoteSnapshotRecoveryStatus.Recovered,
        SnapshotStatusUnsupportedProjectionWire => RemoteSnapshotRecoveryStatus.UnsupportedProjection,
        SnapshotStatusRetentionExpiredWire => RemoteSnapshotRecoveryStatus.RetentionExpired,
        SnapshotStatusAmbiguousPendingOperationWire => RemoteSnapshotRecoveryStatus.AmbiguousPendingOperation,
        SnapshotStatusValidationRejectedWire => RemoteSnapshotRecoveryStatus.ValidationRejected,
        SnapshotStatusCapacityExceededWire => RemoteSnapshotRecoveryStatus.CapacityExceeded,
        SnapshotStatusRetryableConcurrentChangeWire => RemoteSnapshotRecoveryStatus.RetryableConcurrentChange,
        _ => throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation),
    };

    /// <summary>Converts a snapshot operation disposition integer.</summary>
    /// <param name="value">The encoded value.</param>
    /// <returns>The disposition kind.</returns>
    /// <exception cref="HttpRemoteTransportException"><paramref name="value"/> is not a known disposition.</exception>
    private static SnapshotOperationDispositionKind ToSnapshotDispositionKind(int value) => value switch
    {
        SnapshotDispositionIncludedAcceptedWire => SnapshotOperationDispositionKind.IncludedAccepted,
        SnapshotDispositionTerminalRejectedWire => SnapshotOperationDispositionKind.TerminalRejected,
        SnapshotDispositionUnknownWire => SnapshotOperationDispositionKind.Unknown,
        _ => throw new HttpRemoteTransportException(HttpTransportFailureKind.ProtocolViolation),
    };

    /// <summary>Converts snapshot disposition DTOs into domain dispositions.</summary>
    /// <param name="dtos">The DTOs.</param>
    /// <returns>The dispositions.</returns>
    private static SnapshotOperationDisposition[] ToSnapshotDispositions(HttpProtocolJsonContext.SnapshotOperationDispositionWire[] dtos)
    {
        var count = dtos.Length;
        var values = new SnapshotOperationDisposition[count];
        for (var index = 0; index < count; index++)
        {
            var dto = dtos[index];
            values[index] = new()
            {
                OperationId = new(dto.OperationId),
                Kind = ToSnapshotDispositionKind(dto.Kind),
                Result = dto.Result is null ? null : HttpProtocolCodecHelper.ToOperationResult(dto.Result),
            };
        }

        return values;
    }

    /// <summary>Creates a snapshot recovery checkpoint DTO.</summary>
    /// <param name="checkpoint">The checkpoint.</param>
    /// <returns>The DTO.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private HttpProtocolJsonContext.RemoteSnapshotCheckpointWire ToSnapshotCheckpointDto(RemoteSnapshotCheckpoint checkpoint) =>
        new()
        {
            StreamId = checkpoint.StreamId.Value,
            SubscriptionId = checkpoint.SubscriptionId.Value,
            FrontierCursor = checkpoint.FrontierCursor,
            ServerVersion = checkpoint.ServerVersion,
            SnapshotFormatVersion = checkpoint.SnapshotFormatVersion,
            ClientState = ToDto(checkpoint.ClientState),
            ObservedAtUtc = checkpoint.ObservedAtUtc,
        };

    /// <summary>Creates snapshot operation disposition DTOs.</summary>
    /// <param name="dispositions">The dispositions.</param>
    /// <returns>The DTOs.</returns>
    private HttpProtocolJsonContext.SnapshotOperationDispositionWire[] ToSnapshotDispositionDtos(IReadOnlyList<SnapshotOperationDisposition> dispositions)
    {
        var count = dispositions.Count;
        var values = new HttpProtocolJsonContext.SnapshotOperationDispositionWire[count];
        for (var index = 0; index < count; index++)
        {
            var disposition = dispositions[index];
            values[index] = new() { OperationId = disposition.OperationId.Value, Kind = (int)disposition.Kind, Result = disposition.Result is null ? null : ToDto(disposition.Result) };
        }

        return values;
    }

    /// <summary>Converts a checkpoint DTO into a domain checkpoint.</summary>
    /// <param name="dto">The DTO.</param>
    /// <returns>The checkpoint.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private RemoteSnapshotCheckpoint ToSnapshotCheckpoint(HttpProtocolJsonContext.RemoteSnapshotCheckpointWire dto) =>
        new()
        {
            StreamId = new(dto.StreamId),
            SubscriptionId = new(dto.SubscriptionId),
            FrontierCursor = dto.FrontierCursor,
            ServerVersion = dto.ServerVersion,
            SnapshotFormatVersion = dto.SnapshotFormatVersion,
            ClientState = ToPayload(dto.ClientState),
            ObservedAtUtc = dto.ObservedAtUtc,
        };
}
