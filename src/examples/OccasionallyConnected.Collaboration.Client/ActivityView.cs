// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;

using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

/// <summary>Represents the local optimistic or server-confirmed activity view.</summary>
internal sealed record ActivityView
{
    /// <summary>Gets the pending local version marker.</summary>
    public const string PendingVersion = "local-pending";

    /// <summary>Gets the initial server version marker used before the first accepted write.</summary>
    public const string EmptyVersion = "activity-v0";

    /// <summary>Gets the activity status.</summary>
    public required string Status { get; init; }

    /// <summary>Gets the activity title.</summary>
    public string? Title { get; init; }

    /// <summary>Gets the activity details.</summary>
    public string? Details { get; init; }

    /// <summary>Gets the server-trusted accepted client identifier.</summary>
    public required string AcceptedClientId { get; init; }

    /// <summary>Gets the server-trusted accepted operation identifier.</summary>
    public required string AcceptedOperationId { get; init; }

    /// <summary>Gets the server-trusted accepted version.</summary>
    public required string AcceptedVersion { get; init; }

    /// <summary>Gets the server-trusted accepted timestamp.</summary>
    public required string ServerAcceptedUtc { get; init; }

    /// <summary>Gets a value indicating whether this view is still optimistic.</summary>
    public bool IsPending => string.Equals(AcceptedVersion, PendingVersion, StringComparison.Ordinal);

    /// <summary>Creates the empty activity view.</summary>
    /// <returns>The empty view.</returns>
    internal static ActivityView Empty() =>
        new()
        {
            Status = "empty",
            AcceptedClientId = "server",
            AcceptedOperationId = Guid.Empty.ToString("N"),
            AcceptedVersion = EmptyVersion,
            ServerAcceptedUtc = DateTimeOffset.UnixEpoch.ToString("O", CultureInfo.InvariantCulture),
        };

    /// <summary>Creates a canonical view from a remote activity update.</summary>
    /// <param name="update">The remote activity update.</param>
    /// <param name="fallback">The fallback view for metadata that was not present.</param>
    /// <returns>The canonical view.</returns>
    internal static ActivityView FromRemote(ActivityUpdate update, ActivityView fallback)
    {
        ArgumentNullException.ThrowIfNull(update);
        ArgumentNullException.ThrowIfNull(fallback);
        return new()
        {
            Status = update.Status,
            Title = update.TitleSpecified ? update.Title : fallback.Title,
            Details = update.DetailsSpecified ? update.Details : fallback.Details,
            AcceptedClientId = string.IsNullOrWhiteSpace(update.AcceptedClientId) ? fallback.AcceptedClientId : update.AcceptedClientId,
            AcceptedOperationId = string.IsNullOrWhiteSpace(update.AcceptedOperationId) ? fallback.AcceptedOperationId : update.AcceptedOperationId,
            AcceptedVersion = string.IsNullOrWhiteSpace(update.AcceptedVersion) ? fallback.AcceptedVersion : update.AcceptedVersion,
            ServerAcceptedUtc = string.IsNullOrWhiteSpace(update.ServerAcceptedUtc) ? fallback.ServerAcceptedUtc : update.ServerAcceptedUtc,
        };
    }

    /// <summary>Applies a patch to this view and records local pending metadata.</summary>
    /// <param name="update">The activity patch.</param>
    /// <param name="operation">The durable local operation.</param>
    /// <returns>The optimistic pending view.</returns>
    internal ActivityView ApplyLocal(ActivityUpdate update, SyncOperation operation)
    {
        ArgumentNullException.ThrowIfNull(update);
        ArgumentNullException.ThrowIfNull(operation);
        return this with
        {
            Status = update.Status,
            Title = update.TitleSpecified ? update.Title : Title,
            Details = update.DetailsSpecified ? update.Details : Details,
            AcceptedClientId = "local",
            AcceptedOperationId = operation.OperationId.Value.ToString("N"),
            AcceptedVersion = PendingVersion,
            ServerAcceptedUtc = operation.TimestampUtc.ToString("O", CultureInfo.InvariantCulture),
        };
    }
}
