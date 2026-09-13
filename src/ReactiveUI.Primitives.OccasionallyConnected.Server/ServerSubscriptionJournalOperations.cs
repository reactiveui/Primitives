// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server;

/// <summary>Provides shared subscription acknowledgement validation and accounting operations.</summary>
internal static class ServerSubscriptionJournalOperations
{
    /// <summary>The logical nullable marker byte count.</summary>
    private const long NullableMarkerByteCount = 1;

    /// <summary>The logical subscription identifier byte count.</summary>
    private const long SubscriptionIdByteCount = 16;

    /// <summary>The logical timestamp byte count.</summary>
    private const long DateTimeOffsetByteCount = 16;

    /// <summary>The retained fixed bytes for one subscription row.</summary>
    private const long SubscriptionFixedBytes = SubscriptionIdByteCount + (DateTimeOffsetByteCount * 3) + (NullableMarkerByteCount * 4) + (sizeof(long) * 2);

    /// <summary>The retained fixed bytes for one offered cursor row.</summary>
    private const long OfferFixedBytes = SubscriptionIdByteCount + DateTimeOffsetByteCount + sizeof(long);

    /// <summary>Validates a trusted subscription identity.</summary>
    /// <param name="identity">The identity.</param>
    /// <exception cref="ArgumentException">The identity is invalid.</exception>
    internal static void ValidateIdentity(ServerSubscriptionIdentity identity)
    {
        ArgumentExceptionHelper.ThrowIfNull(identity);
        ServerCommitJournalGuard.ValidateStreamKey(identity.StreamKey);
        ValidateClientId(identity.ClientId);
        ValidateSubscriptionId(identity.SubscriptionId);
    }

    /// <summary>Validates a subscription page request.</summary>
    /// <param name="request">The request.</param>
    /// <exception cref="ArgumentException">The request is invalid.</exception>
    /// <exception cref="ArgumentOutOfRangeException">A request bound is invalid.</exception>
    internal static void ValidatePageRequest(ServerSubscriptionPageRequest request)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        ValidateIdentity(request.Identity);
        if (request.Cursor is not null)
        {
            ServerCommitJournalGuard.ValidateCursor(request.Cursor);
        }

        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(request.MaximumGroups);
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(request.MaximumEvents);
        ThrowIfNonPositiveLogicalBytes(request.MaximumLogicalBytes);
    }

    /// <summary>Validates an acknowledgement request.</summary>
    /// <param name="request">The request.</param>
    /// <exception cref="ArgumentException">The request is invalid.</exception>
    internal static void ValidateAcknowledgementRequest(ServerSubscriptionAcknowledgementRequest request)
    {
        ArgumentExceptionHelper.ThrowIfNull(request);
        ArgumentExceptionHelper.ThrowIfNull(request.Acknowledgement);
        ServerCommitJournalGuard.ValidateStreamKey(request.StreamKey);
        ValidateClientId(request.ClientId);
        ValidateSubscriptionId(request.Acknowledgement.SubscriptionId);
        if (request.Acknowledgement.StreamId != request.StreamKey.StreamId)
        {
            throw new ArgumentException("The acknowledgement stream does not match the authenticated stream.", nameof(request));
        }

        ServerCommitJournalGuard.ValidateCursor(request.Acknowledgement.Cursor);
    }

    /// <summary>Checks whether an identity matches a retained record.</summary>
    /// <param name="identity">The supplied identity.</param>
    /// <param name="record">The retained record.</param>
    /// <returns>Whether the identities match.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool IdentityMatches(ServerSubscriptionIdentity identity, ServerSubscriptionRecord record) =>
        IdentityMatches(identity, record.Identity);

    /// <summary>Checks whether two trusted identities match.</summary>
    /// <param name="left">The first identity.</param>
    /// <param name="right">The second identity.</param>
    /// <returns>Whether the identities match.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool IdentityMatches(ServerSubscriptionIdentity left, ServerSubscriptionIdentity right) =>
        left.SubscriptionId == right.SubscriptionId
        && string.Equals(left.ClientId, right.ClientId, StringComparison.Ordinal)
        && left.StreamKey == right.StreamKey;

    /// <summary>Creates a read-only state snapshot.</summary>
    /// <param name="record">The retained subscription record.</param>
    /// <returns>The state snapshot.</returns>
    internal static ServerSubscriptionState CreateState(ServerSubscriptionRecord record) =>
        new(
            record.Identity,
            record.LatestOfferedCursor,
            record.LatestOfferedGroupSequence,
            record.AcknowledgedCursor,
            record.AcknowledgedGroupSequence,
            record.Offers.Count);

    /// <summary>Calculates retained logical bytes for a subscription binding row.</summary>
    /// <param name="identity">The identity.</param>
    /// <param name="latestOfferedCursor">The latest retained offered cursor.</param>
    /// <param name="acknowledgedCursor">The latest retained acknowledged cursor.</param>
    /// <returns>The retained logical byte count.</returns>
    internal static long GetSubscriptionBytes(ServerSubscriptionIdentity identity, string? latestOfferedCursor = null, string? acknowledgedCursor = null)
    {
        var bytes = SubscriptionFixedBytes;
        bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, ServerCommitJournalSizer.GetStreamKeyBytes(identity.StreamKey));
        bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, ServerCommitJournalGuard.GetTextBytes(identity.ClientId));
        bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, GetOptionalCursorBytes(latestOfferedCursor));
        bytes = ServerCommitJournalSizer.AddLogicalBytes(bytes, GetOptionalCursorBytes(acknowledgedCursor));
        return bytes;
    }

    /// <summary>Calculates the retained logical byte delta for a nullable subscription cursor column.</summary>
    /// <param name="previous">The previously retained cursor.</param>
    /// <param name="current">The new retained cursor.</param>
    /// <returns>The logical byte delta.</returns>
    internal static long GetSubscriptionCursorDelta(string? previous, string? current) =>
        GetOptionalCursorBytes(current) - GetOptionalCursorBytes(previous);

    /// <summary>Calculates retained logical bytes for one offered cursor row.</summary>
    /// <param name="cursor">The cursor.</param>
    /// <returns>The retained logical byte count.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long GetOfferBytes(string cursor) =>
        ServerCommitJournalSizer.AddLogicalBytes(OfferFixedBytes, ServerCommitJournalGuard.GetTextBytes(cursor));

    /// <summary>Creates a receive-page request from a subscription page request.</summary>
    /// <param name="request">The subscription page request.</param>
    /// <returns>The receive-page request.</returns>
    internal static ServerReceivePageRequest CreateReceiveRequest(ServerSubscriptionPageRequest request) =>
        new(
            request.Identity.StreamKey,
            request.Cursor,
            request.MaximumGroups,
            request.MaximumEvents,
            request.MaximumLogicalBytes);

    /// <summary>Calculates retained bytes for an optional cursor payload.</summary>
    /// <param name="cursor">The optional cursor.</param>
    /// <returns>The cursor bytes or zero.</returns>
    private static long GetOptionalCursorBytes(string? cursor) =>
        cursor is null ? 0 : ServerCommitJournalGuard.GetTextBytes(cursor);

    /// <summary>Rejects a subscription ID that is not usable for durable binding.</summary>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <exception cref="ArgumentException">The identifier is empty.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateSubscriptionId(SubscriptionId subscriptionId)
    {
        if (subscriptionId.Value != Guid.Empty)
        {
            return;
        }

        throw new ArgumentException("The subscription identifier must be non-empty.", nameof(subscriptionId));
    }

    /// <summary>Rejects a malformed trusted client identifier.</summary>
    /// <param name="clientId">The client identifier.</param>
    /// <exception cref="ArgumentException">The client identifier is invalid.</exception>
    private static void ValidateClientId(string clientId)
    {
        ArgumentExceptionHelper.ThrowIfNull(clientId);
        if (!string.IsNullOrWhiteSpace(clientId))
        {
            _ = ServerCommitJournalGuard.GetTextBytes(clientId);
            return;
        }

        throw new ArgumentException("The authenticated client identifier cannot be empty.", nameof(clientId));
    }

    /// <summary>Rejects a non-positive logical byte budget.</summary>
    /// <param name="maximumLogicalBytes">The logical byte budget.</param>
    /// <exception cref="ArgumentOutOfRangeException">The value is not positive.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ThrowIfNonPositiveLogicalBytes(long maximumLogicalBytes) =>
        _ = maximumLogicalBytes > 0 ? true : throw new ArgumentOutOfRangeException(nameof(maximumLogicalBytes), maximumLogicalBytes, null);
}
