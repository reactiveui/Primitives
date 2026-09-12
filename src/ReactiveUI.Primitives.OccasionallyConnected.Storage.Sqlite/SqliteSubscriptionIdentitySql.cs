// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Shared SQL operations for subscription identity rows.</summary>
internal static class SqliteSubscriptionIdentitySql
{
    /// <summary>The store identity SQL parameter.</summary>
    private const string StoreIdentityParameter = "$storeIdentity";

    /// <summary>The stream identifier SQL parameter.</summary>
    private const string StreamIdParameter = "$streamId";

    /// <summary>Validates identity lookup input.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="preferredId">The preferred subscription identifier.</param>
    /// <exception cref="ArgumentException"><paramref name="streamId"/> or <paramref name="preferredId"/> is invalid.</exception>
    internal static void ValidateLookup(StreamId streamId, SubscriptionId? preferredId)
    {
        if (streamId.Value is null || streamId.Value.Length == 0)
        {
            throw new ArgumentException("StreamId must be non-empty.", nameof(streamId));
        }

        if (preferredId is not { Value: { } value } || value != Guid.Empty)
        {
            return;
        }

        throw new ArgumentException("SubscriptionId must be non-empty when supplied.", nameof(preferredId));
    }

    /// <summary>Throws when an explicit preferred identifier conflicts with storage.</summary>
    /// <param name="preferredId">The preferred identifier.</param>
    /// <param name="stored">The stored identifier.</param>
    /// <exception cref="InvalidOperationException">The identifiers differ.</exception>
    internal static void ThrowIfPreferredMismatch(SubscriptionId? preferredId, SubscriptionId stored)
    {
        if (!preferredId.HasValue || stored == preferredId.Value)
        {
            return;
        }

        throw new InvalidOperationException("The stored subscription identity does not match the requested identity.");
    }

    /// <summary>Inserts a stream identity mapping when one does not already exist.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <param name="storeIdentity">The durable store identity partition.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    internal static void InsertSubscriptionIdentityIfMissing(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId,
        SubscriptionId subscriptionId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            INSERT INTO oc_subscription_identities
                (store_identity, stream_id, subscription_id)
            VALUES
                ($storeIdentity, $streamId, $subscriptionId)
            ON CONFLICT (store_identity, stream_id) DO NOTHING;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, streamId.Value);
        _ = command.Parameters.AddWithValue("$subscriptionId", subscriptionId.Value.ToString("D"));
        _ = command.ExecuteNonQuery();
    }

    /// <summary>Selects a persisted subscription identity.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The current transaction.</param>
    /// <param name="storeIdentity">The durable store identity partition.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <returns>The persisted subscription identity.</returns>
    /// <exception cref="InvalidOperationException">The persisted identity row is missing or malformed.</exception>
    internal static SubscriptionId SelectSubscriptionIdentity(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId)
    {
        using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = """
            SELECT subscription_id FROM oc_subscription_identities
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """;
        _ = command.Parameters.AddWithValue(StoreIdentityParameter, storeIdentity);
        _ = command.Parameters.AddWithValue(StreamIdParameter, streamId.Value);
        return SqliteIdentityStoreData.ReadSubscriptionId(command.ExecuteScalar());
    }
}
