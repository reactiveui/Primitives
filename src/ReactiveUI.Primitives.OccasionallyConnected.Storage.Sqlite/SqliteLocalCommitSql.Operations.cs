// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Executes local SQLite commit SQL.</summary>
/// <content>Materializes selected immutable outbox operations.</content>
internal static partial class SqliteLocalCommitSql
{
    /// <summary>Reads one pending operation row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="streamId">The stream id.</param>
    /// <param name="reader">The row reader.</param>
    /// <param name="maximumPayloadBytes">The maximum payload bytes this adapter can materialize.</param>
    /// <param name="selectedMetadata">The preloaded selected metadata, or null for a point lookup.</param>
    /// <returns>The pending operation.</returns>
    /// <exception cref="InvalidOperationException">Stored SQLite data is invalid.</exception>
    internal static SyncOperation ReadPendingOperation(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        string storeIdentity,
        StreamId streamId,
        SqliteRows reader,
        long maximumPayloadBytes,
        Dictionary<OperationId, Dictionary<string, string>>? selectedMetadata = null)
    {
        const int OperationIdIndex = 0;
        const int ClientSequenceIndex = 1;
        const int TimestampIndex = 2;
        const int BaseVersionIndex = 3;
        const int TypeIndex = 4;
        const int PayloadContractIndex = 5;
        const int PayloadSchemaIndex = 6;
        const int PayloadContentTypeIndex = 7;
        const int PayloadIndex = 8;
        const int PayloadHashIndex = 9;
        const int DeliveryIndex = 10;
        const int DurabilityIndex = 11;
        const int PriorityIndex = 12;
        const int ConflictIndex = 13;
        const int RowIdIndex = 15;
        const int EvidenceIndex = 16;
        var operationId = ReadOperationId(reader, OperationIdIndex);
        var clientSequence = ReadPositiveLong(reader, ClientSequenceIndex, InvalidOperationSequenceMessage);
        var operationType = ReadOperationType(reader, TypeIndex);
        var context = SqliteRecordContext.Outbox(operationId, streamId, clientSequence, operationType);
        var operation = new SyncOperation
        {
            OperationId = operationId,
            StreamId = streamId,
            ClientSequence = clientSequence,
            TimestampUtc = ReadDateTimeOffset(reader, TimestampIndex, "The SQLite operation timestamp is invalid."),
            BaseVersion = ReadProtectedNullableText(connection, reader, BaseVersionIndex, context, SqliteRecordContext.BaseVersionColumn, operationId),
            Type = operationType,
            Payload = ReadOperationPayload(
                connection,
                reader,
                SqlitePayloadColumns.Create(
                    PayloadContractIndex,
                    PayloadSchemaIndex,
                    PayloadContentTypeIndex,
                    PayloadIndex,
                    PayloadHashIndex,
                    new(RowIdIndex, SqliteStoreSchema.OutboxTableName, PayloadColumnName, context),
                    EvidenceIndex),
                operationId,
                maximumPayloadBytes),
            Policy = ReadPolicy(reader, DeliveryIndex, DurabilityIndex, PriorityIndex, ConflictIndex),
            Metadata = GetSelectedMetadata(connection, transaction, storeIdentity, operationId, selectedMetadata),
        };
        SqliteLocalCommitValidation.ValidateCommitInput(operation, new(streamId, operation.Payload, FormatVersion: 1, ExpectedRevision: 0));
        return operation;
    }
}
