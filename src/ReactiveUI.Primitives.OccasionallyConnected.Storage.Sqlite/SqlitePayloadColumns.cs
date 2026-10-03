// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Identifies payload columns and bounded SQL evidence projection columns.</summary>
internal sealed class SqlitePayloadColumns
{
    /// <summary>Initializes a new instance of the <see cref="SqlitePayloadColumns"/> class.</summary>
    /// <param name="contractIndex">The payload contract column index.</param>
    /// <param name="schemaIndex">The payload schema column index.</param>
    /// <param name="contentTypeIndex">The payload content-type column index.</param>
    /// <param name="payloadIndex">The payload bytes column index.</param>
    /// <param name="hashIndex">The payload hash column index.</param>
    /// <param name="source">The payload storage source.</param>
    /// <param name="evidence">The bounded evidence projection columns.</param>
    private SqlitePayloadColumns(
        int contractIndex,
        int schemaIndex,
        int contentTypeIndex,
        int payloadIndex,
        int hashIndex,
        SqlitePayloadStorageSource source,
        SqlitePayloadEvidenceColumns evidence)
    {
        ContractIndex = contractIndex;
        SchemaIndex = schemaIndex;
        ContentTypeIndex = contentTypeIndex;
        PayloadIndex = payloadIndex;
        HashIndex = hashIndex;
        Source = source;
        Evidence = evidence;
    }

    /// <summary>Gets the payload contract column index.</summary>
    internal int ContractIndex { get; }

    /// <summary>Gets the payload schema column index.</summary>
    internal int SchemaIndex { get; }

    /// <summary>Gets the payload content-type column index.</summary>
    internal int ContentTypeIndex { get; }

    /// <summary>Gets the payload bytes column index.</summary>
    internal int PayloadIndex { get; }

    /// <summary>Gets the payload hash column index.</summary>
    internal int HashIndex { get; }

    /// <summary>Gets the payload storage source.</summary>
    internal SqlitePayloadStorageSource Source { get; }

    /// <summary>Gets the bounded evidence projection columns.</summary>
    internal SqlitePayloadEvidenceColumns Evidence { get; }

    /// <summary>Creates a column map with contiguous bounded evidence projections.</summary>
    /// <param name="contractIndex">The payload contract column index.</param>
    /// <param name="schemaIndex">The payload schema column index.</param>
    /// <param name="contentTypeIndex">The payload content-type column index.</param>
    /// <param name="payloadIndex">The payload bytes column index.</param>
    /// <param name="hashIndex">The payload hash column index.</param>
    /// <param name="source">The payload storage source.</param>
    /// <param name="evidenceStartIndex">The first bounded evidence projection column index.</param>
    /// <returns>The column map.</returns>
    internal static SqlitePayloadColumns Create(
        int contractIndex,
        int schemaIndex,
        int contentTypeIndex,
        int payloadIndex,
        int hashIndex,
        SqlitePayloadStorageSource source,
        int evidenceStartIndex) =>
        new(
            contractIndex,
            schemaIndex,
            contentTypeIndex,
            payloadIndex,
            hashIndex,
            source,
            SqlitePayloadEvidenceColumns.StartingAt(evidenceStartIndex));
}
