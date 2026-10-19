// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Identifies bounded SQL projection columns used to capture raw payload evidence.</summary>
internal sealed class SqlitePayloadEvidenceColumns
{
    /// <summary>The offset between adjacent projected evidence columns.</summary>
    private const int NextColumnOffset = 1;

    /// <summary>Initializes a new instance of the <see cref="SqlitePayloadEvidenceColumns"/> class.</summary>
    private SqlitePayloadEvidenceColumns()
    {
    }

    /// <summary>Gets the contract storage type column index.</summary>
    internal int ContractStorageTypeIndex { get; private set; }

    /// <summary>Gets the contract length column index.</summary>
    internal int ContractLengthIndex { get; private set; }

    /// <summary>Gets the contract bytes column index.</summary>
    internal int ContractBytesIndex { get; private set; }

    /// <summary>Gets the schema storage type column index.</summary>
    internal int SchemaStorageTypeIndex { get; private set; }

    /// <summary>Gets the schema value column index.</summary>
    internal int SchemaValueIndex { get; private set; }

    /// <summary>Gets the content-type storage type column index.</summary>
    internal int ContentTypeStorageTypeIndex { get; private set; }

    /// <summary>Gets the content-type length column index.</summary>
    internal int ContentTypeLengthIndex { get; private set; }

    /// <summary>Gets the content-type bytes column index.</summary>
    internal int ContentTypeBytesIndex { get; private set; }

    /// <summary>Gets the payload storage type column index.</summary>
    internal int PayloadStorageTypeIndex { get; private set; }

    /// <summary>Gets the payload length column index.</summary>
    internal int PayloadLengthIndex { get; private set; }

    /// <summary>Gets the payload prefix column index.</summary>
    internal int PayloadPrefixIndex { get; private set; }

    /// <summary>Gets the hash storage type column index.</summary>
    internal int HashStorageTypeIndex { get; private set; }

    /// <summary>Gets the hash length column index.</summary>
    internal int HashLengthIndex { get; private set; }

    /// <summary>Gets the hash bytes column index.</summary>
    internal int HashBytesIndex { get; private set; }

    /// <summary>Creates a contiguous evidence column map starting at the supplied index.</summary>
    /// <param name="startIndex">The first evidence projection column index.</param>
    /// <returns>The column map.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="startIndex"/> is negative.</exception>
    internal static SqlitePayloadEvidenceColumns StartingAt(int startIndex)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegative(startIndex);
        var contractStorageTypeIndex = startIndex;
        var contractLengthIndex = contractStorageTypeIndex + NextColumnOffset;
        var contractBytesIndex = contractLengthIndex + NextColumnOffset;
        var schemaStorageTypeIndex = contractBytesIndex + NextColumnOffset;
        var schemaValueIndex = schemaStorageTypeIndex + NextColumnOffset;
        var contentTypeStorageTypeIndex = schemaValueIndex + NextColumnOffset;
        var contentTypeLengthIndex = contentTypeStorageTypeIndex + NextColumnOffset;
        var contentTypeBytesIndex = contentTypeLengthIndex + NextColumnOffset;
        var payloadStorageTypeIndex = contentTypeBytesIndex + NextColumnOffset;
        var payloadLengthIndex = payloadStorageTypeIndex + NextColumnOffset;
        var payloadPrefixIndex = payloadLengthIndex + NextColumnOffset;
        var hashStorageTypeIndex = payloadPrefixIndex + NextColumnOffset;
        var hashLengthIndex = hashStorageTypeIndex + NextColumnOffset;
        return new()
        {
            ContractStorageTypeIndex = contractStorageTypeIndex,
            ContractLengthIndex = contractLengthIndex,
            ContractBytesIndex = contractBytesIndex,
            SchemaStorageTypeIndex = schemaStorageTypeIndex,
            SchemaValueIndex = schemaValueIndex,
            ContentTypeStorageTypeIndex = contentTypeStorageTypeIndex,
            ContentTypeLengthIndex = contentTypeLengthIndex,
            ContentTypeBytesIndex = contentTypeBytesIndex,
            PayloadStorageTypeIndex = payloadStorageTypeIndex,
            PayloadLengthIndex = payloadLengthIndex,
            PayloadPrefixIndex = payloadPrefixIndex,
            HashStorageTypeIndex = hashStorageTypeIndex,
            HashLengthIndex = hashLengthIndex,
            HashBytesIndex = hashLengthIndex + NextColumnOffset,
        };
    }
}
