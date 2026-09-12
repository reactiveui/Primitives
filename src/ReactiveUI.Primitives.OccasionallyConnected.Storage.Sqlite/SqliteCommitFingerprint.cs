// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Identifies the complete immutable intent of a local commit independently of later snapshots.</summary>
internal static class SqliteCommitFingerprint
{
    /// <summary>UTF-8 encoding that rejects malformed UTF-16 rather than folding distinct inputs together.</summary>
    private static readonly Encoding CanonicalEncoding = new UTF8Encoding(false, true);

    /// <summary>Computes the canonical operation and snapshot mutation fingerprint.</summary>
    /// <param name="operation">The validated operation.</param>
    /// <param name="snapshot">The validated snapshot mutation.</param>
    /// <returns>The SHA-256 fingerprint.</returns>
    internal static byte[] Compute(SyncOperation operation, SnapshotMutation snapshot)
    {
        using var buffer = new MemoryStream();
        using var writer = new BinaryWriter(buffer, CanonicalEncoding, leaveOpen: true);
        WriteOperation(writer, operation);
        writer.Write(snapshot.StreamId.Value);
        writer.Write(snapshot.FormatVersion);
        writer.Write(snapshot.ExpectedRevision);
        WritePayload(writer, snapshot.State);
        WriteOptionalPayload(writer, snapshot.AuthoritativeState);
        writer.Flush();
#if NET5_0_OR_GREATER
        return SHA256.HashData(buffer.GetBuffer().AsSpan(0, (int)buffer.Length));
#else
        using var hash = SHA256.Create();
        return hash.ComputeHash(buffer.GetBuffer(), 0, (int)buffer.Length);
#endif
    }

    /// <summary>Computes the canonical operation and snapshot mutation fingerprint used before authoritative mutations existed.</summary>
    /// <param name="operation">The validated operation.</param>
    /// <param name="snapshot">The validated snapshot mutation.</param>
    /// <returns>The legacy SHA-256 fingerprint.</returns>
    internal static byte[] ComputeLegacy(SyncOperation operation, SnapshotMutation snapshot)
    {
        using var buffer = new MemoryStream();
        using var writer = new BinaryWriter(buffer, CanonicalEncoding, leaveOpen: true);
        WriteOperation(writer, operation);
        writer.Write(snapshot.StreamId.Value);
        writer.Write(snapshot.FormatVersion);
        writer.Write(snapshot.ExpectedRevision);
        WritePayload(writer, snapshot.State);
        writer.Flush();
#if NET5_0_OR_GREATER
        return SHA256.HashData(buffer.GetBuffer().AsSpan(0, (int)buffer.Length));
#else
        using var hash = SHA256.Create();
        return hash.ComputeHash(buffer.GetBuffer(), 0, (int)buffer.Length);
#endif
    }

    /// <summary>Compares persisted and requested commit fingerprints.</summary>
    /// <param name="stored">The persisted fingerprint.</param>
    /// <param name="requested">The requested fingerprint.</param>
    /// <returns>Whether the fingerprints match.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool Matches(byte[] stored, byte[] requested) =>
#if NET5_0_OR_GREATER
        CryptographicOperations.FixedTimeEquals(stored, requested);
#else
        stored.AsSpan().SequenceEqual(requested);
#endif

    /// <summary>Writes an operation using explicit field boundaries and deterministic metadata order.</summary>
    /// <param name="writer">The canonical writer.</param>
    /// <param name="operation">The validated operation.</param>
    private static void WriteOperation(BinaryWriter writer, SyncOperation operation)
    {
        writer.Write(operation.OperationId.Value.ToByteArray());
        writer.Write(operation.StreamId.Value);
        writer.Write(operation.ClientSequence);
        writer.Write(operation.TimestampUtc.UtcTicks);
        writer.Write(operation.BaseVersion is not null);
        writer.Write(operation.BaseVersion ?? string.Empty);
        writer.Write((int)operation.Type);
        WritePayload(writer, operation.Payload);
        writer.Write((int)operation.Policy.DeliveryGuarantee);
        writer.Write((int)operation.Policy.Durability);
        writer.Write(operation.Policy.Priority);
        writer.Write((int)operation.Policy.ConflictPolicy);
        writer.Write(operation.Metadata.Count);
        foreach (var key in new SortedSet<string>(operation.Metadata.Keys, StringComparer.Ordinal))
        {
            writer.Write(key);
            writer.Write(operation.Metadata[key]);
        }
    }

    /// <summary>Writes a complete payload envelope with length-prefixed bytes.</summary>
    /// <param name="writer">The canonical writer.</param>
    /// <param name="payload">The validated payload.</param>
    private static void WritePayload(BinaryWriter writer, PayloadEnvelope payload)
    {
        writer.Write(payload.ContractId);
        writer.Write(payload.SchemaVersion);
        writer.Write(payload.ContentType);
        writer.Write(payload.PayloadHash);
        writer.Write(payload.PayloadLength);
#if NET5_0_OR_GREATER
        writer.Write(payload.Payload.Span);
#else
        writer.Write(payload.Payload.ToArray());
#endif
    }

    /// <summary>Writes an optional payload with an explicit presence marker.</summary>
    /// <param name="writer">The canonical writer.</param>
    /// <param name="payload">The optional payload.</param>
    private static void WriteOptionalPayload(BinaryWriter writer, PayloadEnvelope? payload)
    {
        writer.Write(payload is not null);
        if (payload is null)
        {
            return;
        }

        WritePayload(writer, payload);
    }
}
