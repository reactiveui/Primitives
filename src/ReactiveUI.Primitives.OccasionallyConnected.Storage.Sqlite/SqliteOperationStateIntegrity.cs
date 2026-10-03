// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Globalization;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Authenticates every durable operation state row in a protected store.</summary>
internal static partial class SqliteOperationStateIntegrity
{
    /// <summary>The metadata key for the authenticated set of operation states.</summary>
    internal const string ManifestKey = "rxui.localstore.operation_state_manifest";

    /// <summary>The proof column context.</summary>
    private const string ProofColumn = "operation_state_proof";

    /// <summary>The manifest encryption context.</summary>
    private const string ManifestColumn = "operation_state_manifest";

    /// <summary>The number of authenticated state columns.</summary>
    private const int StateColumnCount = 12;

    /// <summary>The proof column index.</summary>
    private const int ProofColumnIndex = StateColumnCount;

    /// <summary>The proof column index in the manifest query.</summary>
    private const int ManifestProofColumnIndex = 2;

    /// <summary>The maximum number of replacement proofs retained at once.</summary>
    private const int ProofBatchSize = 128;

    /// <summary>Maximum stored characters or bytes in one operation state field.</summary>
    private const int MaximumStateFieldLength = 64 * 1024;

    /// <summary>Maximum encrypted proof size, allowing twelve bounded UTF-8 fields and envelope overhead.</summary>
    private const int MaximumProofLength = 4 * 1024 * 1024;

    /// <summary>The real SQLite storage class tag in a canonical proof.</summary>
    private const byte RealStorageClass = 2;

    /// <summary>The text SQLite storage class tag in a canonical proof.</summary>
    private const byte TextStorageClass = 3;

    /// <summary>The BLOB SQLite storage class tag in a canonical proof.</summary>
    private const byte BlobStorageClass = 4;

    /// <summary>SQLite exposes bounded lengths and raw text bytes without decoding stored text.</summary>
    private const string StateLengthColumns = """
        length(CAST(state.store_identity AS BLOB)) AS state_length_0, CAST(state.store_identity AS BLOB) AS state_bytes_0,
        length(CAST(state.operation_id AS BLOB)) AS state_length_1, CAST(state.operation_id AS BLOB) AS state_bytes_1,
        length(CAST(state.operation_state AS BLOB)) AS state_length_2,
        length(CAST(state.attempt_count AS BLOB)) AS state_length_3,
        length(CAST(state.changed_at_utc AS BLOB)) AS state_length_4, CAST(state.changed_at_utc AS BLOB) AS state_bytes_4,
        length(CAST(state.reason_code AS BLOB)) AS state_length_5, CAST(state.reason_code AS BLOB) AS state_bytes_5,
        length(CAST(state.retry_started_utc AS BLOB)) AS state_length_6, CAST(state.retry_started_utc AS BLOB) AS state_bytes_6,
        length(CAST(state.retry_due_utc AS BLOB)) AS state_length_7, CAST(state.retry_due_utc AS BLOB) AS state_bytes_7,
        length(CAST(state.retry_previous_delay_ticks AS BLOB)) AS state_length_8,
        length(CAST(state.retry_transient_attempt_count AS BLOB)) AS state_length_9,
        length(CAST(state.retry_authentication_state AS BLOB)) AS state_length_10,
        length(CAST(state.retry_credentials_version AS BLOB)) AS state_length_11,
        CAST(state.retry_credentials_version AS BLOB) AS state_bytes_11
        """;

    /// <summary>The query used to inspect all state rows.</summary>
    private const string SelectRows = """
        SELECT state.store_identity, state.operation_id, state.operation_state, state.attempt_count,
               state.changed_at_utc, state.reason_code, state.retry_started_utc, state.retry_due_utc,
               state.retry_previous_delay_ticks, state.retry_transient_attempt_count,
               state.retry_authentication_state, state.retry_credentials_version, proof.proof,
        """ + StateLengthColumns + """
        , length(proof.proof) AS proof_length
        FROM oc_outbox_operation_states AS state
        LEFT JOIN oc_operation_state_proofs AS proof
          ON proof.store_identity = state.store_identity AND proof.operation_id = state.operation_id
        ORDER BY state.store_identity, state.operation_id;
        """;

    /// <summary>Verifies all rows before a protected store can select state.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction, if any.</param>
    /// <exception cref="LocalStoreRecordAuthenticationException">A proof is missing or invalid.</exception>
    internal static void Verify(SqliteDatabase connection, SqliteTransaction? transaction)
    {
        var cipher = SqliteRecordCipher.For(connection);
        if (cipher is null)
        {
            return;
        }

        cipher = new(cipher.Protection, string.Empty);
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql(SelectRows);
        using var reader = command.Query();
        while (reader.Read())
        {
            if (reader.IsDBNull(ProofColumnIndex))
            {
                throw new LocalStoreRecordAuthenticationException("A persisted SQLite operation state proof is missing.");
            }

            var actual = cipher.UnprotectBytes(ReadProof(reader, ProofColumnIndex), SqliteRecordContext.KeyCheck(), ProofColumn);
            var expected = Serialize(reader);
            if (!FixedTimeEquals(actual, expected))
            {
                throw new LocalStoreRecordAuthenticationException("A persisted SQLite operation state failed authentication.");
            }
        }

        using var orphan = connection.CreateStatement();
        orphan.UseTransaction(transaction);
        orphan.SetSql("""
            SELECT COUNT(*) FROM oc_operation_state_proofs AS proof
            LEFT JOIN oc_outbox_operation_states AS state
              ON state.store_identity = proof.store_identity AND state.operation_id = proof.operation_id
            WHERE state.operation_id IS NULL;
            """);
        if (Convert.ToInt64(orphan.Scalar(), CultureInfo.InvariantCulture) != 0)
        {
            throw new LocalStoreRecordAuthenticationException("A persisted SQLite operation state proof is orphaned.");
        }

        VerifyManifest(connection, transaction, cipher);
    }

    /// <summary>Writes all proofs in the same transaction as the state changes.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    internal static void Write(SqliteDatabase connection, SqliteTransaction transaction)
    {
        var cipher = SqliteRecordCipher.For(connection);
        if (cipher is null)
        {
            return;
        }

        cipher = new(cipher.Protection, string.Empty);
        var wroteProof = false;
        var afterStoreIdentity = string.Empty;
        var afterOperationId = string.Empty;
        while (true)
        {
            var batch = ReadProofBatch(connection, transaction, cipher, afterStoreIdentity, afterOperationId);
            afterStoreIdentity = batch.LastStoreIdentity;
            afterOperationId = batch.LastOperationId;
            wroteProof |= WriteProofBatch(connection, transaction, batch.Proofs);

            if (batch.RowCount < ProofBatchSize)
            {
                break;
            }
        }

        var removed = PruneProofs(connection, transaction);
        if (wroteProof || removed > 0 || !ManifestUsesCurrentKey(connection, transaction, cipher))
        {
            WriteManifest(connection, transaction, cipher);
        }
    }

    /// <summary>Reads at most one bounded ordered batch of state rows and replacement proofs.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="cipher">The database scoped cipher.</param>
    /// <param name="afterStoreIdentity">The preceding store identity.</param>
    /// <param name="afterOperationId">The preceding operation identity.</param>
    /// <returns>The batch and its final key.</returns>
    private static (int RowCount, string LastStoreIdentity, string LastOperationId, List<(string StoreIdentity, string OperationId, byte[] Proof)> Proofs)
        ReadProofBatch(SqliteDatabase connection, SqliteTransaction transaction, SqliteRecordCipher cipher, string afterStoreIdentity, string afterOperationId)
    {
        using var read = connection.CreateStatement();
        read.UseTransaction(transaction);
        read.SetSql("""
            SELECT state.store_identity, state.operation_id, state.operation_state, state.attempt_count,
                   state.changed_at_utc, state.reason_code, state.retry_started_utc, state.retry_due_utc,
                   state.retry_previous_delay_ticks, state.retry_transient_attempt_count,
                   state.retry_authentication_state, state.retry_credentials_version, proof.proof,
            """ + StateLengthColumns + """
            , length(proof.proof) AS proof_length
            FROM oc_outbox_operation_states AS state
            LEFT JOIN oc_operation_state_proofs AS proof
              ON proof.store_identity = state.store_identity AND proof.operation_id = state.operation_id
            WHERE state.store_identity > $afterStoreIdentity
               OR (state.store_identity = $afterStoreIdentity AND state.operation_id > $afterOperationId)
            ORDER BY state.store_identity, state.operation_id
            LIMIT $batchSize;
            """);
        _ = read.Bind("$afterStoreIdentity", afterStoreIdentity);
        _ = read.Bind("$afterOperationId", afterOperationId);
        _ = read.Bind("$batchSize", ProofBatchSize);
        using var reader = read.Query();
        List<(string StoreIdentity, string OperationId, byte[] Proof)> proofs = [];
        var rowCount = 0;
        while (reader.Read())
        {
            rowCount++;
            var stateBytes = Serialize(reader);
            afterStoreIdentity = reader.GetString(0);
            afterOperationId = reader.GetString(1);
            if (ProofIsCurrent(reader, cipher, stateBytes))
            {
                continue;
            }

            proofs.Add((afterStoreIdentity, afterOperationId, cipher.ProtectBytes(stateBytes, SqliteRecordContext.KeyCheck(), ProofColumn)));
        }

        return (rowCount, afterStoreIdentity, afterOperationId, proofs);
    }

    /// <summary>Checks whether a row already has a valid proof under the current key.</summary>
    /// <param name="reader">The row reader.</param>
    /// <param name="cipher">The database scoped cipher.</param>
    /// <param name="stateBytes">The canonical state bytes.</param>
    /// <returns>Whether the proof can stay unchanged.</returns>
    private static bool ProofIsCurrent(SqliteRows reader, SqliteRecordCipher cipher, byte[] stateBytes)
    {
        if (reader.IsDBNull(ProofColumnIndex))
        {
            return false;
        }

        var stored = ReadProof(reader, ProofColumnIndex);
        var previous = cipher.UnprotectBytes(stored, SqliteRecordContext.KeyCheck(), ProofColumn);
        return FixedTimeEquals(previous, stateBytes)
            && string.Equals(SqliteRecordCipher.ReadBytesKeyId(stored), cipher.Protection.GetCurrentKeyId(), StringComparison.Ordinal);
    }

    /// <summary>Writes one bounded proof batch.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="proofs">The replacement proofs.</param>
    /// <returns>Whether any proof was written.</returns>
    private static bool WriteProofBatch(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        List<(string StoreIdentity, string OperationId, byte[] Proof)> proofs)
    {
        foreach (var proof in proofs)
        {
            using var command = connection.CreateStatement();
            command.UseTransaction(transaction);
            command.SetSql("""
                INSERT INTO oc_operation_state_proofs (store_identity, operation_id, proof)
                VALUES ($storeIdentity, $operationId, $proof)
                ON CONFLICT (store_identity, operation_id) DO UPDATE SET proof = excluded.proof;
                """);
            _ = command.Bind("$storeIdentity", proof.StoreIdentity);
            _ = command.Bind("$operationId", proof.OperationId);
            _ = command.Bind("$proof", proof.Proof);
            _ = command.Execute();
        }

        return proofs.Count != 0;
    }

    /// <summary>Removes proofs whose state rows were deleted in the same transaction.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <returns>The number of deleted proofs.</returns>
    private static int PruneProofs(SqliteDatabase connection, SqliteTransaction transaction)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            DELETE FROM oc_operation_state_proofs
            WHERE NOT EXISTS (
                SELECT 1 FROM oc_outbox_operation_states AS state
                WHERE state.store_identity = oc_operation_state_proofs.store_identity
                  AND state.operation_id = oc_operation_state_proofs.operation_id);
            """);
        return command.Execute();
    }

    /// <summary>Reports whether the operation state manifest uses the current key.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="cipher">The database scoped cipher.</param>
    /// <returns>Whether the manifest exists.</returns>
    private static bool ManifestUsesCurrentKey(SqliteDatabase connection, SqliteTransaction transaction, SqliteRecordCipher cipher)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("SELECT value FROM oc_metadata WHERE key = $key;");
        _ = command.Bind("$key", ManifestKey);
        return command.Scalar() is string stored
            && string.Equals(SqliteRecordCipher.ReadTextKeyId(stored), cipher.Protection.GetCurrentKeyId(), StringComparison.Ordinal);
    }

    /// <summary>Checks that the authenticated set of state proofs has not changed.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction, if any.</param>
    /// <param name="cipher">The database scoped cipher.</param>
    /// <exception cref="LocalStoreRecordAuthenticationException">The manifest is missing or invalid.</exception>
    private static void VerifyManifest(SqliteDatabase connection, SqliteTransaction? transaction, SqliteRecordCipher cipher)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("SELECT value FROM oc_metadata WHERE key = $key;");
        _ = command.Bind("$key", ManifestKey);
        if (command.Scalar() is not string stored)
        {
            throw new LocalStoreRecordAuthenticationException("The SQLite operation state manifest is missing.");
        }

        var actual = cipher.UnprotectText(stored, SqliteRecordContext.KeyCheck(), ManifestColumn);
        var expected = ComputeManifest(connection, transaction);
        if (!string.Equals(actual, expected, StringComparison.Ordinal))
        {
            throw new LocalStoreRecordAuthenticationException("The SQLite operation state manifest failed authentication.");
        }
    }

    /// <summary>Writes the authenticated set of state proofs after all changes.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="cipher">The database scoped cipher.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteManifest(SqliteDatabase connection, SqliteTransaction transaction, SqliteRecordCipher cipher) =>
        WriteManifest(connection, transaction, cipher, ComputeManifest(connection, transaction));

    /// <summary>Writes the supplied authenticated proof set in the current transaction.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="cipher">The database scoped cipher.</param>
    /// <param name="plaintext">The manifest plaintext.</param>
    private static void WriteManifest(SqliteDatabase connection, SqliteTransaction transaction, SqliteRecordCipher cipher, string plaintext)
    {
        var manifest = cipher.ProtectText(plaintext, SqliteRecordContext.KeyCheck(), ManifestColumn);
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            INSERT INTO oc_metadata (key, value) VALUES ($key, $value)
            ON CONFLICT (key) DO UPDATE SET value = excluded.value;
            """);
        _ = command.Bind("$key", ManifestKey);
        _ = command.Bind("$value", manifest);
        _ = command.Execute();
    }

    /// <summary>Hashes the complete proof set, including row identities.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction, if any.</param>
    /// <returns>The manifest hash.</returns>
    private static string ComputeManifest(SqliteDatabase connection, SqliteTransaction? transaction)
    {
        var digest = new byte[32];
        long count = 0;
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("SELECT store_identity, operation_id, proof, length(proof) AS proof_length FROM oc_operation_state_proofs;");
        using var reader = command.Query();
        while (reader.Read())
        {
            count++;
            XorProofDigest(digest, reader.GetString(0), reader.GetString(1), ReadProof(reader, ManifestProofColumnIndex));
        }

        return FormatManifest(count, digest);
    }

    /// <summary>Combines a proof's identity and ciphertext into a fixed-size set digest.</summary>
    /// <param name="digest">The running digest.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="operationId">The operation identity.</param>
    /// <param name="proof">The authenticated proof bytes.</param>
    private static void XorProofDigest(byte[] digest, string storeIdentity, string operationId, byte[] proof)
    {
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write("ReactiveUI.OccasionallyConnected.Sqlite.OperationStateSet.v1");
            writer.Write(storeIdentity);
            writer.Write(operationId);
            writer.Write(proof.Length);
            writer.Write(proof);
        }

#if NET8_0_OR_GREATER
        var hash = SHA256.HashData(stream.ToArray());
#else
        using var sha = SHA256.Create();
        var hash = sha.ComputeHash(stream.ToArray());
#endif
        for (var index = 0; index < digest.Length; index++)
        {
            digest[index] ^= hash[index];
        }
    }

    /// <summary>Formats the authenticated row count and set digest.</summary>
    /// <param name="count">The row count.</param>
    /// <param name="digest">The set digest.</param>
    /// <returns>The manifest plaintext.</returns>
    private static string FormatManifest(long count, byte[] digest)
    {
#if NET8_0_OR_GREATER
        return $"{count.ToString(CultureInfo.InvariantCulture)}:{Convert.ToHexString(digest)}";
#else
        return $"{count.ToString(CultureInfo.InvariantCulture)}:{BitConverter.ToString(digest).Replace("-", string.Empty)}";
#endif
    }

    /// <summary>Serializes the exact SQLite storage values and nulls in a stable format.</summary>
    /// <param name="reader">The state row reader.</param>
    /// <returns>The canonical state bytes.</returns>
    /// <exception cref="LocalStoreRecordAuthenticationException">A field has an invalid storage class or length.</exception>
    private static byte[] Serialize(SqliteRows reader)
    {
        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, leaveOpen: true))
        {
            writer.Write("ReactiveUI.OccasionallyConnected.Sqlite.OperationState.v1");
            for (var index = 0; index < StateColumnCount; index++)
            {
                if (reader.IsDBNull(index))
                {
                    writer.Write((byte)0);
                    continue;
                }

                EnsureBounded(reader.GetInt64(reader.GetOrdinal($"state_length_{index}")));
                var fieldType = reader.GetFieldType(index);
                if (fieldType == typeof(long))
                {
                    writer.Write((byte)1);
                    writer.Write(reader.GetInt64(index));
                }
                else if (fieldType == typeof(double))
                {
                    writer.Write(RealStorageClass);
                    writer.Write(reader.GetDouble(index));
                }
                else if (fieldType == typeof(string))
                {
                    writer.Write(TextStorageClass);
                    var value = reader.GetFieldValue<byte[]>(reader.GetOrdinal($"state_bytes_{index}"));
                    writer.Write(value.Length);
                    writer.Write(value);
                }
                else if (fieldType == typeof(byte[]))
                {
                    var value = reader.GetFieldValue<byte[]>(index);
                    writer.Write(BlobStorageClass);
                    writer.Write(value.Length);
                    writer.Write(value);
                }
                else
                {
                    throw new LocalStoreRecordAuthenticationException("A persisted SQLite operation state has an unsupported storage class.");
                }
            }
        }

        return stream.ToArray();
    }

    /// <summary>Reads a proof only after its SQLite BLOB length passes the allocation bound.</summary>
    /// <param name="reader">The row reader.</param>
    /// <param name="index">The proof column index.</param>
    /// <returns>The proof bytes.</returns>
    /// <exception cref="LocalStoreRecordAuthenticationException">The proof is oversized or has the wrong storage class.</exception>
    private static byte[] ReadProof(SqliteRows reader, int index)
    {
        if (reader.GetFieldType(index) != typeof(byte[])
            || reader.GetInt64(reader.GetOrdinal("proof_length")) > MaximumProofLength)
        {
            throw new LocalStoreRecordAuthenticationException("A persisted SQLite operation state proof has an invalid storage class or length.");
        }

        return reader.GetFieldValue<byte[]>(index);
    }

    /// <summary>Rejects oversized state values before they are materialized.</summary>
    /// <param name="length">The SQLite field length.</param>
    /// <exception cref="LocalStoreRecordAuthenticationException">The state field is oversized.</exception>
    private static void EnsureBounded(long length)
    {
        if (length > MaximumStateFieldLength)
        {
            throw new LocalStoreRecordAuthenticationException("A persisted SQLite operation state field exceeds the maximum length.");
        }
    }

    /// <summary>Compares authenticated bytes without a content-dependent early return.</summary>
    /// <param name="left">The first value.</param>
    /// <param name="right">The second value.</param>
    /// <returns>Whether both byte sequences are equal.</returns>
    private static bool FixedTimeEquals(byte[] left, byte[] right)
    {
#if NET8_0_OR_GREATER
        return CryptographicOperations.FixedTimeEquals(left, right);
#else
        if (left.Length != right.Length)
        {
            return false;
        }

        var difference = 0;
        for (var index = 0; index < left.Length; index++)
        {
            difference |= left[index] ^ right[index];
        }

        return difference == 0;
#endif
    }
}
