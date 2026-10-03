// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Authenticates changed operation states without scanning unchanged rows.</summary>
internal static partial class SqliteOperationStateIntegrity
{
    /// <summary>The manifest prefix plus 64 hexadecimal digits.</summary>
    private const int ManifestSuffixLength = 65;

    /// <summary>The two characters per digest byte.</summary>
    private const int HexCharactersPerByte = 2;

    /// <summary>The base of decimal manifest counts.</summary>
    private const int DecimalBase = 10;

    /// <summary>The largest decimal digit.</summary>
    private const int LargestDecimalDigit = DecimalBase - 1;

    /// <summary>The index of the journal's existed flag.</summary>
    private const int JournalExistedIndex = 13;

    /// <summary>The store identity parameter.</summary>
    private const string StoreIdentityParameter = "$storeIdentity";

    /// <summary>The operation identity parameter.</summary>
    private const string OperationIdParameter = "$operationId";

    /// <summary>Installs a connection-local journal before a protected write begins.</summary>
    /// <param name="connection">The writable connection.</param>
    internal static void InstallJournal(SqliteDatabase connection)
    {
        using var command = connection.CreateStatement();
        command.SetSql("""
            CREATE TEMP TABLE oc_state_journal (
                store_identity TEXT NOT NULL, operation_id TEXT NOT NULL,
                operation_state INTEGER NULL, attempt_count INTEGER NULL, changed_at_utc TEXT NULL,
                reason_code TEXT NULL, retry_started_utc TEXT NULL, retry_due_utc TEXT NULL,
                retry_previous_delay_ticks INTEGER NULL, retry_transient_attempt_count INTEGER NULL,
                retry_authentication_state INTEGER NULL, retry_credentials_version TEXT NULL,
                proof BLOB NULL, existed INTEGER NOT NULL,
                PRIMARY KEY (store_identity, operation_id));
            CREATE TEMP TRIGGER oc_state_insert AFTER INSERT ON main.oc_outbox_operation_states BEGIN
                INSERT OR IGNORE INTO oc_state_journal (store_identity, operation_id, existed)
                VALUES (NEW.store_identity, NEW.operation_id, 0);
            END;
            CREATE TEMP TRIGGER oc_state_update BEFORE UPDATE ON main.oc_outbox_operation_states BEGIN
                INSERT OR IGNORE INTO oc_state_journal
                SELECT OLD.store_identity, OLD.operation_id, OLD.operation_state, OLD.attempt_count,
                       OLD.changed_at_utc, OLD.reason_code, OLD.retry_started_utc, OLD.retry_due_utc,
                       OLD.retry_previous_delay_ticks, OLD.retry_transient_attempt_count,
                       OLD.retry_authentication_state, OLD.retry_credentials_version,
                       (SELECT proof FROM main.oc_operation_state_proofs
                        WHERE store_identity = OLD.store_identity AND operation_id = OLD.operation_id), 1;
            END;
            CREATE TEMP TRIGGER oc_state_delete BEFORE DELETE ON main.oc_outbox_operation_states BEGIN
                INSERT OR IGNORE INTO oc_state_journal
                SELECT OLD.store_identity, OLD.operation_id, OLD.operation_state, OLD.attempt_count,
                       OLD.changed_at_utc, OLD.reason_code, OLD.retry_started_utc, OLD.retry_due_utc,
                       OLD.retry_previous_delay_ticks, OLD.retry_transient_attempt_count,
                       OLD.retry_authentication_state, OLD.retry_credentials_version,
                       (SELECT proof FROM main.oc_operation_state_proofs
                        WHERE store_identity = OLD.store_identity AND operation_id = OLD.operation_id), 1;
            END;
            """);
        _ = command.Execute();
    }

    /// <summary>Updates proofs and the authenticated manifest for journaled state changes.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The write transaction.</param>
    internal static void WriteChanges(SqliteDatabase connection, SqliteTransaction transaction)
    {
        var cipher = SqliteRecordCipher.For(connection);
        if (cipher is null)
        {
            return;
        }

        cipher = new(cipher.Protection, string.Empty);
        var manifest = ReadManifest(connection, transaction, cipher);
        var afterStoreIdentity = string.Empty;
        var afterOperationId = string.Empty;
        while (true)
        {
            var batch = ReadJournalBatch(connection, transaction, afterStoreIdentity, afterOperationId);
            foreach (var change in batch.Changes)
            {
                ApplyJournalChange(connection, transaction, cipher, manifest, change);
            }

            afterStoreIdentity = batch.LastStoreIdentity;
            afterOperationId = batch.LastOperationId;
            if (batch.Changes.Count < ProofBatchSize)
            {
                break;
            }
        }

        if (manifest.Changed)
        {
            WriteManifest(connection, transaction, cipher, FormatManifest(manifest.Count, manifest.Digest));
        }
    }

    /// <summary>Reads and authenticates the current set accumulator.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="cipher">The database scoped cipher.</param>
    /// <returns>The authenticated accumulator.</returns>
    /// <exception cref="LocalStoreRecordAuthenticationException">The manifest is missing or invalid.</exception>
    private static JournalManifest ReadManifest(SqliteDatabase connection, SqliteTransaction transaction, SqliteRecordCipher cipher)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("SELECT value FROM oc_metadata WHERE key = $key;");
        _ = command.Bind("$key", ManifestKey);
        if (command.Scalar() is not string stored)
        {
            throw new LocalStoreRecordAuthenticationException("The SQLite operation state manifest is missing.");
        }

        var plaintext = cipher.UnprotectText(stored, SqliteRecordContext.KeyCheck(), ManifestColumn);
        var separator = plaintext.IndexOf(':');
        if (separator <= 0 || !TryReadCount(plaintext, separator, out var count)
            || plaintext.Length != separator + ManifestSuffixLength)
        {
            throw new LocalStoreRecordAuthenticationException("The SQLite operation state manifest is invalid.");
        }

        try
        {
            return new(count, ParseDigest(plaintext, separator + 1));
        }
        catch (FormatException exception)
        {
            throw new LocalStoreRecordAuthenticationException("The SQLite operation state manifest is invalid.", exception);
        }
    }

    /// <summary>Parses a 32-byte digest without framework-specific conversion APIs.</summary>
    /// <param name="hex">The manifest text.</param>
    /// <param name="offset">The digest start.</param>
    /// <returns>The digest bytes.</returns>
    private static byte[] ParseDigest(string hex, int offset)
    {
        var digest = new byte[32];
        for (var index = 0; index < digest.Length; index++)
        {
            var high = HexNibble(hex[offset + (index * HexCharactersPerByte)]);
            var low = HexNibble(hex[offset + (index * HexCharactersPerByte) + 1]);
            digest[index] = (byte)((high << 4) | low);
        }

        return digest;
    }

    /// <summary>Reads a nonnegative decimal count without allocating a substring.</summary>
    /// <param name="text">The manifest text.</param>
    /// <param name="length">The count length.</param>
    /// <param name="count">The parsed count.</param>
    /// <returns>Whether the count is valid.</returns>
    private static bool TryReadCount(string text, int length, out long count)
    {
        count = 0;
        for (var index = 0; index < length; index++)
        {
            var digit = text[index] - '0';
            if (digit is < 0 or > LargestDecimalDigit || count > (long.MaxValue - digit) / DecimalBase)
            {
                return false;
            }

            count = (count * DecimalBase) + digit;
        }

        return true;
    }

    /// <summary>Converts one hexadecimal character to a nibble.</summary>
    /// <param name="value">The hexadecimal character.</param>
    /// <returns>The nibble value.</returns>
    /// <exception cref="FormatException">The character is not hexadecimal.</exception>
    private static int HexNibble(char value) => value switch
    {
        >= '0' and <= '9' => value - '0',
        >= 'A' and <= 'F' => value - 'A' + DecimalBase,
        >= 'a' and <= 'f' => value - 'a' + DecimalBase,
        _ => throw new FormatException("The manifest digest contains a non-hexadecimal character."),
    };

    /// <summary>Reads a bounded set of original rows captured by triggers.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="afterStoreIdentity">The last store identity.</param>
    /// <param name="afterOperationId">The last operation identity.</param>
    /// <returns>The changes and final key.</returns>
    private static (List<JournalChange> Changes, string LastStoreIdentity, string LastOperationId) ReadJournalBatch(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        string afterStoreIdentity,
        string afterOperationId)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            SELECT store_identity, operation_id, operation_state, attempt_count, changed_at_utc,
                   reason_code, retry_started_utc, retry_due_utc, retry_previous_delay_ticks,
                   retry_transient_attempt_count, retry_authentication_state, retry_credentials_version,
                   state.proof, state.existed,
            """ + StateLengthColumns + """
            , length(state.proof) AS proof_length
            FROM oc_state_journal AS state
            WHERE store_identity > $storeIdentity
               OR (store_identity = $storeIdentity AND operation_id > $operationId)
            ORDER BY store_identity, operation_id LIMIT $batchSize;
            """);
        _ = command.Bind(StoreIdentityParameter, afterStoreIdentity);
        _ = command.Bind(OperationIdParameter, afterOperationId);
        _ = command.Bind("$batchSize", ProofBatchSize);
        using var reader = command.Query();
        List<JournalChange> changes = [];
        while (reader.Read())
        {
            var existed = reader.GetInt32(JournalExistedIndex) != 0;
            var originalState = existed ? Serialize(reader) : null;
            var proof = reader.IsDBNull(ProofColumnIndex) ? null : ReadProof(reader, ProofColumnIndex);
            afterStoreIdentity = reader.GetString(0);
            afterOperationId = reader.GetString(1);
            changes.Add(new(
                afterStoreIdentity,
                afterOperationId,
                existed,
                originalState,
                proof));
        }

        return (changes, afterStoreIdentity, afterOperationId);
    }

    /// <summary>Authenticates an old row, then signs its replacement or removal.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="cipher">The database scoped cipher.</param>
    /// <param name="manifest">The set accumulator.</param>
    /// <param name="change">The original row.</param>
    /// <exception cref="LocalStoreRecordAuthenticationException">The original row has no valid proof.</exception>
    private static void ApplyJournalChange(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        SqliteRecordCipher cipher,
        JournalManifest manifest,
        JournalChange change)
    {
        if (change.Existed)
        {
            if (change.Proof is null || change.OriginalState is null
                || !FixedTimeEquals(cipher.UnprotectBytes(change.Proof, SqliteRecordContext.KeyCheck(), ProofColumn), change.OriginalState))
            {
                throw new LocalStoreRecordAuthenticationException("A changed SQLite operation state failed authentication.");
            }

            XorProofDigest(manifest.Digest, change.StoreIdentity, change.OperationId, change.Proof);
            manifest.Count--;
        }

        SignCurrentRow(connection, transaction, cipher, manifest, change);
        manifest.Changed = true;
    }

    /// <summary>Signs a journaled current row or removes its old proof.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="cipher">The database scoped cipher.</param>
    /// <param name="manifest">The set accumulator.</param>
    /// <param name="change">The original row.</param>
    private static void SignCurrentRow(
        SqliteDatabase connection,
        SqliteTransaction transaction,
        SqliteRecordCipher cipher,
        JournalManifest manifest,
        JournalChange change)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("""
            SELECT store_identity, operation_id, operation_state, attempt_count, changed_at_utc,
                   reason_code, retry_started_utc, retry_due_utc, retry_previous_delay_ticks,
                   retry_transient_attempt_count, retry_authentication_state, retry_credentials_version,
            """ + StateLengthColumns + " " + """
            FROM oc_outbox_operation_states AS state WHERE store_identity = $storeIdentity AND operation_id = $operationId;
            """);
        _ = command.Bind(StoreIdentityParameter, change.StoreIdentity);
        _ = command.Bind(OperationIdParameter, change.OperationId);
        byte[]? proof;
        using (var reader = command.Query())
        {
            proof = reader.Read()
                ? cipher.ProtectBytes(Serialize(reader), SqliteRecordContext.KeyCheck(), ProofColumn)
                : null;
        }

        if (proof is null)
        {
            DeleteProof(connection, transaction, change);
            return;
        }

        _ = WriteProofBatch(connection, transaction, [(change.StoreIdentity, change.OperationId, proof)]);
        XorProofDigest(manifest.Digest, change.StoreIdentity, change.OperationId, proof);
        manifest.Count++;
    }

    /// <summary>Removes a proof for a deleted state row.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The transaction.</param>
    /// <param name="change">The original row.</param>
    private static void DeleteProof(SqliteDatabase connection, SqliteTransaction transaction, JournalChange change)
    {
        using var command = connection.CreateStatement();
        command.UseTransaction(transaction);
        command.SetSql("DELETE FROM oc_operation_state_proofs WHERE store_identity = $storeIdentity AND operation_id = $operationId;");
        _ = command.Bind(StoreIdentityParameter, change.StoreIdentity);
        _ = command.Bind(OperationIdParameter, change.OperationId);
        _ = command.Execute();
    }

    /// <summary>The authenticated row count and set digest.</summary>
    /// <param name="count">The row count.</param>
    /// <param name="digest">The set digest.</param>
    private sealed class JournalManifest(long count, byte[] digest)
    {
        /// <summary>Gets or sets the row count.</summary>
        internal long Count { get; set; } = count;

        /// <summary>Gets the set digest.</summary>
        internal byte[] Digest { get; } = digest;

        /// <summary>Gets or sets whether the set changed.</summary>
        internal bool Changed { get; set; }
    }

    /// <summary>The original state saved by a connection-local trigger.</summary>
    /// <param name="StoreIdentity">The store identity.</param>
    /// <param name="OperationId">The operation identity.</param>
    /// <param name="Existed">Whether the row existed before the transaction.</param>
    /// <param name="OriginalState">The old canonical state.</param>
    /// <param name="Proof">The old proof.</param>
    private sealed record JournalChange(string StoreIdentity, string OperationId, bool Existed, byte[]? OriginalState, byte[]? Proof);
}
