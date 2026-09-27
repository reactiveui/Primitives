// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Persists local commit and recovery state in SQLite.</summary>
/// <content>Creates and persists quarantine markers for corrupt or unauthenticated rows.</content>
internal sealed partial class SqliteLocalCommitStore
{
    /// <summary>The quarantine reason code for a structurally corrupt payload row.</summary>
    private const string CorruptPayloadRowReasonCode = "sqlite-payload-row-corrupt";

    /// <summary>The quarantine reason code for a protected row that failed decryption or authentication.</summary>
    private const string RecordAuthenticationFailedReasonCode = "sqlite-record-authentication-failed";

    /// <summary>Creates a quarantine request for a recovered corrupt payload row.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="subscriptionId">The subscription identifier.</param>
    /// <param name="exception">The payload corruption exception.</param>
    /// <param name="observedAtUtc">The observation timestamp.</param>
    /// <returns>The quarantine request.</returns>
    private static LocalPayloadQuarantineRequest CreateRecoveryQuarantineRequest(
        StreamId streamId,
        SubscriptionId subscriptionId,
        SqlitePayloadQuarantineException exception,
        DateTimeOffset observedAtUtc) =>
        CreateOutboxQuarantineRequest(
            streamId,
            exception.OperationId,
            exception.Evidence,
            observedAtUtc,
            exception.IsAuthenticationFailure) with { SubscriptionId = subscriptionId };

    /// <summary>Creates the exception thrown after a corrupt or unauthenticated row quarantined its stream.</summary>
    /// <param name="message">The message used for structurally corrupt rows.</param>
    /// <param name="exception">The quarantine exception.</param>
    /// <returns>The exception to throw.</returns>
    private static InvalidOperationException CreateQuarantinedException(string message, SqlitePayloadQuarantineException exception) =>
        exception.IsAuthenticationFailure
            ? new LocalStoreRecordAuthenticationException(
                "A persisted SQLite record failed authentication; its stream was quarantined and the record was not used.",
                exception)
            : new InvalidOperationException(message, exception);

    /// <summary>Creates a quarantine request for a corrupt outbox payload row.</summary>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <param name="evidence">The bounded evidence.</param>
    /// <param name="observedAtUtc">The observation timestamp.</param>
    /// <param name="authenticationFailed">Whether the row failed decryption or authentication.</param>
    /// <returns>The quarantine request.</returns>
    private static LocalPayloadQuarantineRequest CreateOutboxQuarantineRequest(
        StreamId streamId,
        OperationId? operationId,
        LocalPayloadQuarantineEvidence evidence,
        DateTimeOffset observedAtUtc,
        bool authenticationFailed) =>
        new()
        {
            StreamId = streamId,
            OperationId = operationId,
            Source = operationId.HasValue ? LocalPayloadQuarantineSource.OutboxOperation : LocalPayloadQuarantineSource.Snapshot,
            Reason = LocalPayloadQuarantineReason.PersistedRecordCorrupt,
            ReasonCode = authenticationFailed ? RecordAuthenticationFailedReasonCode : CorruptPayloadRowReasonCode,
            Evidence = evidence,
            ObservedAtUtc = observedAtUtc,
        };

    /// <summary>Persists a payload quarantine marker inside the caller's transaction.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The active transaction.</param>
    /// <param name="storeIdentity">The store identity.</param>
    /// <param name="request">The quarantine request.</param>
    /// <param name="evidence">The bounded payload evidence.</param>
    private static void PersistPayloadQuarantine(
        SqliteConnection connection,
        SqliteTransaction transaction,
        string storeIdentity,
        LocalPayloadQuarantineRequest request,
        LocalPayloadQuarantineEvidence evidence)
    {
        var normalized = SqliteLocalQuarantineRequestNormalizer.Normalize(request with { Evidence = evidence, Envelope = null });
        _ = SqliteLocalCommitSql.InsertPayloadQuarantine(connection, transaction, storeIdentity, normalized, Guid.NewGuid());
    }

    /// <summary>Reads recovery stream state and quarantines the stream when its protected cursor fails authentication.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The recovery transaction.</param>
    /// <param name="target">The recovered stream identity.</param>
    /// <param name="stream">The stored stream state.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>Whether the stream row exists.</returns>
    /// <exception cref="InvalidOperationException">The stream row failed validation or authentication and was quarantined.</exception>
    /// <remarks>An already quarantined stream reports its sequence without the cursor that failed authentication.</remarks>
    private bool TryReadRecoveryStreamState(
        SqliteConnection connection,
        SqliteTransaction transaction,
        in SqliteRecoveryTarget target,
        out SqliteLocalStreamState stream,
        CancellationToken cancellationToken)
    {
        try
        {
            return SqliteLocalCommitSql.TryReadStreamState(connection, transaction, target.StoreIdentity, target.StreamId, out stream);
        }
        catch (SqlitePayloadQuarantineException exception)
        {
            if (SqliteLocalCommitSql.IsStreamQuarantined(connection, transaction, target.StoreIdentity, target.StreamId))
            {
                stream = new(SqliteLocalCommitSql.ReadNextClientSequence(connection, transaction, target.StoreIdentity, target.StreamId), null);
                return true;
            }

            throw QuarantineRecovery(connection, transaction, in target, exception, "Recovered SQLite stream data was quarantined.", cancellationToken);
        }
    }

    /// <summary>Persists a recovery quarantine marker, commits it, and creates the exception to throw.</summary>
    /// <param name="connection">The connection.</param>
    /// <param name="transaction">The recovery transaction.</param>
    /// <param name="target">The recovered stream identity.</param>
    /// <param name="exception">The quarantine exception.</param>
    /// <param name="message">The message used for structurally corrupt rows.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The exception to throw.</returns>
    private InvalidOperationException QuarantineRecovery(
        SqliteConnection connection,
        SqliteTransaction transaction,
        in SqliteRecoveryTarget target,
        SqlitePayloadQuarantineException exception,
        string message,
        CancellationToken cancellationToken)
    {
        var request = CreateRecoveryQuarantineRequest(target.StreamId, target.SubscriptionId, exception, _timeProvider.GetUtcNow());
        PersistPayloadQuarantine(connection, transaction, target.StoreIdentity, request, exception.Evidence);
        cancellationToken.ThrowIfCancellationRequested();
        transaction.Commit();
        return CreateQuarantinedException(message, exception);
    }
}
