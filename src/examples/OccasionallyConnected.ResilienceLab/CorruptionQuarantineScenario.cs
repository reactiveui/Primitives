// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;
using ReactiveUI.Primitives.OccasionallyConnected.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Demonstrates that a corrupt SQLite record is quarantined while healthy streams still recover.</summary>
internal static class CorruptionQuarantineScenario
{
    /// <summary>The command-line scenario name.</summary>
    internal const string ScenarioName = "corruption-quarantine";

    /// <summary>The trusted client identifier.</summary>
    private const string ClientId = "device-a";

    /// <summary>The store identity.</summary>
    private const string StoreIdentity = "resilience-lab-corruption";

    /// <summary>The healthy snapshot text.</summary>
    private const string HealthyText = "healthy";

    /// <summary>The private text stored in the record that gets corrupted.</summary>
    private const string PrivateText = "private-reading-4711";

    /// <summary>The payload contract used by the scenario.</summary>
    private const string Contract = "lab-reading";

    /// <summary>The payload content type used by the scenario.</summary>
    private const string ContentType = "application/json";

    /// <summary>The stable reason code the SQLite store writes for corrupt payload rows.</summary>
    private const string CorruptRowReasonCode = "sqlite-payload-row-corrupt";

    /// <summary>The outcome recorded when a stream has no quarantine marker.</summary>
    private const string NotQuarantined = "none";

    /// <summary>The outcome recorded when a stream has a quarantine marker.</summary>
    private const string Quarantined = "quarantined";

    /// <summary>The outcome recorded when recovery withholds a snapshot.</summary>
    private const string Withheld = "withheld";

    /// <summary>The outcome recorded when recovery returns a snapshot.</summary>
    private const string Returned = "returned";

    /// <summary>The outcome recorded when recovery succeeds.</summary>
    private const string Recovered = "recovered";

    /// <summary>The deterministic healthy operation seed.</summary>
    private const int HealthyOperationSeed = 601;

    /// <summary>The deterministic corrupt operation seed.</summary>
    private const int CorruptOperationSeed = 602;

    /// <summary>The healthy stream.</summary>
    private static readonly StreamId HealthyStream = new("resilience/healthy");

    /// <summary>The stream whose snapshot row gets corrupted.</summary>
    private static readonly StreamId CorruptStream = new("resilience/corrupt");

    /// <summary>Runs the scenario.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The scenario invariants.</returns>
    internal static async ValueTask<IReadOnlyList<ResilienceLabCaseResult>> RunAsync(CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(ResilienceLabLoopback.ScenarioTimeout);
        var directory = ResilienceLabContext.CreateTemporaryDirectory("reactiveui-oc-corruption");
        try
        {
            return await RunInDirectoryAsync(Path.Combine(directory, "client.db"), timeout.Token).ConfigureAwait(false);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Runs the scenario against one database file.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The scenario invariants.</returns>
    private static async ValueTask<IReadOnlyList<ResilienceLabCaseResult>> RunInDirectoryAsync(
        string databasePath,
        CancellationToken cancellationToken)
    {
        SubscriptionId healthySubscription;
        SubscriptionId corruptSubscription;
        await using (var store = new SqliteLocalStoreAdapter(databasePath))
        {
            await store.InitializeAsync(CreateInitialization(), cancellationToken).ConfigureAwait(false);
            healthySubscription = await CommitAsync(store, HealthyStream, HealthyOperationSeed, HealthyText, cancellationToken)
                .ConfigureAwait(false);
            corruptSubscription = await CommitAsync(store, CorruptStream, CorruptOperationSeed, PrivateText, cancellationToken)
                .ConfigureAwait(false);
        }

        CorruptSnapshotSchemaVersion(databasePath);

        await using var reopened = new SqliteLocalStoreAdapter(databasePath);
        await reopened.InitializeAsync(CreateInitialization(), cancellationToken).ConfigureAwait(false);
        var healthy = await reopened.RecoverStreamAsync(HealthyStream, healthySubscription, cancellationToken).ConfigureAwait(false);
        var failure = await TryRecoverAsync(reopened, corruptSubscription, cancellationToken).ConfigureAwait(false);
        var marker = await reopened.GetPayloadQuarantineAsync(CorruptStream, cancellationToken).ConfigureAwait(false);
        var healthyMarker = await reopened.GetPayloadQuarantineAsync(HealthyStream, cancellationToken).ConfigureAwait(false);
        var guarded = await reopened.RecoverStreamAsync(CorruptStream, corruptSubscription, cancellationToken).ConfigureAwait(false);
        List<ResilienceLabCaseResult> cases =
        [
            ResilienceLabLoopback.Case($"{ScenarioName}.healthy-snapshot-survives", HealthyText, ReadText(healthy.Snapshot?.State)),
            ResilienceLabLoopback.Case($"{ScenarioName}.healthy-pending-survives", 1, healthy.PendingOperations.Count),
            ResilienceLabLoopback.Case($"{ScenarioName}.healthy-not-quarantined", NotQuarantined, DescribeMarker(healthyMarker)),
        ];
        cases.AddRange(CreateCorruptCases(failure, marker, guarded));
        return cases;
    }

    /// <summary>Creates the invariants for the corrupt stream.</summary>
    /// <param name="failure">The first recovery failure.</param>
    /// <param name="marker">The persisted quarantine marker.</param>
    /// <param name="guarded">The recovery result after quarantine.</param>
    /// <returns>The corrupt stream invariants.</returns>
    private static List<ResilienceLabCaseResult> CreateCorruptCases(
        RecoveryFailure failure,
        LocalPayloadQuarantineRecord? marker,
        RecoveredStream guarded)
    {
        var reasonCode = marker?.ReasonCode ?? string.Empty;
        var sanitized = !failure.Message.Contains(PrivateText, StringComparison.Ordinal)
            && !reasonCode.Contains(PrivateText, StringComparison.Ordinal);
        var guardedMarker = guarded.Quarantine?.QuarantineId == marker?.QuarantineId ? guarded.Quarantine : null;
        return
        [
            ResilienceLabLoopback.Case($"{ScenarioName}.corrupt-recovery-fails-closed", nameof(InvalidOperationException), failure.Outcome),
            ResilienceLabLoopback.Case(
                $"{ScenarioName}.corrupt-quarantine-reason",
                LocalPayloadQuarantineReason.PersistedRecordCorrupt,
                marker?.Reason ?? LocalPayloadQuarantineReason.SchemaRejected),
            ResilienceLabLoopback.Case($"{ScenarioName}.corrupt-reason-code", CorruptRowReasonCode, reasonCode),
            ResilienceLabLoopback.Case($"{ScenarioName}.reason-sanitized", true, sanitized),
            ResilienceLabLoopback.Case($"{ScenarioName}.guarded-recovery-reports-quarantine", Quarantined, DescribeMarker(guardedMarker)),
            ResilienceLabLoopback.Case(
                $"{ScenarioName}.guarded-recovery-withholds-snapshot",
                Withheld,
                guarded.Snapshot is null ? Withheld : Returned),
        ];
    }

    /// <summary>Describes whether a quarantine marker exists.</summary>
    /// <param name="marker">The quarantine marker, or null.</param>
    /// <returns>The marker outcome.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static string DescribeMarker(LocalPayloadQuarantineRecord? marker) =>
        marker is null ? NotQuarantined : Quarantined;

    /// <summary>Commits one pending operation and snapshot for a stream.</summary>
    /// <param name="store">The SQLite store.</param>
    /// <param name="streamId">The stream identifier.</param>
    /// <param name="operationSeed">The deterministic operation seed.</param>
    /// <param name="text">The snapshot text.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The stream subscription identifier.</returns>
    private static async ValueTask<SubscriptionId> CommitAsync(
        SqliteLocalStoreAdapter store,
        StreamId streamId,
        int operationSeed,
        string text,
        CancellationToken cancellationToken)
    {
        var subscriptionId = await store.GetOrCreateSubscriptionIdAsync(streamId, null, cancellationToken).ConfigureAwait(false);
        var operation = new SyncOperation
        {
            OperationId = new(ResilienceLabLoopback.CreateGuid(operationSeed)),
            StreamId = streamId,
            ClientSequence = 1,
            TimestampUtc = ResilienceLabLoopback.InitialTime,
            Type = SyncOperationType.Update,
            Payload = CreatePayload(text),
            Policy = OperationPolicy.Default,
        };
        _ = await store.CommitLocalOperationAsync(operation, new(streamId, CreatePayload(text), 1, 0), cancellationToken)
            .ConfigureAwait(false);
        return subscriptionId;
    }

    /// <summary>Recovers the corrupt stream and reports the typed failure.</summary>
    /// <param name="store">The reopened store.</param>
    /// <param name="subscriptionId">The corrupt stream subscription.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The recovery outcome.</returns>
    private static async ValueTask<RecoveryFailure> TryRecoverAsync(
        SqliteLocalStoreAdapter store,
        SubscriptionId subscriptionId,
        CancellationToken cancellationToken)
    {
        try
        {
            _ = await store.RecoverStreamAsync(CorruptStream, subscriptionId, cancellationToken).ConfigureAwait(false);
            return new(Recovered, string.Empty);
        }
        catch (InvalidOperationException exception)
        {
            return new(exception.GetType().Name, exception.Message);
        }
    }

    /// <summary>Corrupts the stored snapshot schema version of the corrupt stream outside the store API.</summary>
    /// <param name="databasePath">The SQLite database path.</param>
    private static void CorruptSnapshotSchemaVersion(string databasePath)
    {
        using var connection = new SqliteDatabase(databasePath, create: false);

        using var command = connection.CreateStatement();
        command.SetSql("""
            UPDATE oc_snapshots
            SET payload_schema_version = 0
            WHERE store_identity = $storeIdentity AND stream_id = $streamId;
            """);
        _ = command.Bind("$storeIdentity", StoreIdentity);
        _ = command.Bind("$streamId", CorruptStream.Value);
        _ = command.Execute();
    }

    /// <summary>Creates the store initialization.</summary>
    /// <returns>The store initialization.</returns>
    private static LocalStoreInitialization CreateInitialization() =>
        new(StoreIdentity, 1, RequireAuthenticatedEncryptionAtRest: false) { ClientId = ClientId };

    /// <summary>Creates a text payload envelope.</summary>
    /// <param name="text">The payload text.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope CreatePayload(string text) =>
        new(Contract, 1, ContentType, Encoding.UTF8.GetBytes(text), $"hash-{text.Length}");

    /// <summary>Reads the text of a payload envelope.</summary>
    /// <param name="payload">The payload envelope.</param>
    /// <returns>The payload text, or an empty string when absent.</returns>
    private static string ReadText(PayloadEnvelope? payload) =>
        payload is null ? string.Empty : Encoding.UTF8.GetString(payload.Payload.Span);

    /// <summary>Describes one recovery attempt.</summary>
    /// <param name="Outcome">The typed failure name, or the recovered outcome.</param>
    /// <param name="Message">The failure message, or empty after success.</param>
    private readonly record struct RecoveryFailure(string Outcome, string Message);
}
