// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Data.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>SQL metacharacter round-trip tests for <see cref="ServerStreamHub"/> backed by the SQLite journal.</summary>
public sealed partial class ServerStreamHubTests
{
    /// <summary>A tenant identifier made of SQL metacharacters.</summary>
    private const string SqlTenant = "tenant'; DROP TABLE oc_server_journal_events; --";

    /// <summary>A client identifier made of SQL metacharacters.</summary>
    private const string SqlClient = "client\"); DELETE FROM oc_server_journal_ledger; /*";

    /// <summary>A payload contract identifier made of SQL metacharacters.</summary>
    private const string SqlContract = "contract' OR '1'='1";

    /// <summary>A payload hash made of SQL metacharacters.</summary>
    private const string SqlPayloadHash = "hash%_[]\\;--";

    /// <summary>Verifies SQL metacharacters in principals and payload fields round-trip through the SQLite journal unchanged.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ApplyOperationsAsyncWithSqliteRoundTripsSqlMetacharactersAsData()
    {
        using var database = new SqliteLease();
        var operation = Operation(1, PayloadA) with
        {
            Payload = new(SqlContract, 1, ContentType, "'; DROP TABLE x; --"u8.ToArray(), SqlPayloadHash),
            Metadata = new Dictionary<string, string> { ["key'; --"] = "value\"); DROP TABLE y; --" },
        };
        var principal = new ServerAuthenticatedClient(SqlTenant, SqlClient);
        IReadOnlyList<string> tablesBefore;
        await using (var first = ServerStreamHub.CreateSqlite(database.Path, Options(new AllowPolicy(SqlTenant), new RecordingDomainHandler())))
        {
            var accepted = await first.ApplyOperationsAsync(Batch(operation), principal, CancellationToken.None);
            await Assert.That(accepted.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
            tablesBefore = ReadTableNames(database.Path);
        }

        var domain = new RecordingDomainHandler();
        await using var reopened = ServerStreamHub.CreateSqlite(database.Path, Options(new AllowPolicy(SqlTenant), domain));
        var replayed = await reopened.ApplyOperationsAsync(Batch(operation), principal, CancellationToken.None);
        var page = await ReadFirstBatchAsync(reopened, principal, SubscriptionId.New());

        await Assert.That(replayed.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(domain.CallCount).IsEqualTo(0);
        await Assert.That(page.Events).Count().IsEqualTo(SingleCount);
        await Assert.That(page.Events[0].Payload.ContractId).IsEqualTo(SqlContract);
        await Assert.That(page.Events[0].Payload.PayloadHash).IsEqualTo(SqlPayloadHash);
        await Assert.That(page.Events[0].Origin?.ClientId).IsEqualTo(SqlClient);
        await Assert.That(string.Join(",", ReadTableNames(database.Path))).IsEqualTo(string.Join(",", tablesBefore));
    }

    /// <summary>Verifies another tenant cannot read events stored under a SQL-metacharacter tenant through pattern matching.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SubscribeStreamAsyncWithSqliteDoesNotMatchTenantByWildcard()
    {
        using var database = new SqliteLease();
        await using (var writer = ServerStreamHub.CreateSqlite(database.Path, Options(new AllowPolicy(SqlTenant), new RecordingDomainHandler())))
        {
            _ = await writer.ApplyOperationsAsync(Batch(Operation(1, PayloadA)), new(SqlTenant, Client), CancellationToken.None);
        }

        const string WildcardTenant = "tenant%";
        await using var reader = ServerStreamHub.CreateSqlite(database.Path, Options(new AllowPolicy(WildcardTenant), new RecordingDomainHandler()));
        using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(CancellationMilliseconds * SubscribeGuardTimeoutSeconds));
        var enumerable = reader.SubscribeStreamAsync(
            new(Stream, SubscriptionId.New(), null, StartPosition.FromSequence(0)),
            new(WildcardTenant, Client),
            timeout.Token);
        await using var enumerator = enumerable.GetAsyncEnumerator(timeout.Token);
        var events = 0;
        try
        {
            while (await enumerator.MoveNextAsync())
            {
                events += enumerator.Current.Events.Count;
            }
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            // The empty long poll ends at the guard timeout.
        }

        await Assert.That(events).IsEqualTo(0);
    }

    /// <summary>Reads the table names in a SQLite database.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The sorted table names.</returns>
    private static List<string> ReadTableNames(string path)
    {
        var connectionString = new SqliteConnectionStringBuilder { DataSource = path, Pooling = false }.ToString();
        using var connection = new SqliteConnection(connectionString);
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name;";
        using var reader = command.ExecuteReader();
        var names = new List<string>();
        while (reader.Read())
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }
}
