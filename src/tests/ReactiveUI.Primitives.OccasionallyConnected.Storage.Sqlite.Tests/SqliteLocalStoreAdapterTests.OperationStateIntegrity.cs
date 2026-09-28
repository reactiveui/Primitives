// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Authenticated operation state rows in the V1 schema.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The operation attempted after an external state change.</summary>
    private const int SubsequentOperationIndex = 2;

    /// <summary>Changing an operation into a terminal state fails before a filtered recovery read.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenProtectedOperationStateBecomesTerminal_ThenOpenFailsAuthentication()
    {
        using var database = TempDatabase.Create();
        _ = await SeedEncryptedDatabaseAsync(database.Path);
        await using (var connection = OpenRawConnection(database.Path))
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                UPDATE oc_outbox_operation_states SET operation_state = 5
                WHERE operation_id = (SELECT operation_id FROM oc_outbox WHERE client_sequence = 2);
                """;
            _ = await command.ExecuteNonQueryAsync();
        }

        await AssertEncryptedOpenFailsAuthenticationAsync(database.Path);
    }

    /// <summary>Moving a valid state proof onto another row fails authentication.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenOperationStateProofsAreSwapped_ThenOpenFailsAuthentication()
    {
        using var database = TempDatabase.Create();
        _ = await SeedEncryptedDatabaseAsync(database.Path);
        await using (var connection = OpenRawConnection(database.Path))
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = """
                UPDATE oc_operation_state_proofs
                SET proof = (SELECT proof FROM oc_operation_state_proofs
                             WHERE operation_id = (SELECT operation_id FROM oc_outbox WHERE client_sequence = 1))
                WHERE operation_id = (SELECT operation_id FROM oc_outbox WHERE client_sequence = 2);
                """;
            _ = await command.ExecuteNonQueryAsync();
        }

        await AssertEncryptedOpenFailsAuthenticationAsync(database.Path);
    }

    /// <summary>Removing a proof cannot make the operation state appear valid.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenOperationStateProofIsMissing_ThenOpenFailsAuthentication()
    {
        using var database = TempDatabase.Create();
        _ = await SeedEncryptedDatabaseAsync(database.Path);
        await using (var connection = OpenRawConnection(database.Path))
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "DELETE FROM oc_operation_state_proofs WHERE operation_id = (SELECT operation_id FROM oc_outbox WHERE client_sequence = 2);";
            _ = await command.ExecuteNonQueryAsync();
        }

        await AssertEncryptedOpenFailsAuthenticationAsync(database.Path);
    }

    /// <summary>Removing both the state and its proof cannot hide the deletion.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenOperationStateAndProofAreDeleted_ThenManifestFailsAuthentication()
    {
        using var database = TempDatabase.Create();
        _ = await SeedEncryptedDatabaseAsync(database.Path);
        await using (var connection = OpenRawConnection(database.Path))
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "DELETE FROM oc_outbox WHERE client_sequence = 2;";
            _ = await command.ExecuteNonQueryAsync();
        }

        await AssertEncryptedOpenFailsAuthenticationAsync(database.Path);
    }

    /// <summary>An empty protected database remains readable after the old key is retired.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenEmptyProtectedStoreRotatesKey_ThenManifestUsesNewKey()
    {
        using var database = TempDatabase.Create();
        await using (var first = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider()))
        {
            await first.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        }

        var oldKey = CreateTestKey(FirstKeyId, FirstKeyFill);
        var newKey = CreateTestKey(SecondKeyId, SecondKeyFill);
        await using (var rotating = CreateEncryptedAdapter(
            database.Path,
            new StaticLocalStoreKeyProvider(newKey, [oldKey])))
        {
            await rotating.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
            _ = await rotating.RotateEncryptionKeyAsync(CancellationToken.None);
        }

        await using var retired = CreateEncryptedAdapter(database.Path, new StaticLocalStoreKeyProvider(newKey));
        await retired.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
    }

    /// <summary>Measures protected append cost at two queue sizes and checks durable recovery.</summary>
    /// <param name="operationCount">The number of protected commits.</param>
    /// <returns>The asynchronous test.</returns>
    [Test]
    [NotInParallel]
    [Arguments(64)]
    [Arguments(512)]
    [Arguments(1024)]
    public async Task ProtectedOperationCommitsRecoverAtScale(int operationCount)
    {
        using var database = TempDatabase.Create();
        SubscriptionId subscriptionId;
        var allocatedAtStart = GC.GetTotalAllocatedBytes(precise: true);
        var started = Stopwatch.GetTimestamp();
        await using (var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider()))
        {
            await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
            subscriptionId = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            for (var index = 1; index <= operationCount; index++)
            {
                _ = await adapter.CommitLocalOperationAsync(
                    CreateOperation(index),
                    CreateSnapshotMutation(index - 1),
                    CancellationToken.None);
            }
        }

        var elapsed = Stopwatch.GetElapsedTime(started);
        var allocatedPerCommit = (GC.GetTotalAllocatedBytes(precise: true) - allocatedAtStart) / operationCount;
        TestContext.Current?.Output.WriteLine($"protected.sqlite count={operationCount} elapsed_ms={elapsed.TotalMilliseconds:F1} allocated_per_commit={allocatedPerCommit}");
        await using var reopened = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await reopened.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        var recovered = await reopened.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(operationCount);
    }

    /// <summary>An external SQLite writer cannot slip a forged state into the next protected commit.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task WhenStateChangesBetweenProtectedCommits_ThenNextCommitRejectsTampering()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(CreateOperation(1), CreateSnapshotMutation(0), CancellationToken.None);
        await using (var connection = OpenRawConnection(database.Path))
        await using (var command = connection.CreateCommand())
        {
            command.CommandText = "UPDATE oc_outbox_operation_states SET operation_state = 5;";
            _ = await command.ExecuteNonQueryAsync();
        }

        Func<Task> commit = () => adapter.CommitLocalOperationAsync(
            CreateOperation(SubsequentOperationIndex),
            CreateSnapshotMutation(1),
            CancellationToken.None).AsTask();
        await Assert.That(commit).ThrowsExactly<LocalStoreRecordAuthenticationException>();
    }

    /// <summary>Asserts that initialization rejects a modified authenticated state.</summary>
    /// <param name="path">The database path.</param>
    /// <returns>The asynchronous test.</returns>
    private static async Task AssertEncryptedOpenFailsAuthenticationAsync(string path)
    {
        await using var adapter = CreateEncryptedAdapter(path, CreateFirstKeyProvider());
        Func<Task> open = () => adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None).AsTask();
        await Assert.That(open).ThrowsExactly<LocalStoreRecordAuthenticationException>();
    }
}
