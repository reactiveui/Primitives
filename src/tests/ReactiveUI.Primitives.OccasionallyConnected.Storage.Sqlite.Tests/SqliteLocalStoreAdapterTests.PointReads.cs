// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Tests for <see cref="SqliteLocalStoreAdapter"/>.</summary>
/// <content>Indexed protected reads and operation-local cancellation lifetime.</content>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>The number of repeated point reads used to measure warm read costs.</summary>
    private const int PointReadCount = 256;

    /// <summary>The status and retry reads performed for each repetition.</summary>
    private const int PointReadKinds = 2;

    /// <summary>The small history used to warm mutation checks.</summary>
    private const int PointReadMutationHistory = 2;

    /// <summary>The allocation budget for one authenticated status read.</summary>
    private const long MaximumPointReadBytes = 32 * 1024;

    /// <summary>Verifies each warm point read authenticates one row, independent of table size.</summary>
    /// <param name="operationCount">The queued operation count.</param>
    /// <returns>The assertion task.</returns>
    [Test]
    [NotInParallel]
    [Arguments(64)]
    [Arguments(512)]
    [Arguments(1024)]
    public async Task ProtectedPointReadsAuthenticateOnlySelectedRow(int operationCount)
    {
        using var database = TempDatabase.Create();
        var provider = new PointReadKeyProvider(CreateTestKey(FirstKeyId, FirstKeyFill));
        OperationId selected;
        await using (var adapter = CreateEncryptedAdapter(database.Path, provider))
        {
            await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
            _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
            selected = await FillPointReadHistoryAsync(adapter, operationCount);
        }

        await using var reopened = CreateEncryptedAdapter(database.Path, provider);
        await reopened.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        provider.Reads = 0;
        _ = await reopened.GetOperationStatusAsync(selected, CancellationToken.None);
        await Assert.That(provider.Reads).IsEqualTo(1);
        _ = await reopened.GetRetryStateAsync(selected, CancellationToken.None);
        provider.Reads = 0;
        var allocationStart = GC.GetTotalAllocatedBytes(precise: true);
        var started = Stopwatch.GetTimestamp();
        for (var index = 0; index < PointReadCount; index++)
        {
            _ = await reopened.GetOperationStatusAsync(selected, CancellationToken.None);
            _ = await reopened.GetRetryStateAsync(selected, CancellationToken.None);
        }

        var elapsed = Stopwatch.GetElapsedTime(started);
        var bytesPerRead = (GC.GetTotalAllocatedBytes(precise: true) - allocationStart) / (PointReadCount * PointReadKinds);
        TestContext.Current?.Output.WriteLine($"point.sqlite rows={operationCount} reads={PointReadCount * PointReadKinds} "
            + $"key_resolutions={provider.Reads} allocated_bytes_per_read={bytesPerRead} elapsed_ms={elapsed.TotalMilliseconds:F1}");
        await Assert.That(provider.Reads).IsEqualTo(PointReadCount * PointReadKinds);
        await Assert.That(bytesPerRead).IsLessThanOrEqualTo(MaximumPointReadBytes);
        await Assert.That(elapsed).IsLessThanOrEqualTo(MaximumRecoveryTime);
    }

    /// <summary>Verifies an external change invalidates the trusted state set before a point read.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WarmPointReadRejectsExternalTamperingAndProviderRevocation()
    {
        using var database = TempDatabase.Create();
        var provider = new PointReadKeyProvider(CreateTestKey(FirstKeyId, FirstKeyFill));
        await using var adapter = CreateEncryptedAdapter(database.Path, provider);
        await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var selected = await FillPointReadHistoryAsync(adapter, PointReadMutationHistory);
        _ = await adapter.GetOperationStatusAsync(selected, CancellationToken.None);
        provider.Available = false;
        await Assert.That(async () => await adapter.GetOperationStatusAsync(selected, CancellationToken.None))
            .ThrowsExactly<LocalStoreRecordAuthenticationException>();
        provider.Available = true;
        using (var connection = OpenRawConnection(database.Path))
        {
            connection.Execute("UPDATE oc_outbox_operation_states SET operation_state = 5;");
        }

        await Assert.That(async () => await adapter.GetRetryStateAsync(selected, CancellationToken.None))
            .ThrowsExactly<LocalStoreRecordAuthenticationException>();
    }

    /// <summary>Verifies canceling a completed operation cannot interrupt the reused native connection.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task CompletedOperationCancellationDoesNotLeakIntoReusedConnection()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        using var cancellation = new CancellationTokenSource();
        var operation = CreateOperation(1);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), cancellation.Token);
        await cancellation.CancelAsync();
        var status = await adapter.GetOperationStatusAsync(operation.OperationId, CancellationToken.None);
        _ = await adapter.CommitLocalOperationAsync(CreateOperation(SecondClientSequence), CreateSnapshotMutation(1), CancellationToken.None);
        await Assert.That(status?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
    }

    /// <summary>Checks retained statements read current state after the owning connection commits a replacement.</summary>
    /// <returns>The assertion task.</returns>
    [Test]
    public async Task WarmPointReadsObserveOwnCommittedRetryReplacements()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateEncryptedAdapter(database.Path, CreateFirstKeyProvider());
        await adapter.InitializeAsync(CreateEncryptedInitialization(), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(1);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        var first = RetryState.Start(DateTimeOffset.UnixEpoch) with { CredentialsVersion = "first" };
        var second = first with { CredentialsVersion = "second" };
        await adapter.SaveRetryStateAsync(operation.OperationId, first, CancellationToken.None);
        var firstRead = await adapter.GetRetryStateAsync(operation.OperationId, CancellationToken.None);
        await adapter.SaveRetryStateAsync(operation.OperationId, second, CancellationToken.None);
        var secondRead = await adapter.GetRetryStateAsync(operation.OperationId, CancellationToken.None);
        await Assert.That(firstRead).IsEqualTo(first);
        await Assert.That(secondRead).IsEqualTo(second);
    }

    /// <summary>Fills one protected stream and returns the final operation identity.</summary>
    /// <param name="adapter">The initialized adapter.</param>
    /// <param name="count">The queued operation count.</param>
    /// <returns>The final operation identity.</returns>
    private static async Task<OperationId> FillPointReadHistoryAsync(SqliteLocalStoreAdapter adapter, int count)
    {
        var selected = OperationId.New();
        for (var sequence = 1; sequence <= count; sequence++)
        {
            var operation = CreateOperation(sequence);
            selected = operation.OperationId;
            _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(sequence - 1), CancellationToken.None);
        }

        return selected;
    }

    /// <summary>Counts historical key resolutions without changing the provider's key.</summary>
    /// <param name="key">The key used by the test store.</param>
    private sealed class PointReadKeyProvider(LocalStoreKey key) : ILocalStoreKeyProvider
    {
        /// <summary>Gets or sets the historical lookup count.</summary>
        internal int Reads { get; set; }

        /// <summary>Gets or sets whether the key remains available.</summary>
        internal bool Available { get; set; } = true;

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public LocalStoreKey GetCurrentKey() => key;

        /// <inheritdoc/>
        public LocalStoreKey? GetKey(string keyId)
        {
            Reads++;
            return Available && keyId == key.KeyId ? key : null;
        }
    }
}
