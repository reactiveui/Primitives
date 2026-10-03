// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite.Tests;

/// <summary>Verifies persisted adapter retry scheduling and deterministic worker lifetime.</summary>
public sealed partial class SqliteLocalStoreAdapterTests
{
    /// <summary>Verifies closing a canceled enumeration preserves the returned lease until its owner releases it.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task StoppingEnumerationPreservesReturnedLeaseOwnership()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, MinimumRequiredSchemaVersion, false), CancellationToken.None);
        _ = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        var request = new OutboxLeaseRequest(Stream, 1, NormalWorkerBytes, TimeSpan.FromMinutes(1));
        using var requestCancellation = new CancellationTokenSource();
        using var enumerationCancellation = new CancellationTokenSource();
        Guid leaseId;
        await using (var iterator = adapter.LeasePendingOperationsAsync(request, requestCancellation.Token).GetAsyncEnumerator(enumerationCancellation.Token))
        {
            await Assert.That(await iterator.MoveNextAsync()).IsTrue();
            leaseId = iterator.Current.LeaseId;
            await enumerationCancellation.CancelAsync();
        }

        await using (var blocked = adapter.LeasePendingOperationsAsync(request, CancellationToken.None).GetAsyncEnumerator())
        {
            await Assert.That(await blocked.MoveNextAsync()).IsFalse();
        }

        await adapter.RenewLeaseAsync(leaseId, TimeSpan.FromMinutes(1), CancellationToken.None);
        await adapter.ReleaseLeaseAsync(leaseId, CancellationToken.None);
        var nextLease = await ReadSingleLeaseAsync(adapter, request);
        await Assert.That(nextLease.Operations[0].OperationId).IsEqualTo(operation.OperationId);
        await adapter.ReleaseLeaseAsync(nextLease.LeaseId, CancellationToken.None);
    }

    /// <summary>Verifies retry due times survive reopening and block leasing until explicitly rescheduled.</summary>
    /// <returns>The asynchronous test.</returns>
    [Test]
    public async Task RetryScheduleSurvivesReopenAndControlsLeasing()
    {
        using var database = TempDatabase.Create();
        await using var adapter = CreateAdapter(database.Path);
        await adapter.InitializeAsync(new(StoreIdentity, MinimumRequiredSchemaVersion, false), CancellationToken.None);
        var subscription = await adapter.GetOrCreateSubscriptionIdAsync(Stream, null, CancellationToken.None);
        var operation = CreateOperation(FirstClientSequence);
        _ = await adapter.CommitLocalOperationAsync(operation, CreateSnapshotMutation(0), CancellationToken.None);
        var started = TimeProvider.System.GetUtcNow();
        var retry = new RetryState(started, started.AddDays(1), TimeSpan.FromSeconds(1), 1, RetryAuthenticationState.RenewalRetryUsed, "credentials-v1");
        await adapter.SaveRetryStateAsync(operation.OperationId, retry, CancellationToken.None);
        await adapter.DisposeAsync();

        await using var reopened = CreateAdapter(database.Path);
        await reopened.InitializeAsync(new(StoreIdentity, MinimumRequiredSchemaVersion, false), CancellationToken.None);
        await Assert.That(await reopened.GetRetryStateAsync(operation.OperationId, CancellationToken.None)).IsEqualTo(retry);
        await Assert.That((await reopened.RecoverStreamAsync(Stream, subscription, CancellationToken.None)).PendingOperations.Count).IsEqualTo(1);
        var request = new OutboxLeaseRequest(Stream, 1, NormalWorkerBytes, TimeSpan.FromMinutes(1));
        await using (var iterator = reopened.LeasePendingOperationsAsync(request, CancellationToken.None).GetAsyncEnumerator())
        {
            await Assert.That(await iterator.MoveNextAsync()).IsFalse();
        }

        await reopened.SaveRetryStateAsync(operation.OperationId, RetryState.Start(started), CancellationToken.None);
        var lease = await ReadSingleLeaseAsync(reopened, request);
        await Assert.That(lease.Operations[0].OperationId).IsEqualTo(operation.OperationId);
        await reopened.ReleaseLeaseAsync(lease.LeaseId, CancellationToken.None);
    }

    /// <summary>A real adapter clock that signals when its worker enters an active commit.</summary>
    private sealed class BlockingCommitClock : TimeProvider, IDisposable
    {
        /// <summary>The signal permitting the active clock callback to return.</summary>
        private readonly ManualResetEventSlim _release = new();

        /// <summary>Gets the callback entry signal.</summary>
        public ManualResetEventSlim Entered { get; } = new();

        /// <summary>Gets or sets whether the worker callback blocks.</summary>
        public bool Block { get; set; }

        /// <inheritdoc/>
        public override DateTimeOffset GetUtcNow()
        {
            if (Block)
            {
                Entered.Set();
                _release.Wait();
            }

            return TimeProvider.System.GetUtcNow();
        }

        /// <summary>Releases the active callback.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Release() => _release.Set();

        /// <inheritdoc/>
        public void Dispose()
        {
            Release();
            Entered.Dispose();
            _release.Dispose();
        }
    }
}
