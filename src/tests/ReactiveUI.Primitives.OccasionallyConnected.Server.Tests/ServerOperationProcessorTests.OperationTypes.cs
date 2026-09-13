// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="ServerOperationProcessor"/>.</summary>
public sealed partial class ServerOperationProcessorTests
{
    /// <summary>The unknown operation type value used to prove unrecognized values are rejected.</summary>
    private const int UnknownOperationTypeValue = 255;

    /// <summary>Verifies every declared operation type can be processed and committed.</summary>
    /// <param name="sqlite">Whether to use the durable journal.</param>
    /// <param name="operationType">The declared operation type to process.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [Arguments(false, SyncOperationType.Append)]
    [Arguments(false, SyncOperationType.Update)]
    [Arguments(false, SyncOperationType.Delete)]
    [Arguments(false, SyncOperationType.Custom)]
    [Arguments(true, SyncOperationType.Append)]
    [Arguments(true, SyncOperationType.Update)]
    [Arguments(true, SyncOperationType.Delete)]
    [Arguments(true, SyncOperationType.Custom)]
    public async Task ProcessAsyncCommitsEachDeclaredOperationType(bool sqlite, SyncOperationType operationType)
    {
        using var lease = CreateJournal(sqlite);
        var handler = RecordingHandler.CreateAccepted();
        var operation = Operation(FirstOperationSeed) with { Type = operationType };
        var processor = CreateProcessor(lease.Journal, handler);

        var receipt = await processor.ProcessAsync(Batch(operation), ClientIdentity(), CancellationToken.None);
        var snapshot = lease.Journal.Read(StreamKey(), [OperationKey(FirstOperationSeed)]);

        await Assert.That(handler.PrepareCount).IsEqualTo(SingleCount);
        await Assert.That(receipt.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(snapshot.Revision).IsEqualTo(SingleCount);
        await Assert.That(snapshot.Entries.Count).IsEqualTo(SingleCount);
        await Assert.That(snapshot.Entries[0].OperationKey).IsEqualTo(OperationKey(FirstOperationSeed));
    }

    /// <summary>Verifies an unknown operation type is rejected before durable journal effects.</summary>
    /// <param name="sqlite">Whether to use the durable journal.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ProcessAsyncRejectsUnknownOperationTypeBeforeJournalEffects(bool sqlite)
    {
        using var lease = CreateJournal(sqlite);
        var handler = RecordingHandler.CreateAccepted();
        var operation = Operation(FirstOperationSeed) with { Type = (SyncOperationType)UnknownOperationTypeValue };
        var processor = CreateProcessor(lease.Journal, handler);

        async Task Act() => _ = await processor.ProcessAsync(Batch(operation), ClientIdentity(), CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<SyncBatchValidationException>();
        var snapshot = lease.Journal.Read(StreamKey(), [OperationKey(FirstOperationSeed)]);
        await Assert.That(handler.PrepareCount).IsEqualTo(0);
        await Assert.That(snapshot.Revision).IsEqualTo(0);
        await Assert.That(snapshot.Entries.Count).IsEqualTo(0);
        await Assert.That(snapshot.State).IsNull();
    }
}
