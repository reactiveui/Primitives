// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="ServerOperationProcessor"/>.</summary>
/// <content>Proves bounded batching preserves sequential state, independent proofs and cancellation.</content>
public sealed partial class ServerOperationProcessorTests
{
    /// <summary>The operation count used to measure physical transaction amplification.</summary>
    private const int BatchScaleOperationCount = 16;

    /// <summary>The distinct competing writer operation seed.</summary>
    private const int CompetingOperationSeed = 99;

    /// <summary>The preparations required after one competing commit invalidates two plans.</summary>
    private const int BatchReprepareCount = 4;

    /// <summary>The final revision after one competing and two requested operations.</summary>
    private const int CompetingFinalRevision = 3;

    /// <summary>Verifies sixteen dependent operations need two physical write commits and replay without another write.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task SqliteBatchAmortizesPhysicalCommitsAndReplaysIdenticalOutputs()
    {
        using var lease = CreateJournal(true);
        var journal = (SqliteServerCommitJournal)lease.Journal;
        var handler = new RecordingHandler(static (context, count) =>
        {
            if (context.Snapshot.Revision != count - 1)
            {
                throw new InvalidOperationException("A preparation did not see sequential state.");
            }

            return BatchPreparation(context);
        });
        var processor = CreateProcessor(journal, handler);
        var operations = Enumerable.Range(1, BatchScaleOperationCount).Select(Operation).ToArray();
        var batch = Batch(operations);
        var first = await processor.ProcessAsync(batch, ClientIdentity(), CancellationToken.None);
        await Assert.That(journal.CommittedTransactionCount).IsEqualTo(DoubleCount);
        await Assert.That(first.Result.Operations).Count().IsEqualTo(BatchScaleOperationCount);
        await Assert.That(first.ProducedEvents).Count().IsEqualTo(BatchScaleOperationCount);
        var snapshot = journal.Read(StreamKey(), operations.Select(static operation => OperationKey((int)operation.ClientSequence)).ToArray());
        await Assert.That(snapshot.Revision).IsEqualTo(BatchScaleOperationCount);
        await Assert.That(snapshot.State?.Version).IsEqualTo("revision-16");
        await Assert.That(snapshot.Entries).Count().IsEqualTo(BatchScaleOperationCount);

        var replay = await processor.ProcessAsync(batch, ClientIdentity(), CancellationToken.None);
        await Assert.That(journal.CommittedTransactionCount).IsEqualTo(DoubleCount);
        await Assert.That(handler.PrepareCount).IsEqualTo(BatchScaleOperationCount);
        await Assert.That(replay.ProducedEvents.Select(static remoteEvent => remoteEvent.ServerCursor)
            .SequenceEqual(first.ProducedEvents.Select(static remoteEvent => remoteEvent.ServerCursor))).IsTrue();
    }

    /// <summary>Verifies a later preparation failure or cancellation retains only the valid earlier prefix.</summary>
    /// <param name="cancel">Whether the second preparation cancels instead of throwing.</param>
    /// <returns>The test operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SqliteBatchPreservesPreparedPrefixAfterLaterFailure(bool cancel)
    {
        using var lease = CreateJournal(true);
        var journal = (SqliteServerCommitJournal)lease.Journal;
        using var cancellation = new CancellationTokenSource();
        var handler = new RecordingHandler((context, count) =>
        {
            if (count == DoubleCount)
            {
                if (cancel)
                {
                    cancellation.Cancel();
                }
                else
                {
                    throw new InvalidOperationException("Later preparation failed.");
                }
            }

            return BatchPreparation(context);
        });
        var processor = CreateProcessor(journal, handler);
        var pending = processor.ProcessAsync(Batch(Operation(1), Operation(SecondOperationSeed)), ClientIdentity(), cancellation.Token).AsTask();
        if (cancel)
        {
            _ = await Assert.ThrowsAsync<OperationCanceledException>(() => pending);
        }
        else
        {
            _ = await Assert.ThrowsExactlyAsync<InvalidOperationException>(() => pending);
        }

        await Assert.That(journal.CommittedTransactionCount).IsEqualTo(1);
        var snapshot = journal.Read(StreamKey(), [OperationKey(1), OperationKey(SecondOperationSeed)]);
        await Assert.That(snapshot.Entries).Count().IsEqualTo(1);
        await Assert.That(snapshot.Entries[0].OperationKey).IsEqualTo(OperationKey(1));
        await Assert.That(snapshot.State?.Version).IsEqualTo("revision-1");
    }

    /// <summary>Verifies a stale first plan prevents dependent plans from applying speculative state.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task SqliteBatchRepreparesDependentPlansAfterConcurrentWriter()
    {
        using var lease = CreateJournal(true);
        var journal = (SqliteServerCommitJournal)lease.Journal;
        var handler = new RecordingHandler((context, count) =>
        {
            if (count == DoubleCount)
            {
                var key = OperationKey(CompetingOperationSeed);
                _ = journal.TryCommit(new(StreamKey(), 0, State("competing"), Stamp(key), [Entry(key, OperationResultKind.Accepted, [])]));
            }

            return BatchPreparation(context);
        });
        var processor = CreateProcessor(journal, handler);
        var result = await processor.ProcessAsync(Batch(Operation(1), Operation(SecondOperationSeed)), ClientIdentity(), CancellationToken.None);

        await Assert.That(result.Result.Operations.All(static operation => operation.Kind == OperationResultKind.Accepted)).IsTrue();
        await Assert.That(handler.PrepareCount).IsEqualTo(BatchReprepareCount);
        var snapshot = journal.Read(StreamKey(), [OperationKey(1), OperationKey(SecondOperationSeed), OperationKey(CompetingOperationSeed)]);
        await Assert.That(snapshot.Revision).IsEqualTo(CompetingFinalRevision);
        await Assert.That(snapshot.State?.Version).IsEqualTo("revision-3");
        await Assert.That(journal.CommittedTransactionCount).IsEqualTo(CompetingFinalRevision);
    }

    /// <summary>Creates a unique event and state based on the operation's sequential revision.</summary>
    /// <param name="context">The operation context.</param>
    /// <returns>The side-effect-free prepared operation.</returns>
    private static ServerOperationPreparation BatchPreparation(ServerOperationContext context)
    {
        var version = $"revision-{context.Snapshot.Revision + 1}";
        return new(
            new(context.Operation.OperationId, OperationResultKind.Accepted, null, version),
            State(version),
            [],
            [new(context.Operation.OperationId.Value, context.Operation.Payload, new Dictionary<string, string>())]);
    }
}
