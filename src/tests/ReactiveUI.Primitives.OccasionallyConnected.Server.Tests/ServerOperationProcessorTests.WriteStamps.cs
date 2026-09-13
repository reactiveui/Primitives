// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Verifies server-owned write ordering across preparation and clock changes.</summary>
public sealed partial class ServerOperationProcessorTests
{
    /// <summary>Verifies one logical timestamp is captured before domain preparation.</summary>
    /// <param name="sqlite">Whether to use durable SQLite storage.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ProcessAsyncUsesOneServerTimestampBeforePreparation(bool sqlite)
    {
        using var lease = CreateJournal(sqlite);
        var clock = new PreparationClock(Start);
        var handler = new RecordingHandler((context, _) =>
        {
            clock.UtcNow = Start.AddHours(1);
            return new(
                new(context.Operation.OperationId, OperationResultKind.Accepted, null, FirstVersion),
                State(FirstVersion),
                [],
                [new(Guid.Parse(PreparedEventIdText), Payload(FirstVersion), new Dictionary<string, string>())]);
        });
        var processor = new ServerOperationProcessor(
            lease.Journal,
            new RecordingAuthorizer(),
            handler,
            options: new() { TimeProvider = clock });

        _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), CancellationToken.None);

        var snapshot = lease.Journal.Read(StreamKey(), [OperationKey(FirstOperationSeed)]);
        await Assert.That(snapshot.LastWriteStamp?.CommittedAtUtc).IsEqualTo(Start);
        await Assert.That(snapshot.Entries[0].Events[0].CommittedAtUtc).IsEqualTo(Start);
    }

    /// <summary>Verifies a later attempt uses a fresh server time clamped to the committed write time.</summary>
    /// <param name="sqlite">Whether to use durable SQLite storage.</param>
    /// <param name="hours">The clock adjustment after the first write.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [Arguments(false, -1)]
    [Arguments(false, 0)]
    [Arguments(false, 1)]
    [Arguments(true, -1)]
    [Arguments(true, 0)]
    [Arguments(true, 1)]
    public async Task ProcessAsyncPreservesLogicalWriteTimeAcrossClockChanges(bool sqlite, int hours)
    {
        using var lease = CreateJournal(sqlite);
        var clock = new PreparationClock(Start);
        var observed = new List<ServerWriteStamp>();
        var handler = new RecordingHandler((context, _) =>
        {
            observed.Add(context.CandidateWrite);
            return PrepareStampedWrite(context);
        });
        var processor = new ServerOperationProcessor(
            lease.Journal,
            new RecordingAuthorizer(),
            handler,
            options: new() { TimeProvider = clock });
        _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), CancellationToken.None);
        clock.UtcNow = Start.AddHours(hours);

        _ = await processor.ProcessAsync(
            Batch(Operation(SecondOperationSeed) with { TimestampUtc = Start.AddYears(1) }),
            ClientIdentity(),
            CancellationToken.None);

        var expected = Start.AddHours(Math.Max(0, hours));
        var snapshot = lease.Journal.Read(StreamKey(), [OperationKey(SecondOperationSeed)]);
        await Assert.That(observed[1].CommittedAtUtc).IsEqualTo(expected);
        await Assert.That(observed[1].ClientId).IsEqualTo(Client);
        await Assert.That(observed[1].OperationId).IsEqualTo(OperationId(SecondOperationSeed));
        await Assert.That(snapshot.LastWriteStamp).IsEqualTo(observed[1]);
        await Assert.That(snapshot.Entries[0].Events[0].CommittedAtUtc).IsEqualTo(expected);
    }

    /// <summary>Verifies a lost compare-and-swap retries with new provenance from the winning snapshot.</summary>
    /// <param name="sqlite">Whether to use durable SQLite storage.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ProcessAsyncRefreshesWriteStampAfterCompetingCommit(bool sqlite)
    {
        using var lease = CreateJournal(sqlite);
        var clock = new PreparationClock(Start.AddHours(-1));
        var observed = new List<ServerWriteStamp>();
        var handler = new RecordingHandler((context, _) =>
        {
            observed.Add(context.CandidateWrite);
            return PrepareStampedWrite(context);
        });
        var processor = new ServerOperationProcessor(
            new InjectingJournal(lease.Journal, CreateInterferingPlan()),
            new RecordingAuthorizer(),
            handler,
            options: new() { TimeProvider = clock });

        _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed)), ClientIdentity(), CancellationToken.None);

        var snapshot = lease.Journal.Read(StreamKey(), [OperationKey(FirstOperationSeed)]);
        await Assert.That(observed.Count).IsEqualTo(DoubleCount);
        await Assert.That(observed[0].CommittedAtUtc).IsEqualTo(clock.UtcNow);
        await Assert.That(observed[1].CommittedAtUtc).IsEqualTo(Start);
        await Assert.That(snapshot.LastWriteStamp).IsEqualTo(observed[1]);
        await Assert.That(snapshot.Entries[0].Events[0].CommittedAtUtc).IsEqualTo(Start);
    }

    /// <summary>Prepares a state write with a unique canonical event for the operation.</summary>
    /// <param name="context">The server preparation context.</param>
    /// <returns>The prepared write.</returns>
    private static ServerOperationPreparation PrepareStampedWrite(ServerOperationContext context) =>
        new(
            new(context.Operation.OperationId, OperationResultKind.Accepted, null, FirstVersion),
            State(FirstVersion),
            [],
            [new(context.Operation.OperationId.Value, Payload(FirstVersion), new Dictionary<string, string>())]);

    /// <summary>Provides a clock that domain preparation may advance.</summary>
    /// <param name="initial">The initial server time.</param>
    private sealed class PreparationClock(DateTimeOffset initial) : TimeProvider
    {
        /// <summary>Gets or sets the server time.</summary>
        internal DateTimeOffset UtcNow { get; set; } = initial;

        /// <inheritdoc />
        public override DateTimeOffset GetUtcNow() => UtcNow;
    }
}
