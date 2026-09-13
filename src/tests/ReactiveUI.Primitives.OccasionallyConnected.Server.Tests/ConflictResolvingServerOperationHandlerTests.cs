// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="ConflictResolvingServerOperationHandler"/>.</summary>
public sealed partial class ConflictResolvingServerOperationHandlerTests
{
    /// <summary>The authenticated tenant identifier.</summary>
    private const string Tenant = "tenant";

    /// <summary>The first authenticated client identifier.</summary>
    private const string Alpha = "alpha";

    /// <summary>The second authenticated client identifier with a later ordinal value.</summary>
    private const string Omega = "omega";

    /// <summary>The stream identifier text.</summary>
    private const string StreamName = "stream";

    /// <summary>The payload contract identifier.</summary>
    private const string Contract = "contract";

    /// <summary>The payload content type.</summary>
    private const string ContentType = "text/plain";

    /// <summary>The initial server version.</summary>
    private const string InitialVersion = "v0";

    /// <summary>The first accepted server version.</summary>
    private const string FirstVersion = "v1";

    /// <summary>The second accepted server version.</summary>
    private const string SecondVersion = "v2";

    /// <summary>The custom rejection reason.</summary>
    private const string RejectedReason = "custom-rejected";

    /// <summary>The current state payload text.</summary>
    private const string CurrentText = "current";

    /// <summary>The foreign stream and payload text.</summary>
    private const string ForeignText = "foreign";

    /// <summary>The pre-commit resolver event cursor that must not be retained.</summary>
    private const string ResolverEventCursor = "resolver-cursor";

    /// <summary>The interfering commit payload and cursor text.</summary>
    private const string InterferingText = "interfering";

    /// <summary>The expected single item count.</summary>
    private const int SingleCount = 1;

    /// <summary>The expected double item count.</summary>
    private const int DoubleCount = 2;

    /// <summary>The maximum operation count accepted by test processor options.</summary>
    private const int ProcessorMaximumBatchOperations = 16;

    /// <summary>The test journal stream bound.</summary>
    private const int JournalMaximumStreams = 8;

    /// <summary>The test journal row and event bound.</summary>
    private const int JournalMaximumRows = 32;

    /// <summary>The logical byte budget used by the tests.</summary>
    private const int LogicalByteBudget = 16_384;

    /// <summary>The operation retention duration in minutes.</summary>
    private const int RetentionMinutes = 5;

    /// <summary>The canonical fingerprint budget used by tests.</summary>
    private const int FingerprintBudget = 4096;

    /// <summary>The operation seed used for the injected competing commit.</summary>
    private const int InterferingSeed = 9;

    /// <summary>The first operation seed.</summary>
    private const int FirstOperationSeed = 1;

    /// <summary>The second operation seed.</summary>
    private const int SecondOperationSeed = 2;

    /// <summary>The third operation seed.</summary>
    private const int ThirdOperationSeed = 3;

    /// <summary>An unsupported conflict policy numeric value.</summary>
    private const int UnsupportedConflictPolicyValue = 999;

    /// <summary>An unsupported conflict policy value.</summary>
    private const ConflictPolicy UnsupportedConflictPolicy = (ConflictPolicy)UnsupportedConflictPolicyValue;

    /// <summary>The maximum CAS attempts used by these tests.</summary>
    private const int MaximumCommitAttempts = 2;

    /// <summary>The malicious negative retained collection count.</summary>
    private const int NegativeRetainedCount = -1;

    /// <summary>The first unsupported retained dictionary count.</summary>
    private const int OversizedDictionaryCount = 33;

    /// <summary>The metadata payload text.</summary>
    private const string MetadataText = "metadata";

    /// <summary>The fixed server timestamp.</summary>
    private static readonly DateTimeOffset Start = new(2026, 9, 13, 9, 0, 0, TimeSpan.Zero);

    /// <summary>The default stream identifier.</summary>
    private static readonly StreamId Stream = new(StreamName);

    /// <summary>The deterministic resolver-produced event identifier.</summary>
    private static readonly Guid ResolverEventId = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");

    /// <summary>Verifies missing trusted server provenance is rejected with a stable reason.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task LastWriterWinsResolverRejectsMissingServerProvenance()
    {
        var resolver = new LastWriterWinsResolver(new() { VersionFactory = new IncrementingVersionFactory() });
        var operation = Operation(FirstOperationSeed, Alpha, FirstVersion, Start);
        var context = new ConflictContext(State(InitialVersion, "initial"), [operation], ClientIdentity(Alpha));

        var result = await resolver.ResolveAsync(context, CancellationToken.None);

        await Assert.That(result.AcceptedOperations).IsEmpty();
        await Assert.That(result.RejectedOperations).Count().IsEqualTo(SingleCount);
        await Assert.That(result.RejectedOperations[0].ReasonCode).IsEqualTo("missing-server-write-provenance");
        await Assert.That(result.ServerVersion).IsEqualTo(InitialVersion);
    }

    /// <summary>Verifies missing incoming operations reject with a stable empty operation id.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task LastWriterWinsResolverRejectsMissingIncomingOperation()
    {
        var resolver = new LastWriterWinsResolver(new() { VersionFactory = new IncrementingVersionFactory() });
        var context = new ConflictContext(
            State(InitialVersion, CurrentText),
            [],
            ClientIdentity(Alpha),
            new() { CandidateWrite = Stamp(Start, Alpha, OperationId(FirstOperationSeed)) });

        var result = await resolver.ResolveAsync(context, CancellationToken.None);

        await Assert.That(result.AcceptedOperations).IsEmpty();
        await Assert.That(result.RejectedOperations).Count().IsEqualTo(SingleCount);
        await Assert.That(result.RejectedOperations[0].OperationId.Value).IsEqualTo(Guid.Empty);
        await Assert.That(result.RejectedOperations[0].ReasonCode).IsEqualTo("missing-server-write-provenance");
    }

    /// <summary>Verifies older trusted server writes lose to the current write stamp.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task LastWriterWinsResolverRejectsOlderServerWrite()
    {
        var resolver = new LastWriterWinsResolver(new() { VersionFactory = new IncrementingVersionFactory() });
        var operation = Operation(FirstOperationSeed, Alpha, InitialVersion, Start);
        var context = new ConflictContext(
            State(FirstVersion, CurrentText),
            [operation],
            ClientIdentity(Alpha),
            new() { CandidateWrite = Stamp(Start.AddMinutes(-1), Alpha, operation.OperationId), CurrentWrite = Stamp(Start, Alpha, OperationId(SecondOperationSeed)) });

        var result = await resolver.ResolveAsync(context, CancellationToken.None);

        await Assert.That(result.AcceptedOperations).IsEmpty();
        await Assert.That(result.RejectedOperations[0].ReasonCode).IsEqualTo("lww-stale-write");
        await Assert.That(result.RejectedOperations[0].MayResubmit).IsTrue();
        await Assert.That(result.ServerVersion).IsEqualTo(FirstVersion);
    }

    /// <summary>Verifies trusted server write stamps decide LWW, not client clocks.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task LastWriterWinsResolverIgnoresClientClockSkew()
    {
        var resolver = new LastWriterWinsResolver(new() { VersionFactory = new IncrementingVersionFactory() });
        var operation = Operation(SecondOperationSeed, Omega, InitialVersion, Start.AddYears(-1));
        var context = new ConflictContext(
            State(FirstVersion, CurrentText),
            [operation],
            ClientIdentity(Omega),
            new() { CandidateWrite = Stamp(Start, Omega, operation.OperationId), CurrentWrite = Stamp(Start, Alpha, OperationId(FirstOperationSeed)) });

        var result = await resolver.ResolveAsync(context, CancellationToken.None);

        await Assert.That(result.AcceptedOperations).Contains(operation.OperationId);
        await Assert.That(result.RejectedOperations).IsEmpty();
        await Assert.That(result.Conflicts[0].ResolutionCode).IsEqualTo("lww.accepted");
        await Assert.That(result.ServerVersion).IsEqualTo(SecondVersion);
    }

    /// <summary>Verifies LWW accepts stale-base candidates when no current trusted write exists.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task LastWriterWinsResolverAcceptsWhenCurrentWriteIsMissing()
    {
        var resolver = new LastWriterWinsResolver(new() { VersionFactory = new IncrementingVersionFactory() });
        var operation = Operation(FirstOperationSeed, Alpha, "missing", Start);
        var context = new ConflictContext(
            State(InitialVersion, CurrentText),
            [operation],
            ClientIdentity(Alpha),
            new() { CandidateWrite = Stamp(Start, Alpha, operation.OperationId) });

        var result = await resolver.ResolveAsync(context, CancellationToken.None);

        await Assert.That(result.AcceptedOperations).Contains(operation.OperationId);
        await Assert.That(result.ServerVersion).IsEqualTo(FirstVersion);
    }

    /// <summary>Verifies LWW uses operation identifiers as the final deterministic tie-breaker.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task LastWriterWinsResolverUsesOperationIdTieBreaker()
    {
        var resolver = new LastWriterWinsResolver(new() { VersionFactory = new IncrementingVersionFactory() });
        var operation = Operation(SecondOperationSeed, Alpha, InitialVersion, Start);
        var context = new ConflictContext(
            State(FirstVersion, CurrentText),
            [operation],
            ClientIdentity(Alpha),
            new() { CandidateWrite = Stamp(Start, Alpha, operation.OperationId), CurrentWrite = Stamp(Start, Alpha, OperationId(FirstOperationSeed)) });

        var result = await resolver.ResolveAsync(context, CancellationToken.None);

        await Assert.That(result.AcceptedOperations).Contains(operation.OperationId);
        await Assert.That(result.ServerVersion).IsEqualTo(SecondVersion);
    }

    /// <summary>Verifies competing SQLite writes resolve deterministically and survive restart.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ProcessAsyncWithSqliteKeepsCompetingClientLwwWinnerAfterRestart()
    {
        using var database = new SqliteLease();
        var clock = new ManualClock(Start);
        var resolver = new LastWriterWinsResolver(new() { VersionFactory = new IncrementingVersionFactory() });
        using (var journal = database.Open(clock))
        {
            var processor = Processor(journal, Handler(resolver, new PayloadDomainHandler()), clock);
            _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed, Alpha, null, Start.AddYears(1))), ClientIdentity(Alpha), CancellationToken.None);
            _ = await processor.ProcessAsync(Batch(Operation(SecondOperationSeed, Omega, InitialVersion, Start.AddYears(-1))), ClientIdentity(Omega), CancellationToken.None);
        }

        using var reopened = database.Open(clock);
        var snapshot = reopened.Read(StreamKey(), [OperationKey(Omega, SecondOperationSeed)]);

        await Assert.That(snapshot.State?.Version).IsEqualTo(SecondVersion);
        await Assert.That(Text(snapshot.State?.State)).IsEqualTo("operation-2");
        await Assert.That(snapshot.LastWriteStamp?.ClientId).IsEqualTo(Omega);
    }

    /// <summary>Verifies duplicate operation replay after restart does not call resolver or domain handler again.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ProcessAsyncWithSqliteReplaysReceiptWithoutCallingResolverOrDomainHandlerAfterRestart()
    {
        using var database = new SqliteLease();
        var clock = new ManualClock(Start);
        var resolver = new RecordingResolver(Accept);
        var domain = new PayloadDomainHandler();
        var operation = Operation(FirstOperationSeed, Alpha, null, Start);
        ServerSyncResult first;
        using (var journal = database.Open(clock))
        {
            first = await Processor(journal, Handler(resolver, domain), clock).ProcessAsync(Batch(operation), ClientIdentity(Alpha), CancellationToken.None);
        }

        using (var reopened = database.Open(clock))
        {
            var replay = await Processor(reopened, Handler(resolver, domain), clock).ProcessAsync(Batch(operation), ClientIdentity(Alpha), CancellationToken.None);
            await Assert.That(replay.ProducedEvents[0].EventId).IsEqualTo(first.ProducedEvents[0].EventId);
        }

        await Assert.That(resolver.CallCount).IsEqualTo(SingleCount);
        await Assert.That(domain.CallCount).IsEqualTo(SingleCount);
    }

    /// <summary>Verifies stale CAS retries observe the fresh committed write stamp.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ProcessAsyncWithSqliteRetriesStaleCasUsingFreshConflictServerContext()
    {
        using var database = new SqliteLease();
        var clock = new ManualClock(Start.AddMinutes(-1));
        using var journal = database.Open(clock);
        var observed = new List<ConflictWriteStamp?>();
        var resolver = new RecordingResolver(context =>
        {
            observed.Add(context.Server?.CurrentWrite);
            return Accept(context);
        });
        var injecting = new InjectingJournal(journal, InterferingPlan());
        var processor = Processor(injecting, Handler(resolver, new PayloadDomainHandler()), clock);

        _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed, Omega, InitialVersion, Start.AddYears(1))), ClientIdentity(Omega), CancellationToken.None);

        await Assert.That(observed).Count().IsEqualTo(DoubleCount);
        await Assert.That(observed[0]).IsNull();
        await Assert.That(observed[1]?.ClientId).IsEqualTo(Alpha);
        await Assert.That(observed[1]?.CommittedAtUtc).IsEqualTo(Start);
    }

    /// <summary>Verifies custom rejection bypasses domain effects and persists only the terminal row.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConflictResolvingServerOperationHandlerDoesNotCallDomainHandlerForRejectedResolution()
    {
        var journal = new InMemoryServerCommitJournal(JournalOptions(new ManualClock(Start)));
        var resolver = new RecordingResolver(static context => new(
            [],
            [new(context.Incoming[0].OperationId, RejectedReason, false)],
            [],
            [],
            context.Current.Version));
        var domain = new PayloadDomainHandler();
        var processor = Processor(journal, Handler(resolver, domain, ConflictPolicy.Custom), new ManualClock(Start));

        var result = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed, Alpha, null, Start, ConflictPolicy.Custom)), ClientIdentity(Alpha), CancellationToken.None);
        var snapshot = journal.Read(StreamKey(), [OperationKey(Alpha, FirstOperationSeed)]);

        await Assert.That(resolver.CallCount).IsEqualTo(SingleCount);
        await Assert.That(domain.CallCount).IsEqualTo(0);
        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Rejected);
        await Assert.That(result.Result.Operations[0].ReasonCode).IsEqualTo(RejectedReason);
        await Assert.That(snapshot.State).IsNull();
        await Assert.That(snapshot.Entries[0].Events).IsEmpty();
    }

    /// <summary>Verifies rejected LWW decisions retain the current server version for clients.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConflictResolvingServerOperationHandlerPreservesRejectedLwwServerVersion()
    {
        var journal = new InMemoryServerCommitJournal(JournalOptions(new ManualClock(Start)));
        var clock = new ManualClock(Start);
        var resolver = new LastWriterWinsResolver(new() { VersionFactory = new IncrementingVersionFactory() });
        var processor = Processor(journal, Handler(resolver, new PayloadDomainHandler()), clock);

        _ = await processor.ProcessAsync(Batch(Operation(SecondOperationSeed, Omega, null, Start)), ClientIdentity(Omega), CancellationToken.None);
        var result = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed, Alpha, InitialVersion, Start)), ClientIdentity(Alpha), CancellationToken.None);

        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Rejected);
        await Assert.That(result.Result.Operations[0].ReasonCode).IsEqualTo("lww-stale-write");
        await Assert.That(result.Result.Operations[0].ServerVersion).IsEqualTo(FirstVersion);
    }

    /// <summary>Verifies rejected resolver decisions cannot hide durable event proposals.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConflictResolvingServerOperationHandlerRejectsProducedEventsOnRejectedResolution()
    {
        var journal = new InMemoryServerCommitJournal(JournalOptions(new ManualClock(Start)));
        var resolver = new RecordingResolver(static context => new(
            [],
            [new(context.Incoming[0].OperationId, RejectedReason, false)],
            [],
            [ResolverEvent(context.Incoming[0], "rejected-event")],
            context.Current.Version));
        var processor = Processor(journal, Handler(resolver, new PayloadDomainHandler(), ConflictPolicy.Custom), new ManualClock(Start));

        async Task Act() => _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed, Alpha, null, Start, ConflictPolicy.Custom)), ClientIdentity(Alpha), CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
        await Assert.That(journal.Read(StreamKey(), [OperationKey(Alpha, FirstOperationSeed)]).Entries).IsEmpty();
    }

    /// <summary>Verifies resolver-produced events are treated as proposals and server-stamped.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConflictResolvingServerOperationHandlerServerStampsResolverProducedEvents()
    {
        var journal = new InMemoryServerCommitJournal(JournalOptions(new ManualClock(Start)));
        var resolver = new RecordingResolver(static context => new(
            [context.Incoming[0].OperationId],
            [],
            [],
            [new(ResolverEventId, Stream, ResolverEventCursor, Start.AddYears(-1), null, Payload("resolver-event"), new Dictionary<string, string> { ["source"] = "resolver" })],
            FirstVersion));
        var processor = Processor(journal, Handler(resolver, new PayloadDomainHandler(produceEvents: false)), new ManualClock(Start));

        var result = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed, Alpha, null, Start)), ClientIdentity(Alpha), CancellationToken.None);

        await Assert.That(result.ProducedEvents).Count().IsEqualTo(SingleCount);
        await Assert.That(result.ProducedEvents[0].EventId).IsEqualTo(ResolverEventId);
        await Assert.That(result.ProducedEvents[0].ServerCursor).IsNotEqualTo(ResolverEventCursor);
        await Assert.That(result.ProducedEvents[0].CommittedAtUtc).IsEqualTo(Start);
        await Assert.That(result.ProducedEvents[0].Origin?.ClientId).IsEqualTo(Alpha);
        await Assert.That(Text(result.ProducedEvents[0].Payload)).IsEqualTo("resolver-event");
    }

    /// <summary>Verifies resolver event proposals must target the resolved stream.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConflictResolvingServerOperationHandlerRejectsForeignResolverEventStream()
    {
        var journal = new InMemoryServerCommitJournal(JournalOptions(new ManualClock(Start)));
        var domain = new PayloadDomainHandler();
        var resolver = new RecordingResolver(static context => new(
            [context.Incoming[0].OperationId],
            [],
            [],
            [
                new(
                    Guid.NewGuid(),
                    new(ForeignText),
                    ResolverEventCursor,
                    Start.AddYears(-1),
                    context.Incoming[0].OperationId,
                    Payload(ForeignText),
                    new Dictionary<string, string>()),
            ],
            FirstVersion));
        var processor = Processor(journal, Handler(resolver, domain), new ManualClock(Start));

        async Task Act() =>
            _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed, Alpha, null, Start)), ClientIdentity(Alpha), CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
        await Assert.That(domain.CallCount).IsEqualTo(0);
    }

    /// <summary>Verifies combined resolver and domain event proposals are bounded before retention.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConflictResolvingServerOperationHandlerBoundsProducedEventsBeforeCopy()
    {
        var journal = new InMemoryServerCommitJournal(JournalOptions(new ManualClock(Start)));
        var resolver = new RecordingResolver(static context => new(
            [context.Incoming[0].OperationId],
            [],
            [],
            [ResolverEvent(context.Incoming[0], "resolver")],
            FirstVersion));
        var options = new ServerConflictHandlerOptions { MaximumProducedEvents = 1, Streams = [Registration(resolver, new PayloadDomainHandler())] };
        var processor = Processor(journal, new ConflictResolvingServerOperationHandler(options), new ManualClock(Start));

        async Task Act() => _ = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed, Alpha, null, Start)), ClientIdentity(Alpha), CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(journal.Read(StreamKey(), [OperationKey(Alpha, FirstOperationSeed)]).Entries).IsEmpty();
    }

    /// <summary>Verifies handler options reject empty registrations.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ServerConflictHandlerOptionsRejectsEmptyStreams()
    {
        var options = new ServerConflictHandlerOptions { Streams = [] };

        Task Act()
        {
            options.Validate();
            return Task.CompletedTask;
        }

        await Assert.That(Act).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies handler options reject duplicate stream registrations.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ServerConflictHandlerOptionsRejectsDuplicateStreams()
    {
        var resolver = new RecordingResolver(Accept);
        var domain = new PayloadDomainHandler();
        var options = new ServerConflictHandlerOptions { Streams = [Registration(resolver, domain), Registration(resolver, domain)] };

        Task Act()
        {
            options.Validate();
            return Task.CompletedTask;
        }

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies stream registrations reject uninitialized stream identifiers.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ServerConflictStreamRegistrationRejectsUninitializedStreamId()
    {
        var resolver = new RecordingResolver(Accept);
        var registration = Registration(resolver, new PayloadDomainHandler()) with { StreamId = default };

        Task Act()
        {
            registration.Validate();
            return Task.CompletedTask;
        }

        await Assert.That(Act).ThrowsExactly<ArgumentException>();
    }

    /// <summary>Verifies metadata dictionaries are copied before server event proposals are retained.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ServerProducedEventCopiesMetadata()
    {
        var metadata = new Dictionary<string, string> { ["one"] = "two" };
        var proposal = new ServerProducedEvent { EventId = ResolverEventId, Payload = Payload(MetadataText), Metadata = metadata };
        metadata["one"] = "changed";

        await Assert.That(proposal.Metadata["one"]).IsEqualTo("two");
    }

    /// <summary>Verifies empty metadata dictionaries reuse the immutable empty instance.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ServerProducedEventCopiesEmptyMetadata()
    {
        Dictionary<string, string> metadata = [];
        var proposal = new ServerProducedEvent { EventId = ResolverEventId, Payload = Payload(MetadataText), Metadata = metadata };
        await Assert.That(proposal.Metadata).IsEmpty();
    }

    /// <summary>Verifies retained server collections reject negative counts before allocation.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ServerCollectionCopyRejectsNegativeCountsBeforeCopy()
    {
        Task Act()
        {
            _ = new ServerConflictHandlerOptions { Streams = new NegativeRegistrationList(NegativeRetainedCount) };
            return Task.CompletedTask;
        }

        await Assert.That(Act).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies retained server dictionaries reject oversized counts before copying entries.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ServerCollectionCopyRejectsOversizedDictionaryCountsBeforeCopy()
    {
        Task Act()
        {
            _ = new ServerProducedEvent { EventId = ResolverEventId, Payload = Payload(MetadataText), Metadata = new OversizedStringDictionary(OversizedDictionaryCount) };
            return Task.CompletedTask;
        }

        await Assert.That(Act).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies merge policy routes null, matching, and stale base versions to the configured resolver.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ConflictResolvingServerOperationHandlerRoutesMergeForEveryBaseVersion() =>
        AssertResolverRoutesAllBaseVersionsAsync(ConflictPolicy.Merge);

    /// <summary>Verifies custom policy routes null, matching, and stale base versions to the configured resolver.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task ConflictResolvingServerOperationHandlerRoutesCustomForEveryBaseVersion() =>
        AssertResolverRoutesAllBaseVersionsAsync(ConflictPolicy.Custom);

    /// <summary>Verifies unregistered streams reject without invoking configured stream behavior.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConflictResolvingServerOperationHandlerRejectsUnregisteredStream()
    {
        var resolver = new RecordingResolver(Accept);
        var handler = Handler(resolver, new PayloadDomainHandler());
        var operation = Operation(FirstOperationSeed, Alpha, null, Start) with { StreamId = new("missing") };

        var preparation = await handler.PrepareAsync(PrepareContext(operation), CancellationToken.None);

        await Assert.That(preparation.Result.Kind).IsEqualTo(OperationResultKind.Rejected);
        await Assert.That(preparation.Result.ReasonCode).IsEqualTo("unregistered-stream");
        await Assert.That(resolver.CallCount).IsEqualTo(0);
    }

    /// <summary>Verifies malformed resolver decisions fail closed.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConflictResolvingServerOperationHandlerRejectsUndecidedResolution()
    {
        var resolver = new RecordingResolver(static context => new([], [], [], [], context.Current.Version));
        var handler = Handler(resolver, new PayloadDomainHandler());

        async Task Act() => _ = await handler.PrepareAsync(PrepareContext(Operation(FirstOperationSeed, Alpha, null, Start)), CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies accepted resolver decisions cannot name a foreign operation.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConflictResolvingServerOperationHandlerRejectsForeignAcceptedOperation()
    {
        var resolver = new RecordingResolver(static context => new([OperationId(SecondOperationSeed)], [], [], [], context.Current.Version));
        var handler = Handler(resolver, new PayloadDomainHandler());

        async Task Act() => _ = await handler.PrepareAsync(PrepareContext(Operation(FirstOperationSeed, Alpha, null, Start)), CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies duplicate resolver rejections fail closed.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConflictResolvingServerOperationHandlerRejectsDuplicateRejectedOperation()
    {
        var resolver = new RecordingResolver(static context =>
        {
            var rejected = new RejectedOperation(context.Incoming[0].OperationId, RejectedReason, false);
            return new([], [rejected, rejected], [], [], context.Current.Version);
        });
        var handler = Handler(resolver, new PayloadDomainHandler());

        async Task Act() => _ = await handler.PrepareAsync(PrepareContext(Operation(FirstOperationSeed, Alpha, null, Start)), CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies resolver conflict effects must belong to the resolved operation.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConflictResolvingServerOperationHandlerRejectsForeignConflictEffect()
    {
        var resolver = new RecordingResolver(static context => new(
            [context.Incoming[0].OperationId],
            [],
            [new(OperationId(SecondOperationSeed), "foreign-conflict", null)],
            [],
            context.Current.Version));
        var handler = Handler(resolver, new PayloadDomainHandler());

        async Task Act() => _ = await handler.PrepareAsync(PrepareContext(Operation(FirstOperationSeed, Alpha, null, Start)), CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies resolver event causes must belong to the resolved operation.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConflictResolvingServerOperationHandlerRejectsForeignResolverEventCause()
    {
        var resolver = new RecordingResolver(static context => new(
            [context.Incoming[0].OperationId],
            [],
            [],
            [new(Guid.NewGuid(), Stream, ResolverEventCursor, Start, OperationId(SecondOperationSeed), Payload("foreign-cause"), new Dictionary<string, string>())],
            context.Current.Version));
        var handler = Handler(resolver, new PayloadDomainHandler());

        async Task Act() => _ = await handler.PrepareAsync(PrepareContext(Operation(FirstOperationSeed, Alpha, null, Start)), CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies resolver-produced event counts are bounded before retained conversion.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConflictResolvingServerOperationHandlerBoundsResolverProducedEventsBeforeCopy()
    {
        var resolver = new RecordingResolver(static context => new(
            [context.Incoming[0].OperationId],
            [],
            [],
            [ResolverEvent(context.Incoming[0], "first"), ResolverEvent(context.Incoming[0], "second")],
            context.Current.Version));
        var options = new ServerConflictHandlerOptions { MaximumProducedEvents = 1, Streams = [Registration(resolver, new PayloadDomainHandler(false))] };
        var handler = new ConflictResolvingServerOperationHandler(options);

        async Task Act() => _ = await handler.PrepareAsync(PrepareContext(Operation(FirstOperationSeed, Alpha, null, Start)), CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<ArgumentOutOfRangeException>();
    }

    /// <summary>Verifies domain results must stay on the operation stream.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConflictResolvingServerOperationHandlerRejectsForeignDomainState()
    {
        var domain = new DelegatingDomainHandler(static context => new() { NewState = new(new(ForeignText), context.Resolution.ServerVersion, context.Operation.Payload) });
        var handler = Handler(new RecordingResolver(Accept), domain);

        async Task Act() => _ = await handler.PrepareAsync(PrepareContext(Operation(FirstOperationSeed, Alpha, null, Start)), CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies domain results must use the resolver-provided server version.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConflictResolvingServerOperationHandlerRejectsDomainVersionOverride()
    {
        var domain = new DelegatingDomainHandler(static context => new() { NewState = new(context.Operation.StreamId, SecondVersion, context.Operation.Payload) });
        var handler = Handler(new RecordingResolver(Accept), domain);

        async Task Act() => _ = await handler.PrepareAsync(PrepareContext(Operation(FirstOperationSeed, Alpha, null, Start)), CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies initial state factories must produce the requested stream.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConflictResolvingServerOperationHandlerRejectsForeignInitialState()
    {
        var resolver = new RecordingResolver(Accept);
        var registration = Registration(resolver, new PayloadDomainHandler()) with { InitialStateFactory = new ForeignInitialStateFactory() };
        var handler = new ConflictResolvingServerOperationHandler(new() { Streams = [registration] });

        async Task Act() => _ = await handler.PrepareAsync(PrepareContext(Operation(FirstOperationSeed, Alpha, null, Start)), CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies unsupported conflict policies fail closed when called through the internal handler.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConflictResolvingServerOperationHandlerRejectsUnsupportedConflictPolicy()
    {
        var handler = Handler(new RecordingResolver(Accept), new PayloadDomainHandler());
        var operation = Operation(FirstOperationSeed, Alpha, null, Start) with
        {
            Policy = new(DeliveryGuarantee.AtLeastOnce, OperationDurability.Durable, 0, UnsupportedConflictPolicy),
        };

        async Task Act() => _ = await handler.PrepareAsync(PrepareContext(operation), CancellationToken.None);

        await Assert.That(Act).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies handler options own the registration collection supplied by callers.</summary>
    /// <returns>The asynchronous assertion operation.</returns>
    [Test]
    public async Task ConflictResolvingServerOperationHandlerDefensivelyCapturesRegistrations()
    {
        var registrations = new List<ServerConflictStreamRegistration> { Registration(new RecordingResolver(Accept), new PayloadDomainHandler()) };
        var options = new ServerConflictHandlerOptions { Streams = registrations };
        registrations.Clear();
        var journal = new InMemoryServerCommitJournal(JournalOptions(new ManualClock(Start)));
        var processor = Processor(journal, new ConflictResolvingServerOperationHandler(options), new ManualClock(Start));

        var result = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed, Alpha, null, Start)), ClientIdentity(Alpha), CancellationToken.None);

        await Assert.That(result.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
    }

    /// <summary>Creates a processor over the supplied journal and operation handler.</summary>
    /// <param name="journal">The commit journal.</param>
    /// <param name="handler">The operation handler.</param>
    /// <param name="clock">The server clock.</param>
    /// <returns>The operation processor.</returns>
    private static ServerOperationProcessor Processor(
        IServerCommitJournal journal,
        IServerOperationHandler handler,
        TimeProvider clock) =>
        new(journal, new RecordingAuthorizer(), handler, options: ProcessorOptions(clock));

    /// <summary>Creates a conflict-resolving operation handler for one policy.</summary>
    /// <param name="resolver">The resolver selected for the policy.</param>
    /// <param name="domainHandler">The domain handler.</param>
    /// <param name="policy">The operation conflict policy.</param>
    /// <returns>The configured operation handler.</returns>
    private static ConflictResolvingServerOperationHandler Handler(
        IConflictResolver resolver,
        IServerDomainHandler domainHandler,
        ConflictPolicy policy = ConflictPolicy.LastWriterWins) =>
        new(new() { Streams = [Registration(resolver, domainHandler, policy)] });

    /// <summary>Creates a stream registration for one selected resolver.</summary>
    /// <param name="resolver">The selected resolver.</param>
    /// <param name="domainHandler">The domain handler.</param>
    /// <param name="policy">The policy that selects <paramref name="resolver"/>.</param>
    /// <returns>The stream registration.</returns>
    private static ServerConflictStreamRegistration Registration(
        IConflictResolver resolver,
        IServerDomainHandler domainHandler,
        ConflictPolicy policy = ConflictPolicy.LastWriterWins)
    {
        var reject = new RecordingResolver(static context => new(
            [],
            [new(context.Incoming[0].OperationId, "unused-policy", false)],
            [],
            [],
            context.Current.Version));
        return new()
        {
            StreamId = Stream,
            InitialStateFactory = new InitialStateFactory(),
            LastWriterWinsResolver = policy == ConflictPolicy.LastWriterWins ? resolver : reject,
            MergeResolver = policy == ConflictPolicy.Merge ? resolver : reject,
            CustomResolver = policy == ConflictPolicy.Custom ? resolver : reject,
            DomainHandler = domainHandler,
        };
    }

    /// <summary>Creates an accepted conflict result for the first operation.</summary>
    /// <param name="context">The conflict context.</param>
    /// <returns>The accepted result.</returns>
    private static ConflictResolutionResult Accept(ConflictContext context) =>
        new(
            [context.Incoming[0].OperationId],
            [],
            [],
            [],
            FirstVersion);

    /// <summary>Creates bounded processor options.</summary>
    /// <param name="clock">The server clock.</param>
    /// <returns>The processor options.</returns>
    private static ServerOperationProcessorOptions ProcessorOptions(TimeProvider clock) =>
        new()
        {
            MaximumBatchOperations = ProcessorMaximumBatchOperations,
            MaximumBatchLogicalBytes = LogicalByteBudget,
            MaximumPreparedConflicts = ProcessorMaximumBatchOperations,
            MaximumPreparedEvents = ProcessorMaximumBatchOperations,
            MaximumPreparedLogicalBytes = LogicalByteBudget,
            MaximumActiveRequests = ProcessorMaximumBatchOperations,
            MaximumCommitAttempts = MaximumCommitAttempts,
            TimeProvider = clock,
        };

    /// <summary>Creates bounded journal options.</summary>
    /// <param name="clock">The journal clock.</param>
    /// <returns>The journal options.</returns>
    private static ServerCommitJournalOptions JournalOptions(TimeProvider clock) =>
        new()
        {
            MaximumStreams = JournalMaximumStreams,
            MaximumLedgerEntries = JournalMaximumRows,
            MaximumEvents = JournalMaximumRows,
            MaximumLogicalBytes = LogicalByteBudget,
            OperationRetention = TimeSpan.FromMinutes(RetentionMinutes),
            TimeProvider = clock,
        };

    /// <summary>Creates a competing commit plan that wins the first CAS attempt.</summary>
    /// <returns>The injected commit plan.</returns>
    private static ServerCommitPlan InterferingPlan()
    {
        var key = OperationKey(Alpha, InterferingSeed);
        var remoteEvent = new RemoteEvent(
            Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
            Stream,
            InterferingText,
            Start,
            key.OperationId,
            Payload(InterferingText),
            new Dictionary<string, string>())
        { Origin = new(Alpha, key.OperationId) };
        return new(
            StreamKey(),
            0,
            State(FirstVersion, InterferingText),
            new(Start, Alpha, key.OperationId),
            [new(key, Fingerprint(key.ClientId, key.OperationId), new(key.OperationId, OperationResultKind.Accepted, null, FirstVersion), [], [remoteEvent])]);
    }

    /// <summary>Creates a canonical operation fingerprint for a test operation.</summary>
    /// <param name="clientId">The authenticated client identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The commit fingerprint.</returns>
    private static ServerCommitFingerprint Fingerprint(string clientId, OperationId operationId) =>
        new(CanonicalOperationFingerprint.Compute(Tenant, clientId, OperationForFingerprint(operationId), FingerprintBudget));

    /// <summary>Creates an operation used only for synthetic fingerprints.</summary>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The synthetic operation.</returns>
    private static SyncOperation OperationForFingerprint(OperationId operationId) =>
        Operation(FirstOperationSeed, Alpha, InitialVersion, Start) with { OperationId = operationId };

    /// <summary>Creates a synchronization batch containing one operation.</summary>
    /// <param name="operation">The operation.</param>
    /// <returns>The synchronization batch.</returns>
    private static SyncBatch Batch(SyncOperation operation) =>
        new(Guid.Parse("dddddddd-dddd-dddd-dddd-dddddddddddd"), [operation]);

    /// <summary>Creates a synchronization operation.</summary>
    /// <param name="seed">The deterministic operation seed.</param>
    /// <param name="clientId">The client identifier.</param>
    /// <param name="baseVersion">The candidate base version.</param>
    /// <param name="timestampUtc">The client-supplied timestamp.</param>
    /// <param name="conflictPolicy">The conflict policy.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation Operation(
        int seed,
        string clientId,
        string? baseVersion,
        DateTimeOffset timestampUtc,
        ConflictPolicy conflictPolicy = ConflictPolicy.LastWriterWins) =>
        new()
        {
            OperationId = OperationId(seed),
            StreamId = Stream,
            ClientSequence = seed,
            TimestampUtc = timestampUtc,
            BaseVersion = baseVersion,
            Type = SyncOperationType.Update,
            Payload = Payload($"operation-{seed}"),
            Policy = new(DeliveryGuarantee.AtLeastOnce, OperationDurability.Durable, 0, conflictPolicy),
        };

    /// <summary>Creates a payload envelope containing UTF-8 text.</summary>
    /// <param name="text">The payload text.</param>
    /// <returns>The payload envelope.</returns>
    private static PayloadEnvelope Payload(string text) =>
        new(Contract, 1, ContentType, Encoding.UTF8.GetBytes(text), text);

    /// <summary>Asserts the handler does not bypass a selected resolver for any base-version shape.</summary>
    /// <param name="policy">The resolver policy under test.</param>
    /// <returns>The asynchronous assertion operation.</returns>
    private static async Task AssertResolverRoutesAllBaseVersionsAsync(ConflictPolicy policy)
    {
        var journal = new InMemoryServerCommitJournal(JournalOptions(new ManualClock(Start)));
        var resolver = new RecordingResolver(Accept);
        var domain = new PayloadDomainHandler(produceEvents: false);
        var processor = Processor(journal, Handler(resolver, domain, policy), new ManualClock(Start));

        var nullBase = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed, Alpha, null, Start, policy)), ClientIdentity(Alpha), CancellationToken.None);
        var matchingBase = await processor.ProcessAsync(Batch(Operation(SecondOperationSeed, Alpha, FirstVersion, Start, policy)), ClientIdentity(Alpha), CancellationToken.None);
        var staleBase = await processor.ProcessAsync(Batch(Operation(ThirdOperationSeed, Alpha, InitialVersion, Start, policy)), ClientIdentity(Alpha), CancellationToken.None);

        await Assert.That(resolver.CallCount).IsEqualTo(ThirdOperationSeed);
        await Assert.That(domain.CallCount).IsEqualTo(ThirdOperationSeed);
        await Assert.That(nullBase.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(matchingBase.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
        await Assert.That(staleBase.Result.Operations[0].Kind).IsEqualTo(OperationResultKind.Accepted);
    }

    /// <summary>Creates an internal operation preparation context.</summary>
    /// <param name="operation">The operation.</param>
    /// <returns>The preparation context.</returns>
    private static ServerOperationContext PrepareContext(SyncOperation operation)
    {
        var streamKey = new ServerStreamKey(Tenant, operation.StreamId);
        var operationKey = new ServerOperationKey(Alpha, operation.OperationId);
        var candidateWrite = new ServerWriteStamp(Start, Alpha, operation.OperationId);
        return new(
            ClientIdentity(Alpha),
            operation,
            new(Tenant, Alpha),
            streamKey,
            operationKey,
            new(streamKey, 0, null, null, [], null, 0),
            candidateWrite);
    }

    /// <summary>Creates a resolver-side remote event proposal.</summary>
    /// <param name="operation">The source operation.</param>
    /// <param name="text">The event payload text.</param>
    /// <returns>The event proposal.</returns>
    private static RemoteEvent ResolverEvent(SyncOperation operation, string text) =>
        new(Guid.NewGuid(), operation.StreamId, ResolverEventCursor, Start.AddYears(-1), operation.OperationId, Payload(text), new Dictionary<string, string>());

    /// <summary>Reads UTF-8 text from a payload envelope.</summary>
    /// <param name="payload">The payload envelope.</param>
    /// <returns>The payload text.</returns>
    private static string? Text(PayloadEnvelope? payload) =>
        payload is null ? null : Encoding.UTF8.GetString(payload.Payload.ToArray());

    /// <summary>Creates a server state for the default stream.</summary>
    /// <param name="version">The state version.</param>
    /// <param name="text">The state text.</param>
    /// <returns>The server state.</returns>
    private static ServerState State(string version, string text) =>
        new(Stream, version, Payload(text));

    /// <summary>Creates a public conflict write stamp.</summary>
    /// <param name="committedAtUtc">The server commit timestamp.</param>
    /// <param name="clientId">The authenticated client identifier.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>The write stamp.</returns>
    private static ConflictWriteStamp Stamp(DateTimeOffset committedAtUtc, string clientId, OperationId operationId) =>
        new() { CommittedAtUtc = committedAtUtc, ClientId = clientId, OperationId = operationId };

    /// <summary>Creates a client identity.</summary>
    /// <param name="clientId">The client identifier.</param>
    /// <returns>The client identity.</returns>
    private static ClientIdentity ClientIdentity(string clientId) => new(clientId, Tenant);

    /// <summary>Creates the default server stream key.</summary>
    /// <returns>The server stream key.</returns>
    private static ServerStreamKey StreamKey() => new(Tenant, Stream);

    /// <summary>Creates an authenticated operation key.</summary>
    /// <param name="clientId">The client identifier.</param>
    /// <param name="seed">The operation seed.</param>
    /// <returns>The operation key.</returns>
    private static ServerOperationKey OperationKey(string clientId, int seed) => new(clientId, OperationId(seed));

    /// <summary>Creates a deterministic operation identifier.</summary>
    /// <param name="seed">The operation seed.</param>
    /// <returns>The operation identifier.</returns>
    private static OperationId OperationId(int seed) => new(new Guid(seed, 0, 0, [0, 0, 0, 0, 0, 0, 0, 1]));
}
