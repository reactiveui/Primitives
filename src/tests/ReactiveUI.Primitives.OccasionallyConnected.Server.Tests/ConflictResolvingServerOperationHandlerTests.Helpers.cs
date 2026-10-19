// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Text;

namespace ReactiveUI.Primitives.OccasionallyConnected.Server.Tests;

/// <summary>Tests for <see cref="ConflictResolvingServerOperationHandler"/>.</summary>
public sealed partial class ConflictResolvingServerOperationHandlerTests
{
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
        Operation(FirstOperationSeed, InitialVersion, Start) with { OperationId = operationId };

    /// <summary>Creates a synchronization batch containing one operation.</summary>
    /// <param name="operation">The operation.</param>
    /// <returns>The synchronization batch.</returns>
    private static SyncBatch Batch(SyncOperation operation) =>
        new(Guid.Parse("dddddddd-dddd-dddd-dddd-dddddddddddd"), [operation]);

    /// <summary>Creates a synchronization operation.</summary>
    /// <param name="seed">The deterministic operation seed.</param>
    /// <param name="baseVersion">The candidate base version.</param>
    /// <param name="timestampUtc">The client-supplied timestamp.</param>
    /// <param name="conflictPolicy">The conflict policy.</param>
    /// <returns>The operation.</returns>
    private static SyncOperation Operation(
        int seed,
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

        var nullBase = await processor.ProcessAsync(Batch(Operation(FirstOperationSeed, null, Start, policy)), ClientIdentity(Alpha), CancellationToken.None);
        var matchingBase = await processor.ProcessAsync(Batch(Operation(SecondOperationSeed, FirstVersion, Start, policy)), ClientIdentity(Alpha), CancellationToken.None);
        var staleBase = await processor.ProcessAsync(Batch(Operation(ThirdOperationSeed, InitialVersion, Start, policy)), ClientIdentity(Alpha), CancellationToken.None);

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
