// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;
using ReactiveUI.Primitives.OccasionallyConnected.Crdt;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;
using static ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab.DurableHttpLostAckProofEvaluator;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Runs the durable HTTP lost acknowledgement recovery scenario.</summary>
internal static partial class DurableHttpLostAckScenario
{
    /// <summary>The runner scenario name.</summary>
    internal const string ScenarioName = "durable-http-lost-ack";

    /// <summary>The small collection and batch capacity used by the lab.</summary>
    private const int SmallCapacity = 4;

    /// <summary>The journal and dot capacity used by the lab.</summary>
    private const int JournalCapacity = 8;

    /// <summary>The maximum logical payload size in bytes.</summary>
    private const int PayloadBytes = 8192;

    /// <summary>The maximum retained journal size in bytes.</summary>
    private const int JournalBytes = 32_768;

    /// <summary>The maximum register or element size in bytes.</summary>
    private const int ElementBytes = 256;

    /// <summary>The maximum encoded counter payload size in bytes.</summary>
    private const int CounterEncodedBytes = 4096;

    /// <summary>The maximum retained typed input size in bytes.</summary>
    private const int TypedInputBytes = 1024;

    /// <summary>The number of minutes used for lab retry and retention limits.</summary>
    private const int RetentionMinutes = 5;

    /// <summary>The maximum retry attempts allowed in this bounded lab.</summary>
    private const int RetryAttempts = 6;

    /// <summary>The number of independent streams accepted by the server journal.</summary>
    private const int JournalStreams = 2;

    /// <summary>The minimum push requests proving one retry after the lost ACK.</summary>
    private const int MinimumPushRequests = 2;

    /// <summary>The polling interval in milliseconds for observable and store proofs.</summary>
    private const int ProofPollMilliseconds = 25;

    /// <summary>The whole-scenario timeout in seconds.</summary>
    private const int ScenarioTimeoutSeconds = 40;

    /// <summary>The deterministic tenant accepted by the lab server policy.</summary>
    private const string TenantId = "tenant-resilience-lab";

    /// <summary>The durable writer identity bound to the reopened SQLite store.</summary>
    private const string WriterClientId = "client-lost-ack-writer";

    /// <summary>The independent observer identity.</summary>
    private const string ObserverClientId = "client-lost-ack-observer";

    /// <summary>The first writer context lab credential.</summary>
    private const string FirstCredential = "lost-ack-first-context";

    /// <summary>The reopened writer context lab credential.</summary>
    private const string ReopenedCredential = "lost-ack-reopened-context";

    /// <summary>The independent observer lab credential.</summary>
    private const string ObserverCredential = "lost-ack-observer-context";

    /// <summary>The stable writer store partition used across reopen.</summary>
    private const string WriterStoreIdentity = "resilience-lab-lost-ack-writer-store";

    /// <summary>The independent observer store partition.</summary>
    private const string ObserverStoreIdentity = "resilience-lab-lost-ack-observer-store";

    /// <summary>The bounded asynchronous operation timeout.</summary>
    private static readonly TimeSpan OperationTimeout = TimeSpan.FromSeconds(10);

    /// <summary>The deterministic retry delay requested through public retry options.</summary>
    private static readonly TimeSpan RetryDelay = TimeSpan.FromMilliseconds(100);

    /// <summary>The tested stream.</summary>
    private static readonly StreamId Stream = new("resilience/lost-ack/gcounter");

    /// <summary>The writer durable subscription.</summary>
    private static readonly SubscriptionId WriterSubscription = new(new("D2939DC6-565C-4EE5-9B71-CF15C809210B"));

    /// <summary>The observer durable subscription.</summary>
    private static readonly SubscriptionId ObserverSubscription = new(new("CC82AE70-132E-4232-8809-678D9D6DF36B"));

    /// <summary>The deterministic first operation identifier.</summary>
    private static readonly OperationId OriginalOperationId = new(new("0C9E9F98-72F5-4A7B-B8D9-0890DE91C7F6"));

    /// <summary>Runs the scenario.</summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The scenario proof cases.</returns>
    /// <exception cref="InvalidOperationException">Scenario cleanup fails.</exception>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    internal static ValueTask<IReadOnlyList<ResilienceLabCaseResult>> RunAsync(CancellationToken cancellationToken) =>
        RunWithRootFactoryAsync(CreateTemporaryRoot, cancellationToken);

    /// <summary>Runs the scenario with an owned temporary-directory allocator.</summary>
    /// <param name="createRoot">The directory allocator.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The scenario proof cases.</returns>
    /// <exception cref="InvalidOperationException">Scenario cleanup fails.</exception>
    internal static async ValueTask<IReadOnlyList<ResilienceLabCaseResult>> RunWithRootFactoryAsync(
        Func<string> createRoot,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(ScenarioTimeoutSeconds));
        var root = createRoot();
        var clock = new MutableTimeProvider(DateTimeOffset.UnixEpoch.AddDays(1));
        DurableHttpLostAckHost? host = null;
        IReadOnlyList<ResilienceLabCaseResult> result = [];
        Exception? primaryFailure = null;

        try
        {
            var writerStorePath = Path.Combine(root, "writer.sqlite");
            var observerStorePath = Path.Combine(root, "observer.sqlite");
            var serverStorePath = Path.Combine(root, "server.sqlite");
            host = await DurableHttpLostAckHost.StartAsync(serverStorePath, clock, timeout.Token).ConfigureAwait(false);
            result = BuildCases(await RunWorkflowAsync(
                writerStorePath,
                observerStorePath,
                serverStorePath,
                host,
                clock,
                timeout.Token).ConfigureAwait(false));
        }
        catch (Exception exception)
        {
            primaryFailure = exception;
        }

        var cleanupFailure = await CleanupRunAsync(host, root).ConfigureAwait(false);
        ThrowIfRunFailed(primaryFailure, cleanupFailure);
        if (cleanupFailure is not null)
        {
            throw new InvalidOperationException("Durable HTTP lost-ACK scenario cleanup failed.", cleanupFailure);
        }

        return result;
    }

    /// <summary>Throws the correct preserved run failure.</summary>
    /// <param name="primaryFailure">The primary scenario failure.</param>
    /// <param name="cleanupFailure">The cleanup failure.</param>
    /// <exception cref="AggregateException">Both the scenario and cleanup fail.</exception>
    internal static void ThrowIfRunFailed(Exception? primaryFailure, Exception? cleanupFailure)
    {
        if (primaryFailure is null)
        {
            return;
        }

        if (cleanupFailure is not null)
        {
            throw new AggregateException(primaryFailure, cleanupFailure);
        }

        ExceptionDispatchInfo.Capture(primaryFailure).Throw();
    }

    /// <summary>Runs asynchronous run cleanup while retaining each cleanup failure.</summary>
    /// <param name="existing">The existing cleanup failure.</param>
    /// <param name="cleanup">The cleanup action.</param>
    /// <returns>The cleanup failure aggregate, or null.</returns>
    internal static async ValueTask<Exception?> CaptureRunCleanupFailureAsync(Exception? existing, Func<Task> cleanup)
    {
        try
        {
            await cleanup().ConfigureAwait(false);
            return existing;
        }
        catch (Exception exception)
        {
            return existing is null ? exception : new AggregateException(existing, exception);
        }
    }

    /// <summary>Runs synchronous run cleanup while retaining each cleanup failure.</summary>
    /// <param name="existing">The existing cleanup failure.</param>
    /// <param name="cleanup">The cleanup action.</param>
    /// <returns>The cleanup failure aggregate, or null.</returns>
    internal static Exception? CaptureRunCleanupFailure(Exception? existing, Action cleanup)
    {
        try
        {
            cleanup();
            return existing;
        }
        catch (Exception exception)
        {
            return existing is null ? exception : new AggregateException(existing, exception);
        }
    }

    /// <summary>Runs the writer crash/reopen workflow and an independent observer client.</summary>
    /// <param name="writerStorePath">The writer store path.</param>
    /// <param name="observerStorePath">The observer store path.</param>
    /// <param name="serverStorePath">The server store path.</param>
    /// <param name="host">The host.</param>
    /// <param name="clock">The clock.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The result.</returns>
    private static async ValueTask<LostAckProof> RunWorkflowAsync(
        string writerStorePath,
        string observerStorePath,
        string serverStorePath,
        DurableHttpLostAckHost host,
        MutableTimeProvider clock,
        CancellationToken cancellationToken)
    {
        await using var observer = await StartObserverAsync(observerStorePath, host, clock, cancellationToken).ConfigureAwait(false);
        var first = await PublishAndLoseFirstAckAsync(writerStorePath, serverStorePath, host, clock, cancellationToken).ConfigureAwait(false);
        var afterFirstClose = await ReadWriterStoreProofAsync(writerStorePath, clock, first.Receipt.OperationId, cancellationToken).ConfigureAwait(false);
        first = first with { BeforeRestart = afterFirstClose };
        var second = await ReopenAndRetryAsync(writerStorePath, host, clock, first, cancellationToken).ConfigureAwait(false);
        var observerCounter = await WaitForObserverConvergenceAsync(observer, cancellationToken).ConfigureAwait(false);
        await observer.Context.StopAsync(cancellationToken).ConfigureAwait(false);
        var observerProof = new ObserverProof(
            observer.ConnectedStream.SubscriptionId,
            observer.Telemetry.RemoteCount,
            observerCounter,
            observer.Telemetry.TerminalFaultCount);
        var writerFinal = await ReadWriterStoreProofAsync(writerStorePath, clock, first.Receipt.OperationId, cancellationToken).ConfigureAwait(false);
        var observerFinal = await ReadInitializedClientStoreProofAsync(
            observer.Store,
            observerStorePath,
            ObserverStoreIdentity,
            ObserverSubscription,
            operationId: null,
            cancellationToken: cancellationToken).ConfigureAwait(false);
        var serverEffectCount = ReadServerEffectCount(serverStorePath, first.Receipt.OperationId);
        return CreateProof(first, second, observerProof, writerFinal, observerFinal, host, serverEffectCount);
    }

    /// <summary>Starts the independent observer and waits until its subscribe request is parked at the host gate.</summary>
    /// <param name="observerStorePath">The observer store path.</param>
    /// <param name="host">The host.</param>
    /// <param name="clock">The clock.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The result.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static ValueTask<ClientSession> StartObserverAsync(
        string observerStorePath,
        DurableHttpLostAckHost host,
        MutableTimeProvider clock,
        CancellationToken cancellationToken) =>
        StartObserverWithResourcesAsync(observerStorePath, host, clock, ClientSessionResourceFactory.Default, cancellationToken);

    /// <summary>Publishes one durable writer operation and drops the first successful server ACK.</summary>
    /// <param name="writerStorePath">The writer store path.</param>
    /// <param name="serverStorePath">The server store path.</param>
    /// <param name="host">The host.</param>
    /// <param name="clock">The clock.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The result.</returns>
    private static async ValueTask<FirstClientProof> PublishAndLoseFirstAckAsync(
        string writerStorePath,
        string serverStorePath,
        DurableHttpLostAckHost host,
        MutableTimeProvider clock,
        CancellationToken cancellationToken)
    {
        await using var session = await CreateClientSessionAsync(
            writerStorePath,
            host.BaseAddress,
            clock,
            new(WriterClientId, WriterStoreIdentity, WriterSubscription, FirstCredential, new FixedOperationIdSource(OriginalOperationId))).ConfigureAwait(false);
        await session.Context.StartAsync(cancellationToken).ConfigureAwait(false);
        var receipt = await session.ConnectedStream.PublishAsync(CreateCounterInput(), CreatePublishOptions(), cancellationToken).ConfigureAwait(false);
        clock.Advance(CreateOptions().Batching.MaximumDwellTime);
        await host.WaitForFirstPushCommittedAsync(cancellationToken).ConfigureAwait(false);
        var serverEffectCountBeforeAckLoss = ReadServerEffectCount(serverStorePath, receipt.OperationId);
        host.ReleaseFirstPushResponse();
        await host.WaitForFirstPushResponseAbortedAsync(cancellationToken).ConfigureAwait(false);
        var pending = await WaitForPendingRetryableAsync(session.Store, writerStorePath, receipt.OperationId, cancellationToken).ConfigureAwait(false);
        await session.Context.StopAsync(cancellationToken).ConfigureAwait(false);

        // Stop may persist the final retry state; sample the still-owned store after that lifecycle boundary.
        var beforeClose = await ReadInitializedClientStoreProofAsync(
            session.Store,
            writerStorePath,
            WriterStoreIdentity,
            WriterSubscription,
            receipt.OperationId,
            cancellationToken).ConfigureAwait(false);
        return new(receipt, session.ConnectedStream.SubscriptionId, beforeClose, pending, serverEffectCountBeforeAckLoss, session.Telemetry.TerminalFaultCount);
    }

    /// <summary>Reopens the durable writer and lets the public retry loop resend persisted work.</summary>
    /// <param name="writerStorePath">The writer store path.</param>
    /// <param name="host">The host.</param>
    /// <param name="clock">The clock.</param>
    /// <param name="first">The first.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The result.</returns>
    private static async ValueTask<SecondClientProof> ReopenAndRetryAsync(
        string writerStorePath,
        DurableHttpLostAckHost host,
        MutableTimeProvider clock,
        FirstClientProof first,
        CancellationToken cancellationToken)
    {
        await using var session = await CreateClientSessionAsync(
            writerStorePath,
            host.BaseAddress,
            clock,
            new(WriterClientId, WriterStoreIdentity, WriterSubscription, ReopenedCredential, ThrowingOperationIdSource.Instance)).ConfigureAwait(false);
        await session.Context.StartAsync(cancellationToken).ConfigureAwait(false);
        await host.WaitForSubscribeObservedAsync(ReopenedCredential, cancellationToken).ConfigureAwait(false);
        AdvanceClockToRetryDeadline(clock, first.BeforeRestart.RetryState);
        var retryOperationId = await host.WaitForRetryPushOperationIdAsync(cancellationToken).ConfigureAwait(false);
        host.ReleaseSubscribeResponses();
        _ = await host.WaitForRetryPushResponseAsync(cancellationToken).ConfigureAwait(false);
        _ = await WaitForWriterDurableSynchronizationAsync(
            session.Store,
            writerStorePath,
            first.BeforeRestart,
            first.Receipt.OperationId,
            session.Telemetry,
            cancellationToken).ConfigureAwait(false);
        var observed = await session.Telemetry.WaitForOperationStateAsync(
            first.Receipt.OperationId,
            SyncOperationState.Synchronized,
            cancellationToken).ConfigureAwait(false);
        await session.Telemetry.WaitForRemoteCountAsync(1, cancellationToken).ConfigureAwait(false);
        _ = await session.Telemetry.WaitForLocalCounterAsync(1, cancellationToken).ConfigureAwait(false);
        await session.Context.StopAsync(cancellationToken).ConfigureAwait(false);
        return new(session.ConnectedStream.SubscriptionId, retryOperationId, observed, session.Telemetry.TerminalFaultCount);
    }

    /// <summary>Waits for observer B to receive the remote effect and converge to counter 1.</summary>
    /// <param name="observer">The observer.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The result.</returns>
    private static async ValueTask<long> WaitForObserverConvergenceAsync(
        ClientSession observer,
        CancellationToken cancellationToken)
    {
        await observer.Telemetry.WaitForRemoteCountAsync(1, cancellationToken).ConfigureAwait(false);
        return await observer.Telemetry.WaitForLocalCounterAsync(1, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Cleans up scenario resources after success or failure.</summary>
    /// <param name="host">The optional host.</param>
    /// <param name="root">The temporary root directory.</param>
    /// <returns>The cleanup failure aggregate, or null.</returns>
    private static async ValueTask<Exception?> CleanupRunAsync(DurableHttpLostAckHost? host, string root)
    {
        Exception? cleanupFailure = null;
        if (host is not null)
        {
            host.ReleaseFirstPushResponse();
            host.ReleaseSubscribeResponses();
            cleanupFailure = await CaptureRunCleanupFailureAsync(cleanupFailure, () => ((IAsyncDisposable)host).DisposeAsync().AsTask()).ConfigureAwait(false);
        }

        cleanupFailure = CaptureRunCleanupFailure(cleanupFailure, () => DeleteDirectory(root));
        return cleanupFailure;
    }

    /// <summary>Disposes a partially started observer while preserving the start failure.</summary>
    /// <param name="session">The observer session.</param>
    /// <param name="startFailure">The start failure.</param>
    /// <returns>The asynchronous operation.</returns>
    /// <exception cref="AggregateException">Both observer start and cleanup fail.</exception>
    private static async ValueTask DisposeFailedObserverAsync(ClientSession session, Exception startFailure)
    {
        try
        {
            await session.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception cleanupFailure)
        {
            throw new AggregateException(startFailure, cleanupFailure);
        }
    }

    /// <summary>Advances to the exact persisted retry due time after the reopened context is ready.</summary>
    /// <param name="clock">The clock.</param>
    /// <param name="retryState">The retry state.</param>
    private static void AdvanceClockToRetryDeadline(MutableTimeProvider clock, RetryState? retryState)
    {
        var now = clock.GetUtcNow();
        var dueUtc = retryState?.DueUtc ?? now;
        var delta = dueUtc > now ? dueUtc - now : TimeSpan.Zero;
        clock.Advance(delta + TimeSpan.FromMilliseconds(1));
    }

    /// <summary>Waits for durable proof that the operation remains pending with a persisted retry due time.</summary>
    /// <param name="store">The active initialized writer store.</param>
    /// <param name="writerStorePath">The writer store path.</param>
    /// <param name="operationId">The operation id.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The result.</returns>
    private static async ValueTask<ClientStoreProof> WaitForPendingRetryableAsync(
        SqliteLocalStoreAdapter store,
        string writerStorePath,
        OperationId operationId,
        CancellationToken cancellationToken) =>
        await WaitForAsync(
            () => ReadInitializedClientStoreProofAsync(store, writerStorePath, WriterStoreIdentity, WriterSubscription, operationId, cancellationToken),
            IsPendingRetryable,
            cancellationToken).ConfigureAwait(false);

    /// <summary>Waits for writer durable cursor, snapshot, and operation synchronization after the retry.</summary>
    /// <param name="store">The active initialized writer store.</param>
    /// <param name="writerStorePath">The writer SQLite database path.</param>
    /// <param name="beforeRestart">The before-restart durable proof.</param>
    /// <param name="operationId">The stable operation id.</param>
    /// <param name="telemetry">The public stream telemetry.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The synchronized durable writer proof.</returns>
    /// <exception cref="InvalidOperationException">The retry did not durably synchronize before the scenario deadline.</exception>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static ValueTask<ClientStoreProof> WaitForWriterDurableSynchronizationAsync(
        SqliteLocalStoreAdapter store,
        string writerStorePath,
        ClientStoreProof beforeRestart,
        OperationId operationId,
        StreamTelemetry telemetry,
        CancellationToken cancellationToken) =>
        WaitForWriterDurableSynchronizationWithReaderAsync(
            token => ReadInitializedClientStoreProofAsync(
                store,
                writerStorePath,
                WriterStoreIdentity,
                WriterSubscription,
                operationId,
                token),
            beforeRestart,
            telemetry,
            cancellationToken);

    /// <summary>Determines whether the writer durable store has synchronized and stored remote progress.</summary>
    /// <param name="beforeRestart">The before-restart durable proof.</param>
    /// <param name="proof">The sampled durable proof.</param>
    /// <returns>True when durable synchronization, cursor progress, and snapshot restore are all visible.</returns>
    private static bool IsDurablySynchronized(ClientStoreProof beforeRestart, ClientStoreProof proof) =>
        proof.OperationStatus is { State: SyncOperationState.Synchronized }
        && proof.PendingCount == 0
        && CursorAdvanced(beforeRestart, proof)
        && proof.SnapshotCounter == 1;

    /// <summary>Creates the CRDT input published by the scenario.</summary>
    /// <returns>The counter input.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static CrdtInput CreateCounterInput() => CrdtInput.ForMutation(CrdtMutation.GCounterSet(WriterClientId, 1));
}
