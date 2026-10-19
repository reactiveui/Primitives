// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.OccasionallyConnected.Mergeable;
using ReactiveUI.Primitives.OccasionallyConnected.Server;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.ResilienceLab;

/// <summary>Demonstrates bounded retries through a public context and durable store.</summary>
internal static partial class RetryBackoffScenario
{
    /// <summary>The command-line scenario name.</summary>
    internal const string ScenarioName = "retry-backoff";

    /// <summary>The trusted client identifier.</summary>
    private const string ClientId = "retry-client";

    /// <summary>The polling interval in milliseconds.</summary>
    private const int PollMilliseconds = 10;

    /// <summary>The original upload plus one configured retry.</summary>
    private const int ExpectedUploads = 2;

    /// <summary>The fixed delay configured through public options.</summary>
    private static readonly TimeSpan RetryDelay = TimeSpan.FromMilliseconds(100);

    /// <summary>The retry hint returned by the application transport.</summary>
    private static readonly TimeSpan ServerHint = TimeSpan.FromMilliseconds(500);

    /// <summary>The counter stream.</summary>
    private static readonly StreamId Stream = new("resilience/retry-backoff");

    /// <summary>Runs the retry scenario using public runtime contracts.</summary>
    /// <param name="timeProvider">The context clock.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The scenario invariants.</returns>
    internal static async ValueTask<IReadOnlyList<ResilienceLabCaseResult>> RunAsync(TimeProvider timeProvider, CancellationToken cancellationToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(ResilienceLabLoopback.GuardTimeout);
        var directory = ResilienceLabContext.CreateTemporaryDirectory("reactiveui-oc-retry");
        try
        {
            return await RunInDirectoryAsync(directory, timeProvider, timeout.Token).ConfigureAwait(false);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    /// <summary>Runs retries over a temporary SQLite database.</summary>
    /// <param name="directory">The SQLite directory.</param>
    /// <param name="timeProvider">The clock.</param>
    /// <param name="cancellationToken">The deadline token.</param>
    /// <returns>The scenario invariants.</returns>
    /// <exception cref="InvalidOperationException">The engine does not persist a retry before the deadline.</exception>
    private static async ValueTask<IReadOnlyList<ResilienceLabCaseResult>> RunInDirectoryAsync(
        string directory,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
    {
        await using var hub = ServerStreamHub.CreateInMemory(ResilienceLabLoopback.CreateHubOptions(
            timeProvider,
            ResilienceLabLoopback.CreateJournalLimits(TimeSpan.FromMinutes(1)),
            Stream));
        await using var store = new SqliteLocalStoreAdapter(Path.Combine(directory, "client.db"));
        var transport = new FailingUploadAdapter(new LoopbackTransportAdapter(ResilienceLabLoopback.CreateLoopbackOptions(
            hub,
            ClientId,
            MergeableLoopbackScenarioShape.VolatileLoopbackCapabilities)));
        var options = new OccasionallyConnectedOptions { Retry = new() { MinimumDelay = RetryDelay, MaximumDelay = RetryDelay, MaximumRetryAttempts = 1 } };
        await using var context = new OccasionallyConnectedBuilder()
            .UseClient(new(ClientId, ResilienceLabLoopback.TenantId))
            .UseBorrowedStore(store)
            .UseTransport(transport)
            .UseSerializer(new MergeablePayloadSerializer(ResilienceLabLoopback.Bounds))
            .UseStoreInitialization(new(ClientId, 1, RequireAuthenticatedEncryptionAtRest: false) { ClientId = ClientId })
            .UseTimeProvider(timeProvider)
            .UseOptions(options)
            .Build();
        var publishOptions = new RemotePublishOptions { StreamId = Stream };
        var definition = ResilienceLabContext.CreateDefinition(Stream, ClientId) with { Publish = publishOptions };
        var stream = context.GetOrCreateStream(definition);
        var faults = new FaultObserver();
        using var faultSubscription = context.SyncEngine.Faults.Subscribe(faults);
        await context.StartAsync(cancellationToken).ConfigureAwait(false);
        var receipt = await stream.PublishAsync(ResilienceLabContext.CreateCounterInput(ClientId, 1), publishOptions, cancellationToken).ConfigureAwait(false);
        RetryState retry;
        try
        {
            retry = await ReadScheduledRetryAsync(store, receipt.OperationId, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException exception)
        {
            var status = await store.GetOperationStatusAsync(receipt.OperationId, CancellationToken.None).ConfigureAwait(false);
            throw new InvalidOperationException($"Retry proof timed out: uploads={transport.UploadCount}; status={status}; faults={faults.Latest}", exception);
        }

        await transport.SecondUpload.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        await Task.Delay(ServerHint + RetryDelay, cancellationToken).ConfigureAwait(false);
        var delay = retry.PreviousDelay ?? TimeSpan.Zero;
        return
        [
            new($"{ScenarioName}.server-hint-applied", ServerHint, delay, delay == ServerHint),
            new($"{ScenarioName}.attempt-persisted", 1, retry.TransientAttemptCount, retry.TransientAttemptCount == 1),
            new($"{ScenarioName}.delay-bounded", true, delay <= ServerHint, delay <= ServerHint),
            new($"{ScenarioName}.attempts-exhausted", ExpectedUploads, transport.UploadCount, transport.UploadCount == ExpectedUploads),
        ];
    }

    /// <summary>Reads retry state after the engine saves its due time.</summary>
    /// <param name="store">The durable store.</param>
    /// <param name="operationId">The published operation.</param>
    /// <param name="cancellationToken">The deadline token.</param>
    /// <returns>The persisted retry state.</returns>
    private static async ValueTask<RetryState> ReadScheduledRetryAsync(
        SqliteLocalStoreAdapter store,
        OperationId operationId,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            var state = await store.GetRetryStateAsync(operationId, cancellationToken).ConfigureAwait(false);
            if (state is { DueUtc: not null })
            {
                return state;
            }

            await Task.Delay(PollMilliseconds, cancellationToken).ConfigureAwait(false);
        }
    }
}
