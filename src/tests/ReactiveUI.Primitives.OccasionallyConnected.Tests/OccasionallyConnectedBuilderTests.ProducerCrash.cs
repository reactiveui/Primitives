// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <content>Public producer and buffer strategy persistence across abrupt process termination.</content>
public sealed partial class OccasionallyConnectedBuilderTests
{
    /// <summary>The child-process case environment variable.</summary>
    private const string ProducerCrashCaseVariable = "RXUI_OC_PRODUCER_CRASH_CASE";

    /// <summary>The child test filter.</summary>
    private const string ProducerCrashChildFilter = $"/*/*/*/{nameof(ProducerCrashChildPersistsAdmission)}";

    /// <summary>The test assembly loaded in the child.</summary>
    private const string ProducerCrashAssembly = "ReactiveUI.Primitives.OccasionallyConnected.Tests.dll";

    /// <summary>The direct stream publication producer.</summary>
    private const string PublishProducer = "publish";

    /// <summary>The remote observer producer.</summary>
    private const string RemoteProducer = "remote";

    /// <summary>The synchronous stream input producer.</summary>
    private const string InputProducer = "input";

    /// <summary>The child signal poll interval.</summary>
    private const int ProducerCrashPollMilliseconds = 25;

    /// <summary>The maximum wait for one child-side durable admission.</summary>
    private static readonly TimeSpan ProducerCrashAdmissionTimeout = TimeSpan.FromSeconds(15);

    /// <summary>The maximum wait for the child to signal after several admissions.</summary>
    private static readonly TimeSpan ProducerCrashTimeout = TimeSpan.FromSeconds(60);

    /// <summary>Checks each applicable publish strategy after an ungraceful producer process exit.</summary>
    /// <param name="strategy">The outbox strategy.</param>
    /// <returns>A task that completes after SQLite is reopened.</returns>
    [Test]
    [NotInParallel("oc-producer-crash")]
    [Arguments(BufferStrategy.Reject)]
    [Arguments(BufferStrategy.DropNewest)]
    [Arguments(BufferStrategy.DropOldest)]
    [Arguments(BufferStrategy.Block)]
    [Arguments(BufferStrategy.Custom)]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task PublishStrategySurvivesProcessTermination(BufferStrategy strategy) =>
        AssertProducerCrashAsync(PublishProducer, strategy);

    /// <summary>Checks the remote observer's asynchronous publication API after an ungraceful process exit.</summary>
    /// <param name="strategy">The outbox strategy.</param>
    /// <returns>A task that completes after SQLite is reopened.</returns>
    [Test]
    [NotInParallel("oc-producer-crash")]
    [Arguments(BufferStrategy.Reject)]
    [Arguments(BufferStrategy.DropNewest)]
    [Arguments(BufferStrategy.DropOldest)]
    [Arguments(BufferStrategy.Block)]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task RemoteObserverPublishStrategySurvivesProcessTermination(BufferStrategy strategy) =>
        AssertProducerCrashAsync(RemoteProducer, strategy);

    /// <summary>Checks every supported synchronous input strategy after an ungraceful process exit.</summary>
    /// <param name="strategy">The input queue strategy.</param>
    /// <returns>A task that completes after SQLite is reopened.</returns>
    [Test]
    [NotInParallel("oc-producer-crash")]
    [Arguments(BufferStrategy.Reject)]
    [Arguments(BufferStrategy.DropNewest)]
    [Arguments(BufferStrategy.DropOldest)]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Task StreamInputStrategySurvivesProcessTermination(BufferStrategy strategy) =>
        AssertProducerCrashAsync(InputProducer, strategy);

    /// <summary>Runs only inside the child process selected by the parent matrix test.</summary>
    /// <returns>A task that blocks until the child is killed.</returns>
    /// <exception cref="InvalidOperationException">The child case is malformed.</exception>
    [Test]
    public async Task ProducerCrashChildPersistsAdmission()
    {
        var encoded = Environment.GetEnvironmentVariable(ProducerCrashCaseVariable);
        if (encoded is null)
        {
            await Assert.That(encoded).IsNull();
            return;
        }

        var fields = encoded.Split('\n');
        if (fields.Length != 4 || !Enum.TryParse<BufferStrategy>(fields[1], out var strategy))
        {
            throw new InvalidOperationException("The producer crash case is malformed.");
        }

        await using var store = new SqliteLocalStoreAdapter(fields[2]);
        await using var transport = new RecordingTransportAdapter();
        var policy = new ScriptedOverflowPolicy(static context => BufferOverflowDecision.Evict(context.Candidates[^1].OperationId));
        var serializer = new PaddedCounterPayloadSerializer(fields[0] == InputProducer ? GatedInputDelta : int.MinValue);
        var builder = CreatePublicAdmissionBuilder(
            store,
            transport,
            serializer,
            new() { MaxOperations = OverflowOutboxOperations, MaxBytes = PublicAdmissionOutboxBytes }).UseBufferOverflowPolicy(policy);
        await using var context = builder.Build();
        var definition = fields[0] == InputProducer
            ? CreateObserverInputDefinition(strategy, ObserverInputBufferCount)
            : CreateObserverInputDefinition(BufferStrategy.Reject, ObserverInputBufferCount);
        var stream = context.GetOrCreateStream(definition);

        if (fields[0] == InputProducer)
        {
            stream.Input.OnNext(new(GatedInputDelta));
            await serializer.GateEntered.Task.WaitAsync(GuardTimeout);
            stream.Input.OnNext(new(OverflowFourthDelta));
            serializer.ReleaseGate();
            await WaitForProducerCrashCountAsync(store, stream.SubscriptionId, 1);
            await WriteProducerCrashSignalAsync(fields[3], stream.SubscriptionId, null, null);
        }
        else
        {
            await RunCrashPublishProducerAsync(fields[0], strategy, fields[3], context, definition, stream);
        }

        using var never = new ManualResetEventSlim(false);
        never.Wait();
    }

    /// <summary>Runs one asynchronous producer until its durable admission result is observable.</summary>
    /// <param name="producer">The producer path.</param>
    /// <param name="strategy">The outbox strategy.</param>
    /// <param name="signalPath">The child signal path.</param>
    /// <param name="context">The public context.</param>
    /// <param name="definition">The stream definition.</param>
    /// <param name="stream">The public stream.</param>
    /// <returns>A task that completes when the signal is written.</returns>
    private static async Task RunCrashPublishProducerAsync(
        string producer,
        BufferStrategy strategy,
        string signalPath,
        IOccasionallyConnectedContext context,
        StreamDefinition<CounterState, CounterInput> definition,
        IOccasionallyConnectedStream<CounterState, CounterInput> stream)
    {
        var seed = CreatePublicPublishOptions(BufferStrategy.Reject, durable: false);
        var first = await PublishCrashInputAsync(producer, context, definition, stream, 1, seed);
        var second = await PublishCrashInputAsync(producer, context, definition, stream, OverflowSecondDelta, seed);
        var incoming = CreatePublicPublishOptions(strategy, durable: false);
        if (strategy == BufferStrategy.Block)
        {
            using CancellationTokenSource cancellation = new();
            var blocked = PublishCrashInputAsync(producer, context, definition, stream, OverflowThirdDelta, incoming, cancellation.Token);
            await Assert.That(blocked.IsCompleted).IsFalse();
            await cancellation.CancelAsync();
            await Assert.That(async () => await blocked.WaitAsync(GuardTimeout)).Throws<OperationCanceledException>();
        }
        else if (strategy is BufferStrategy.Reject or BufferStrategy.DropNewest)
        {
            await Assert.That(async () => await PublishCrashInputAsync(producer, context, definition, stream, OverflowThirdDelta, incoming))
                .ThrowsExactly<QueueCapacityExceededException>();
        }
        else
        {
            _ = await PublishCrashInputAsync(producer, context, definition, stream, OverflowThirdDelta, incoming);
        }

        await WriteProducerCrashSignalAsync(signalPath, stream.SubscriptionId, first.OperationId, second.OperationId);
    }

    /// <summary>Kills a producer process and checks its reopened public stream.</summary>
    /// <param name="producer">The producer path.</param>
    /// <param name="strategy">The strategy under test.</param>
    /// <returns>A task that completes after recovery assertions.</returns>
    private static async Task AssertProducerCrashAsync(string producer, BufferStrategy strategy)
    {
        var directory = SqliteTestDirectory.Create("oc-producer-crash-");
        try
        {
            var databasePath = Path.Combine(directory.FullName, RecoveredUploadDatabaseFileName);
            var signalPath = Path.Combine(directory.FullName, "ready.signal");
            await KillProducerCrashChildAsync(string.Join('\n', producer, strategy, databasePath, signalPath), signalPath);
            await WaitForProducerCrashOwnerReleaseAsync(databasePath);
            var fields = (await File.ReadAllTextAsync(signalPath)).Split('\n');
            var subscriptionId = new SubscriptionId(Guid.Parse(fields[0]));
            await using var store = new SqliteLocalStoreAdapter(databasePath);
            await store.InitializeAsync(
                new(StoreIdentity, 1, false) { ClientId = ClientId, Outbox = new() { MaxOperations = OverflowOutboxOperations, MaxBytes = PublicAdmissionOutboxBytes } },
                CancellationToken.None);
            await using var transport = new RecordingTransportAdapter();
            await using var context = CreatePublicAdmissionBuilder(
                store,
                transport,
                new PaddedCounterPayloadSerializer(),
                new() { MaxOperations = OverflowOutboxOperations, MaxBytes = PublicAdmissionOutboxBytes }).Build();
            var definition = producer == InputProducer
                ? CreateObserverInputDefinition(strategy, ObserverInputBufferCount)
                : CreateObserverInputDefinition(BufferStrategy.Reject, ObserverInputBufferCount);
            _ = context.GetOrCreateStream(definition);
            var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
            await Assert.That(recovered.SubscriptionId).IsEqualTo(subscriptionId);
            await Assert.That(recovered.ServerCursor).IsNull();
            await Assert.That(recovered.Snapshot?.ServerCursor).IsNull();
            if (producer == InputProducer)
            {
                await AssertCrashInputRecoveryAsync(recovered);
            }
            else
            {
                await AssertCrashPublishRecoveryAsync(store, recovered, strategy, fields);
            }
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    /// <summary>Checks the durable boundary for a synchronous observer input queue.</summary>
    /// <param name="recovered">The reopened stream.</param>
    /// <returns>A task representing the assertions.</returns>
    private static async Task AssertCrashInputRecoveryAsync(RecoveredStream recovered)
    {
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(0);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(recovered.NextClientSequence).IsEqualTo(SecondClientSequence);
        await Assert.That(PaddedCounterPayloadSerializer.Parse(recovered.PendingOperations[0].Payload)).IsEqualTo(GatedInputDelta);
        await Assert.That(recovered.Snapshot is null).IsFalse();
        await Assert.That(PaddedCounterPayloadSerializer.Parse(recovered.Snapshot!.State)).IsEqualTo(GatedInputDelta);
    }

    /// <summary>Checks the durable operation, dead-letter, and snapshot state for an asynchronous producer.</summary>
    /// <param name="store">The reopened SQLite adapter.</param>
    /// <param name="recovered">The reopened stream.</param>
    /// <param name="strategy">The outbox strategy.</param>
    /// <param name="fields">The child operation identifiers.</param>
    /// <returns>A task representing the assertions.</returns>
    private static async Task AssertCrashPublishRecoveryAsync(
        SqliteLocalStoreAdapter store,
        RecoveredStream recovered,
        BufferStrategy strategy,
        string[] fields)
    {
        var firstId = new OperationId(Guid.Parse(fields[1]));
        var secondId = new OperationId(Guid.Parse(fields[2]));
        var evicts = strategy is BufferStrategy.DropOldest or BufferStrategy.Custom;
        var expectedSum = strategy switch
        {
            BufferStrategy.DropOldest => OverflowSumAfterFirstEviction,
            BufferStrategy.Custom => OverflowFourthDelta,
            _ => OverflowThirdDelta,
        };
        await Assert.That(recovered.DeadLetters.Count).IsEqualTo(evicts ? 1 : 0);
        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(OverflowSecondDelta);
        await Assert.That(recovered.NextClientSequence).IsEqualTo(evicts ? OverflowFourthDelta : OverflowThirdDelta);
        await Assert.That(recovered.PendingOperations[0].ClientSequence).IsEqualTo(strategy == BufferStrategy.DropOldest ? SecondClientSequence : 1L);
        await Assert.That(recovered.PendingOperations[1].ClientSequence).IsEqualTo(evicts ? OverflowThirdDelta : SecondClientSequence);
        await Assert.That(PaddedCounterPayloadSerializer.Parse(recovered.PendingOperations[0].Payload))
            .IsEqualTo(strategy == BufferStrategy.DropOldest ? OverflowSecondDelta : 1);
        await Assert.That(PaddedCounterPayloadSerializer.Parse(recovered.PendingOperations[1].Payload))
            .IsEqualTo(evicts ? OverflowThirdDelta : OverflowSecondDelta);
        await Assert.That(recovered.Snapshot is null).IsFalse();
        await Assert.That(PaddedCounterPayloadSerializer.Parse(recovered.Snapshot!.State)).IsEqualTo(expectedSum);
        await AssertCrashOperationStatusesAsync(store, strategy, firstId, secondId);
    }

    /// <summary>Checks terminal and pending operation states after process restart.</summary>
    /// <param name="store">The reopened SQLite adapter.</param>
    /// <param name="strategy">The outbox strategy.</param>
    /// <param name="firstId">The first operation.</param>
    /// <param name="secondId">The second operation.</param>
    /// <returns>A task representing the assertions.</returns>
    private static async Task AssertCrashOperationStatusesAsync(
        SqliteLocalStoreAdapter store,
        BufferStrategy strategy,
        OperationId firstId,
        OperationId secondId)
    {
        var evicts = strategy is BufferStrategy.DropOldest or BufferStrategy.Custom;
        if (evicts)
        {
            var evictedId = strategy == BufferStrategy.DropOldest ? firstId : secondId;
            await AssertDeadLetteredAsync(store, evictedId, strategy == BufferStrategy.Custom ? CustomEvictedReasonCode : DroppedOldestReasonCode);
        }
        else
        {
            await Assert.That((await store.GetOperationStatusAsync(firstId, CancellationToken.None))?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
            await Assert.That((await store.GetOperationStatusAsync(secondId, CancellationToken.None))?.State).IsEqualTo(SyncOperationState.QueuedForUpload);
        }
    }

    /// <summary>Publishes through the selected public asynchronous producer.</summary>
    /// <param name="producer">The producer path.</param>
    /// <param name="context">The public context.</param>
    /// <param name="definition">The stream definition.</param>
    /// <param name="stream">The public stream.</param>
    /// <param name="value">The input delta.</param>
    /// <param name="options">The admission options.</param>
    /// <param name="cancellationToken">The caller cancellation token.</param>
    /// <returns>The local publication receipt.</returns>
    private static async Task<PublishReceipt> PublishCrashInputAsync(
        string producer,
        IOccasionallyConnectedContext context,
        StreamDefinition<CounterState, CounterInput> definition,
        IOccasionallyConnectedStream<CounterState, CounterInput> stream,
        int value,
        RemotePublishOptions options,
        CancellationToken cancellationToken = default)
    {
        if (producer == PublishProducer)
        {
            return await stream.PublishAsync(new(value), options, cancellationToken).AsTask().WaitAsync(ProducerCrashAdmissionTimeout, cancellationToken);
        }

        await using var adapter = new RecordingObserver<CounterInput>()
            .ToRemoteObserver(context, definition, CreatePublicPublishOptions(BufferStrategy.Reject, durable: false));
        return await adapter.PublishAsync(new(value), options, cancellationToken).AsTask().WaitAsync(ProducerCrashAdmissionTimeout, cancellationToken);
    }

    /// <summary>Waits for asynchronous input persistence before signaling the parent.</summary>
    /// <param name="store">The child SQLite adapter.</param>
    /// <param name="subscriptionId">The stream subscription.</param>
    /// <param name="count">The expected pending count.</param>
    /// <returns>A task representing the wait.</returns>
    /// <exception cref="TimeoutException">The input did not persist in time.</exception>
    private static async Task WaitForProducerCrashCountAsync(SqliteLocalStoreAdapter store, SubscriptionId subscriptionId, int count)
    {
        var started = Stopwatch.GetTimestamp();
        while (Stopwatch.GetElapsedTime(started) < ProducerCrashTimeout)
        {
            var recovered = await store.RecoverStreamAsync(Stream, subscriptionId, CancellationToken.None);
            if (recovered.PendingOperations.Count >= count)
            {
                return;
            }

            await Task.Delay(ProducerCrashPollMilliseconds);
        }

        throw new TimeoutException("The producer did not persist the expected operation count.");
    }

    /// <summary>Atomically signals the parent after the durable boundary.</summary>
    /// <param name="signalPath">The signal file path.</param>
    /// <param name="subscriptionId">The durable stream subscription.</param>
    /// <param name="firstId">The first operation, when applicable.</param>
    /// <param name="secondId">The second operation, when applicable.</param>
    /// <returns>A task representing the write.</returns>
    private static async Task WriteProducerCrashSignalAsync(string signalPath, SubscriptionId subscriptionId, OperationId? firstId, OperationId? secondId)
    {
        var temporaryPath = $"{signalPath}.{Environment.ProcessId.ToString(CultureInfo.InvariantCulture)}.tmp";
        await File.WriteAllTextAsync(temporaryPath, string.Join('\n', subscriptionId.Value.ToString("D"), firstId?.Value.ToString("D"), secondId?.Value.ToString("D")));
        File.Move(temporaryPath, signalPath);
    }

    /// <summary>Runs the selected test in a child and kills it after the durable signal.</summary>
    /// <param name="encoded">The child case.</param>
    /// <param name="signalPath">The child signal file.</param>
    /// <returns>A task that completes when the child exits.</returns>
    /// <exception cref="InvalidOperationException">The child fails before signaling.</exception>
    private static async Task KillProducerCrashChildAsync(string encoded, string signalPath)
    {
        var assembly = Path.Combine(AppContext.BaseDirectory, ProducerCrashAssembly);
        ProcessStartInfo info = new("dotnet") { CreateNoWindow = true, RedirectStandardError = true, RedirectStandardOutput = true, UseShellExecute = false };
        info.ArgumentList.Add(assembly);
        info.ArgumentList.Add("--treenode-filter");
        info.ArgumentList.Add(ProducerCrashChildFilter);
        info.Environment[ProducerCrashCaseVariable] = encoded;
        using var child = Process.Start(info) ?? throw new InvalidOperationException("The producer crash child did not start.");
        var output = child.StandardOutput.ReadToEndAsync();
        var error = child.StandardError.ReadToEndAsync();
        var started = Stopwatch.GetTimestamp();
        while (!File.Exists(signalPath) && !child.HasExited && Stopwatch.GetElapsedTime(started) < ProducerCrashTimeout)
        {
            await Task.Delay(ProducerCrashPollMilliseconds);
        }

        var signaled = File.Exists(signalPath);
        if (!child.HasExited)
        {
            child.Kill(entireProcessTree: true);
            await child.WaitForExitAsync().WaitAsync(GuardTimeout);
        }

        if (!signaled)
        {
            throw new InvalidOperationException(string.Join(Environment.NewLine, "The producer crash child did not signal.", await output, await error));
        }
    }

    /// <summary>Waits until the killed producer releases its exclusive SQLite owner handle.</summary>
    /// <param name="databasePath">The child database path.</param>
    /// <returns>A task representing the bounded wait.</returns>
    private static async Task WaitForProducerCrashOwnerReleaseAsync(string databasePath)
    {
        var ownerPath = $"{databasePath}.rxui-owner";
        var started = Stopwatch.GetTimestamp();
        var released = false;
        while (!released && Stopwatch.GetElapsedTime(started) < ProducerCrashTimeout)
        {
            try
            {
                await using var owner = new FileStream(ownerPath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                released = true;
            }
            catch (IOException)
            {
                await Task.Delay(ProducerCrashPollMilliseconds);
            }
            catch (UnauthorizedAccessException)
            {
                await Task.Delay(ProducerCrashPollMilliseconds);
            }
        }

        await Assert.That(released).IsTrue();
    }
}
