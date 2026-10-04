// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Globalization;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="OccasionallyConnectedStream{TState,TInput}"/>.</summary>
/// <content>Crash matrix tests that kill a child process around serialization and observer notification.</content>
public sealed partial class OccasionallyConnectedStreamTests
{
    /// <summary>The child crash case environment variable.</summary>
    private const string StreamCrashCaseVariable = "RXUI_OC_STREAM_CRASH_CASE";

    /// <summary>The crash point before the input is serialized.</summary>
    private const string BeforeSerializationPoint = "before-serialization";

    /// <summary>The crash point after the input is serialized and before the local commit.</summary>
    private const string AfterSerializationPoint = "after-serialization";

    /// <summary>The crash point after the durable inbox insert and before observer notification.</summary>
    private const string AfterInboxBeforeNotificationPoint = "after-inbox-before-notification";

    /// <summary>The adapter kind that selects the in-memory store.</summary>
    private const int CrashInMemoryAdapterKind = 0;

    /// <summary>The adapter kind that selects the SQLite store.</summary>
    private const int CrashSqliteAdapterKind = 1;

    /// <summary>The number of encoded child case fields.</summary>
    private const int StreamCrashCaseFieldCount = 4;

    /// <summary>The second receive cursor used by redelivery.</summary>
    private const string CrashRedeliveryCursor = "cursor-crash-redelivery";

    /// <summary>The signal polling interval in milliseconds.</summary>
    private const int StreamCrashPollMilliseconds = 50;

    /// <summary>The test assembly file name used by direct MTP execution.</summary>
    private const string StreamCrashTestAssemblyFileName = "ReactiveUI.Primitives.OccasionallyConnected.Tests.dll";

    /// <summary>The child test tree node filter.</summary>
    private const string StreamCrashChildTreeNodeFilter = $"/*/*/*/{nameof(WhenStreamCrashChildReachesPoint_ThenSignalIsPublished)}";

    /// <summary>The maximum time to wait for the child signal.</summary>
    private static readonly TimeSpan StreamCrashSignalTimeout = TimeSpan.FromSeconds(25);

    /// <summary>The maximum time to wait for the killed child to exit and drain output.</summary>
    private static readonly TimeSpan StreamCrashExitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies a process killed around serialization or before remote notification reopens without loss or duplication.</summary>
    /// <param name="point">The crash point.</param>
    /// <param name="adapterKind">The local store adapter kind.</param>
    /// <returns>A task that completes when the test finishes.</returns>
    /// <exception cref="InvalidOperationException">The child process fails to start or signal.</exception>
    [Test]
    [NotInParallel("oc-stream-crash-matrix")]
    [Arguments(BeforeSerializationPoint, CrashInMemoryAdapterKind)]
    [Arguments(BeforeSerializationPoint, CrashSqliteAdapterKind)]
    [Arguments(AfterSerializationPoint, CrashInMemoryAdapterKind)]
    [Arguments(AfterSerializationPoint, CrashSqliteAdapterKind)]
    [Arguments(AfterInboxBeforeNotificationPoint, CrashInMemoryAdapterKind)]
    [Arguments(AfterInboxBeforeNotificationPoint, CrashSqliteAdapterKind)]
    public async Task WhenProcessDiesAtStreamCrashPoint_ThenReopenedStreamHonorsDurableBoundary(string point, int adapterKind)
    {
        if (adapterKind == CrashInMemoryAdapterKind)
        {
            await using var inMemory = new InMemoryLocalStoreAdapter();
            await Assert.That(inMemory.Capabilities & LocalStoreCapabilities.DurableLocalCommit).IsEqualTo(LocalStoreCapabilities.None);
            Skip.Test("InMemoryLocalStoreAdapter does not advertise DurableLocalCommit; process-kill and reopen invariants do not apply.");
        }

        var directory = SqliteTestDirectory.Create("oc-stream-crash-");
        try
        {
            var databasePath = Path.Combine(directory.FullName, LocalDatabaseFileName);
            var signalPath = Path.Combine(directory.FullName, $"{point}.signal");
            var eventId = Guid.NewGuid();
            await RunStreamCrashChildAsync(string.Join('\n', point, databasePath, signalPath, eventId.ToString("D")), signalPath);
            await Assert.That(await File.ReadAllTextAsync(signalPath)).IsEqualTo(point);

            await (point == AfterInboxBeforeNotificationPoint
                ? AssertInboxSurvivesWithoutRenotificationAsync(databasePath, eventId)
                : AssertSerializationCrashLeavesNoOperationAsync(databasePath));
        }
        finally
        {
            directory.Delete(recursive: true);
        }
    }

    /// <summary>Child workflow used by stream crash matrix parent tests.</summary>
    /// <returns>A task that completes when the test finishes.</returns>
    /// <exception cref="InvalidOperationException">The child environment is malformed or the crash point was not reached.</exception>
    [Test]
    public async Task WhenStreamCrashChildReachesPoint_ThenSignalIsPublished()
    {
        var encoded = Environment.GetEnvironmentVariable(StreamCrashCaseVariable);
        if (encoded is null)
        {
            await Assert.That(encoded).IsNull();
            return;
        }

        var fields = encoded.Split('\n');
        if (fields.Length != StreamCrashCaseFieldCount || !Guid.TryParse(fields[3], out var eventId))
        {
            throw new InvalidOperationException("The stream crash matrix environment is malformed.");
        }

        await using var store = await CreateInitializedStoreAsync(fields[1]);
        var scheduler = new ControlledObserverScheduler();
        var serializer = new CrashingPayloadSerializer(fields[0], fields[2]);
        await using var stream = CreateStream(store, scheduler: scheduler, serializer: serializer);
        await stream.StartAsync(CancellationToken.None);
        if (fields[0] != AfterInboxBeforeNotificationPoint)
        {
            _ = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);
            throw new InvalidOperationException("The stream crash child published without reaching the serialization crash point.");
        }

        using var subscription = stream.Remote.Subscribe(new RecordingObserver<RemoteMessage<CounterInput>>());
        _ = await stream.ApplyRemoteBatchAsync(CreateCrashRemoteBatch(eventId, null, FirstCursor), CancellationToken.None);
        BlockAfterSignal(fields[2], fields[0]);
    }

    /// <summary>Asserts a crash before the local commit left no operation and did not consume a client sequence.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <returns>A task that represents the asynchronous assertion.</returns>
    private static async Task AssertSerializationCrashLeavesNoOperationAsync(string databasePath)
    {
        await using var store = await CreateInitializedStoreAsync(databasePath);
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, scheduler: scheduler);
        await stream.StartAsync(CancellationToken.None);
        var recovered = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);

        await Assert.That(recovered.PendingOperations.Count).IsEqualTo(0);
        await Assert.That(recovered.NextClientSequence).IsEqualTo(FirstSequence);

        var receipt = await stream.PublishAsync(new(FirstValue), null, CancellationToken.None);
        var afterPublish = await store.RecoverStreamAsync(Stream, stream.SubscriptionId, CancellationToken.None);

        await Assert.That(receipt.ClientSequence).IsEqualTo(FirstSequence);
        await Assert.That(afterPublish.PendingOperations.Count).IsEqualTo(1);
        await Assert.That(afterPublish.PendingOperations[0].OperationId).IsEqualTo(receipt.OperationId);
    }

    /// <summary>Asserts an applied remote event survives the crash, is not notified again, and dedups on redelivery.</summary>
    /// <param name="databasePath">The database path.</param>
    /// <param name="eventId">The remote event identifier.</param>
    /// <returns>A task that represents the asynchronous assertion.</returns>
    private static async Task AssertInboxSurvivesWithoutRenotificationAsync(string databasePath, Guid eventId)
    {
        await using var store = await CreateInitializedStoreAsync(databasePath);
        var scheduler = new ControlledObserverScheduler();
        await using var stream = CreateStream(store, scheduler: scheduler);
        await stream.StartAsync(CancellationToken.None);
        var remote = new RecordingObserver<RemoteMessage<CounterInput>>();
        var local = new RecordingObserver<CounterState>();
        using var remoteSubscription = stream.Remote.Subscribe(remote);
        using var localSubscription = stream.Local.Subscribe(local);
        scheduler.RunAll();
        var unapplied = await store.GetUnappliedEventIdsAsync(Stream, [eventId], CancellationToken.None);

        await Assert.That(unapplied.Count).IsEqualTo(0);
        await Assert.That(remote.Values).IsEmpty();
        await Assert.That(local.Values[^1].Sum).IsEqualTo(ThirdValue);

        var redelivery = new RemoteEventBatch(
            Guid.NewGuid(),
            Stream,
            FirstCursor,
            CrashRedeliveryCursor,
            [CreateCrashRemoteEvent(eventId, FirstCursor, ThirdValue), CreateCrashRemoteEvent(Guid.NewGuid(), CrashRedeliveryCursor, SecondValue)]);
        var redelivered = await stream.ApplyRemoteBatchAsync(redelivery, CancellationToken.None);
        scheduler.RunAll();

        await Assert.That(remote.Values.Count).IsEqualTo(1);
        await Assert.That(remote.Values[0].Value.Delta).IsEqualTo(SecondValue);
        await Assert.That(redelivered.State.State.Sum).IsEqualTo(ThirdValue + SecondValue);
    }

    /// <summary>Creates a remote batch that carries only the crash event.</summary>
    /// <param name="eventId">The event identifier.</param>
    /// <param name="previousCursor">The previous cursor.</param>
    /// <param name="nextCursor">The next cursor.</param>
    /// <returns>The remote batch.</returns>
    private static RemoteEventBatch CreateCrashRemoteBatch(Guid eventId, string? previousCursor, string nextCursor) =>
        new(Guid.NewGuid(), Stream, previousCursor, nextCursor, [CreateCrashRemoteEvent(eventId, nextCursor, ThirdValue)]);

    /// <summary>Creates a remote counter event with a fixed identifier.</summary>
    /// <param name="eventId">The event identifier.</param>
    /// <param name="cursor">The event cursor.</param>
    /// <param name="value">The counter delta.</param>
    /// <returns>The remote event.</returns>
    private static RemoteEvent CreateCrashRemoteEvent(Guid eventId, string cursor, int value) =>
        new(eventId, Stream, cursor, Now, null, CreatePayload(value), new Dictionary<string, string>());

    /// <summary>Starts the child, waits for its signal, and kills it.</summary>
    /// <param name="encodedCase">The encoded child case.</param>
    /// <param name="signalPath">The signal path.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The child did not start or signal.</exception>
    private static async Task RunStreamCrashChildAsync(string encodedCase, string signalPath)
    {
        var testAssembly = Path.Combine(AppContext.BaseDirectory, StreamCrashTestAssemblyFileName);
        ProcessStartInfo startInfo = new("dotnet") { CreateNoWindow = true, RedirectStandardError = true, RedirectStandardOutput = true, UseShellExecute = false };
        startInfo.ArgumentList.Add(testAssembly);
        startInfo.ArgumentList.Add("--treenode-filter");
        startInfo.ArgumentList.Add(StreamCrashChildTreeNodeFilter);
        startInfo.Environment[StreamCrashCaseVariable] = encodedCase;
        using var child = Process.Start(startInfo) ?? throw new InvalidOperationException("The stream crash child did not start.");
        var standardOutput = child.StandardOutput.ReadToEndAsync();
        var standardError = child.StandardError.ReadToEndAsync();
        var startTimestamp = Stopwatch.GetTimestamp();
        while (!File.Exists(signalPath) && !child.HasExited && Stopwatch.GetElapsedTime(startTimestamp) < StreamCrashSignalTimeout)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(StreamCrashPollMilliseconds));
        }

        var signaled = File.Exists(signalPath);
        if (!child.HasExited)
        {
            child.Kill(entireProcessTree: true);
            await child.WaitForExitAsync().WaitAsync(StreamCrashExitTimeout);
        }

        var output = await standardOutput.WaitAsync(StreamCrashExitTimeout);
        var error = await standardError.WaitAsync(StreamCrashExitTimeout);
        if (!signaled)
        {
            throw new InvalidOperationException(string.Join(Environment.NewLine, "The stream crash child did not signal.", output, error));
        }
    }

    /// <summary>Publishes an atomic signal file and blocks the calling thread until the process is killed.</summary>
    /// <param name="signalPath">The signal path.</param>
    /// <param name="point">The reached crash point.</param>
    private static void BlockAfterSignal(string signalPath, string point)
    {
        var temporaryPath = $"{signalPath}.{Environment.ProcessId.ToString(CultureInfo.InvariantCulture)}.tmp";
        File.WriteAllText(temporaryPath, point);
        File.Move(temporaryPath, signalPath);
        using var never = new ManualResetEventSlim(false);
        never.Wait();
    }

    /// <summary>Blocks input serialization at the configured crash point.</summary>
    /// <param name="point">The crash point.</param>
    /// <param name="signalPath">The signal path.</param>
    private sealed class CrashingPayloadSerializer(string point, string signalPath) : IPayloadSerializer
    {
        /// <summary>The serializer that produces the payload bytes.</summary>
        private readonly ScriptedPayloadSerializer _inner = new();

        /// <inheritdoc />
        public string ContentType => _inner.ContentType;

        /// <inheritdoc />
        public async ValueTask<PayloadEnvelope> SerializeAsync<T>(
            string contractId,
            int schemaVersion,
            T value,
            CancellationToken cancellationToken)
        {
            var isInput = value is CounterInput;
            if (isInput && point == BeforeSerializationPoint)
            {
                BlockAfterSignal(signalPath, point);
            }

            var envelope = await _inner.SerializeAsync(contractId, schemaVersion, value, cancellationToken).ConfigureAwait(false);
            if (isInput && point == AfterSerializationPoint)
            {
                BlockAfterSignal(signalPath, point);
            }

            return envelope;
        }

        /// <inheritdoc />
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public ValueTask<object> DeserializeAsync(PayloadEnvelope envelope, Type targetType, CancellationToken cancellationToken) =>
            _inner.DeserializeAsync(envelope, targetType, cancellationToken);
    }
}
