// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using ReactiveUI.Primitives.OccasionallyConnected;
using ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;
using ReactiveUI.Primitives.OccasionallyConnected.Transport.Http;

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

/// <summary>Creates collaboration client sessions and runs command-line operations.</summary>
internal static class CollaborationClientApplication
{
    /// <summary>The bounded stream buffer capacity used by the example.</summary>
    private const int ActivityBufferCapacity = 64;

    /// <summary>Opens a collaboration client session.</summary>
    /// <param name="options">The client options.</param>
    /// <returns>The opened session.</returns>
    internal static async ValueTask<CollaborationClientSession> OpenAsync(CollaborationClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.Validate();
        var httpClient = new HttpClient { BaseAddress = options.ServerUri };
        OccasionallyConnectedContext? context = null;
        SqliteLocalStoreAdapter? store = null;
        HttpRemoteTransportAdapter? transport = null;
        _ = httpClient.DefaultRequestHeaders.TryAddWithoutValidation(CollaborationClientOptions.DevelopmentTokenHeaderName, options.Token);
        try
        {
            transport = new(new()
            {
                HttpClient = httpClient,
                BaseAddress = options.ServerUri,
                AllowInsecureLoopbackHttp = options.ServerUri.IsLoopback,
                MaximumRequestBytes = options.MaximumTransportBytes,
                MaximumResponseBytes = options.MaximumTransportBytes,
                MaximumPayloadBytes = ActivityPayloadSerializer.MaximumPayloadBytes,
            });
            store = new(options.DatabasePath);
            context = new OccasionallyConnectedBuilder()
                .UseClient(new(options.ClientId))
                .UseStore(store)
                .UseTransport(transport)
                .UseSerializer(ActivityPayloadSerializer.Instance)
                .UseStoreIdentity(options.StoreIdentity)
                .UseOptions(OccasionallyConnectedOptions.Default with { AutoStart = options.AutoStart })
                .Build();
            var activity = context.GetOrCreateStream(CreateActivityDefinition());
            return new(context, activity, httpClient);
        }
        catch (Exception exception)
        {
            await DisposeAfterOpenFailureAsync(context, store, transport, httpClient, exception).ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>Runs a parsed command-line command.</summary>
    /// <param name="command">The command.</param>
    /// <param name="output">The output writer.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The process exit code.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The command kind is unsupported.</exception>
    internal static async Task<int> RunAsync(
        CollaborationClientCommand command,
        TextWriter output,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(command);
        ArgumentNullException.ThrowIfNull(output);
        if (command.Kind is not CollaborationClientCommandKind.Publish and not CollaborationClientCommandKind.Watch)
        {
            throw new ArgumentOutOfRangeException(nameof(command), command.Kind, "Unsupported collaboration client command.");
        }

        if (command.Kind == CollaborationClientCommandKind.Publish)
        {
            var publishCommand = command with { Options = command.Options with { AutoStart = false } };
            await using var publishSession = await OpenAsync(publishCommand.Options).ConfigureAwait(false);
            return await RunPublishAsync(publishSession, command, output, cancellationToken).ConfigureAwait(false);
        }

        await using var session = await OpenAsync(command.Options).ConfigureAwait(false);
        using var subscription = session.Activity.Local.Subscribe(new PrintingActivityObserver(output));
        await session.StartAsync(cancellationToken).ConfigureAwait(false);
        return await WaitForWatchCancellationAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Keeps a watch command active until cancellation ends it.</summary>
    /// <param name="cancellationToken">The cancellation token that ends the watch.</param>
    /// <returns>A task that ends by cancellation and never reports a successful watch exit.</returns>
    internal static async Task<int> WaitForWatchCancellationAsync(CancellationToken cancellationToken)
    {
        var completion = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var registration = cancellationToken.UnsafeRegister(
            state =>
            {
                _ = completion.TrySetCanceled(cancellationToken);
            },
            null);
        return await completion.Task.ConfigureAwait(false);
    }

    /// <summary>Disposes partially created dependencies after an open failure.</summary>
    /// <param name="context">The partially created context.</param>
    /// <param name="store">The partially created store.</param>
    /// <param name="transport">The partially created transport.</param>
    /// <param name="httpClient">The HTTP client to dispose.</param>
    /// <param name="openFailure">The original open failure.</param>
    /// <returns>The cleanup task.</returns>
    internal static async ValueTask DisposeAfterOpenFailureAsync(
        IAsyncDisposable? context,
        IAsyncDisposable? store,
        IAsyncDisposable? transport,
        HttpClient httpClient,
        Exception openFailure)
    {
        Exception? cleanupFailure = null;
        try
        {
            if (context is not null)
            {
                await context.DisposeAsync().ConfigureAwait(false);
            }
            else
            {
                cleanupFailure = await CaptureCleanupFailureAsync(transport, cleanupFailure).ConfigureAwait(false);
                cleanupFailure = await CaptureCleanupFailureAsync(store, cleanupFailure).ConfigureAwait(false);
            }
        }
        catch (Exception exception)
        {
            cleanupFailure = exception;
        }
        finally
        {
            httpClient.Dispose();
        }

        if (cleanupFailure is null)
        {
            return;
        }

        var aggregate = new AggregateException(
            "Opening the collaboration client failed, and cleanup also failed.",
            openFailure,
            cleanupFailure);
        ExceptionDispatchInfo.Capture(aggregate).Throw();
    }

    /// <summary>Writes the bounded sync and fault summary for a publish command.</summary>
    /// <param name="output">The command output writer.</param>
    /// <param name="diagnostics">The retained command diagnostics.</param>
    /// <returns>The write task.</returns>
    internal static async Task WriteDiagnosticsSummaryAsync(TextWriter output, PublishDiagnosticsObserver diagnostics)
    {
        await WriteSyncSummaryAsync(output, diagnostics).ConfigureAwait(false);
        await WriteFaultSummaryAsync(output, diagnostics).ConfigureAwait(false);
    }

    /// <summary>Runs a publish command and writes bounded diagnostics.</summary>
    /// <param name="session">The client session.</param>
    /// <param name="command">The publish command.</param>
    /// <param name="output">The output writer.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The process exit code.</returns>
    private static async Task<int> RunPublishAsync(
        CollaborationClientSession session,
        CollaborationClientCommand command,
        TextWriter output,
        CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linked.CancelAfter(command.Options.WaitTimeout);
        var latest = new LatestActivityObserver();
        var diagnostics = new PublishDiagnosticsObserver();
        using var localSubscription = session.Activity.Local.Subscribe(latest);
        using var syncSubscription = session.ContextSyncStates.Subscribe(diagnostics);
        using var operationSubscription = session.Activity.OperationStates.Subscribe(diagnostics);
        using var faultSubscription = session.Activity.Faults.Subscribe(diagnostics);
        var receipt = await session.PublishAsync(command.Update, linked.Token).ConfigureAwait(false);
        diagnostics.TrackOperation(receipt.OperationId);
        await output.WriteLineAsync($"queued {receipt.OperationId.Value:N}").ConfigureAwait(false);
        if (!command.Options.AutoStart)
        {
            await WriteOperationSummaryAsync(output, receipt).ConfigureAwait(false);
            await WriteFaultSummaryAsync(output, diagnostics).ConfigureAwait(false);
            return 0;
        }

        await session.StartAsync(linked.Token).ConfigureAwait(false);
        var terminal = await diagnostics.WaitForTerminalOperationAsync(linked.Token).ConfigureAwait(false);
        var exitCode = terminal.Status is { } status ? GetPublishExitCode(status.State) : 1;
        if (terminal.Status is { State: SyncOperationState.Synchronized } synchronized)
        {
            var expectedOperationId = receipt.OperationId.Value.ToString("N");
            var view = await latest.WaitForAsync(value => IsAcceptedView(value, expectedOperationId), linked.Token)
                .ConfigureAwait(false);
            await WriteViewAsync(output, view).ConfigureAwait(false);
            await WriteOperationSummaryAsync(output, synchronized).ConfigureAwait(false);
        }
        else if (terminal.Status is { } terminalStatus)
        {
            await WriteOperationSummaryAsync(output, terminalStatus).ConfigureAwait(false);
        }
        else
        {
            await output.WriteLineAsync($"operation: {receipt.OperationId.Value:N} Faulted").ConfigureAwait(false);
        }

        await WriteDiagnosticsSummaryAsync(output, diagnostics).ConfigureAwait(false);
        return exitCode;
    }

    /// <summary>Creates the activity stream definition.</summary>
    /// <returns>The stream definition.</returns>
    private static StreamDefinition<ActivityView, ActivityUpdate> CreateActivityDefinition() =>
        new()
        {
            StreamId = ActivityContracts.StreamId,
            Projection = ActivityProjection.Instance,
            InputContractId = ActivityContracts.ContractId,
            StateContractId = ActivityContracts.ContractId,
            InputSchemaVersion = ActivityContracts.SchemaVersion,
            StateSchemaVersion = ActivityContracts.SchemaVersion,
            Subscription = new()
            {
                StreamId = ActivityContracts.StreamId,
                StartPosition = StartPosition.FromSequence(0),
                DeliveryGuarantee = DeliveryGuarantee.AtLeastOnce,
                BufferStrategy = BufferStrategy.Block,
                BufferCapacity = ActivityBufferCapacity,
                BufferCapacityBytes = ActivityPayloadSerializer.MaximumRetainedInputBytes * ActivityBufferCapacity,
            },
            Publish = new()
            {
                StreamId = ActivityContracts.StreamId,
                Durable = true,
                DeliveryGuarantee = DeliveryGuarantee.AtLeastOnce,
                ConflictPolicy = ConflictPolicy.Merge,
                AdmissionStrategy = BufferStrategy.Block,
            },
            Input = CreateInputOptions(),
            InputCapture = ActivityPayloadSerializer.Instance,
            TypedInput = new()
            {
                BufferCapacity = ActivityBufferCapacity,
                BufferCapacityBytes = ActivityPayloadSerializer.MaximumRetainedInputBytes * ActivityBufferCapacity,
                MaximumRetainedInputBytes = ActivityPayloadSerializer.MaximumRetainedInputBytes,
            },
        };

    /// <summary>Creates local typed input buffering options.</summary>
    /// <returns>The local typed input buffering options.</returns>
    private static ObserverInputOptions CreateInputOptions() =>
        new() { BufferStrategy = BufferStrategy.Reject, BufferCapacity = ActivityBufferCapacity, BufferCapacityBytes = ActivityPayloadSerializer.MaximumRetainedInputBytes * ActivityBufferCapacity };

    /// <summary>Disposes one partial dependency and aggregates cleanup failures.</summary>
    /// <param name="disposable">The disposable dependency.</param>
    /// <param name="cleanupFailure">The previous cleanup failure.</param>
    /// <returns>The aggregated cleanup failure.</returns>
    private static async ValueTask<Exception?> CaptureCleanupFailureAsync(
        IAsyncDisposable? disposable,
        Exception? cleanupFailure)
    {
        if (disposable is null)
        {
            return cleanupFailure;
        }

        try
        {
            await disposable.DisposeAsync().ConfigureAwait(false);
            return cleanupFailure;
        }
        catch (Exception exception)
        {
            return cleanupFailure is null ? exception : new AggregateException(cleanupFailure, exception);
        }
    }

    /// <summary>Determines whether a view accepted the expected operation.</summary>
    /// <param name="value">The activity view.</param>
    /// <param name="expectedOperationId">The expected operation id text.</param>
    /// <returns>Whether the view accepted the operation.</returns>
    private static bool IsAcceptedView(ActivityView value, string expectedOperationId) =>
        !value.IsPending && string.Equals(value.AcceptedOperationId, expectedOperationId, StringComparison.Ordinal);

    /// <summary>Writes a synchronization lifecycle summary.</summary>
    /// <param name="output">The output writer.</param>
    /// <param name="diagnostics">The retained diagnostics.</param>
    /// <returns>The write task.</returns>
    private static Task WriteSyncSummaryAsync(TextWriter output, PublishDiagnosticsObserver diagnostics)
    {
        var snapshot = diagnostics.Snapshot();
        var status = snapshot.LatestSyncStatus?.ToString() ?? "none";
        return output.WriteLineAsync($"sync: {status}");
    }

    /// <summary>Writes an operation summary from a local receipt.</summary>
    /// <param name="output">The output writer.</param>
    /// <param name="receipt">The publish receipt.</param>
    /// <returns>The write task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task WriteOperationSummaryAsync(TextWriter output, PublishReceipt receipt) =>
        output.WriteLineAsync($"operation: {receipt.OperationId.Value:N} {receipt.State}");

    /// <summary>Writes an operation summary from an operation status.</summary>
    /// <param name="output">The output writer.</param>
    /// <param name="status">The operation status.</param>
    /// <returns>The write task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task WriteOperationSummaryAsync(TextWriter output, SyncOperationStatus status) =>
        output.WriteLineAsync($"operation: {status.OperationId.Value:N} {status.State}");

    /// <summary>Writes a bounded fault summary.</summary>
    /// <param name="output">The output writer.</param>
    /// <param name="diagnostics">The retained diagnostics.</param>
    /// <returns>The write task.</returns>
    private static async Task WriteFaultSummaryAsync(TextWriter output, PublishDiagnosticsObserver diagnostics)
    {
        var snapshot = diagnostics.Snapshot();
        await output.WriteLineAsync($"faults: {snapshot.FaultCount}").ConfigureAwait(false);
        if (snapshot.LatestFault is { } fault)
        {
            await WriteFaultLineAsync(output, fault).ConfigureAwait(false);
        }
    }

    /// <summary>Writes a redacted fault line.</summary>
    /// <param name="output">The output writer.</param>
    /// <param name="fault">The fault.</param>
    /// <returns>The write task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task WriteFaultLineAsync(TextWriter output, OccasionallyConnectedFault fault) =>
        output.WriteLineAsync($"fault: {fault.Code}");

    /// <summary>Gets the process exit code for a terminal publish state.</summary>
    /// <param name="state">The terminal state.</param>
    /// <returns>The process exit code.</returns>
    private static int GetPublishExitCode(SyncOperationState state) =>
        state == SyncOperationState.Synchronized ? 0 : 1;

    /// <summary>Writes an activity view.</summary>
    /// <param name="output">The output writer.</param>
    /// <param name="view">The view.</param>
    /// <returns>The write task.</returns>
    private static async Task WriteViewAsync(TextWriter output, ActivityView view)
    {
        await output.WriteLineAsync($"status: {view.Status}").ConfigureAwait(false);
        await output.WriteLineAsync($"title: {view.Title ?? string.Empty}").ConfigureAwait(false);
        await output.WriteLineAsync($"details: {view.Details ?? string.Empty}").ConfigureAwait(false);
        await output.WriteLineAsync($"accepted: {view.AcceptedClientId}/{view.AcceptedVersion}").ConfigureAwait(false);
    }

    /// <summary>Retains bounded publish diagnostics for one command.</summary>
    internal sealed class PublishDiagnosticsObserver :
        IObserver<SyncState>,
        IObserver<SyncOperationStatus>,
        IObserver<OccasionallyConnectedFault>
    {
        /// <summary>The maximum retained operation states.</summary>
        private const int MaxRetainedOperationStates = 16;

        /// <summary>The maximum retained fault count.</summary>
        private const int MaxRetainedFaultCount = 64;

        /// <summary>The synchronization gate.</summary>
        private readonly object _gate = new();

        /// <summary>The recent operation states.</summary>
        private readonly List<SyncOperationStatus> _operationStates = [];

        /// <summary>The terminal operation completion.</summary>
        private readonly TaskCompletionSource<SyncOperationStatus> _operationCompletion =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The matching fault completion.</summary>
        private readonly TaskCompletionSource<OccasionallyConnectedFault> _faultCompletion =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The tracked operation id.</summary>
        private OperationId? _operationId;

        /// <summary>The latest sync lifecycle status.</summary>
        private SyncLifecycleStatus? _latestSyncStatus;

        /// <summary>The bounded fault count.</summary>
        private int _faultCount;

        /// <summary>The latest retained fault.</summary>
        private OccasionallyConnectedFault? _latestFault;

        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        public void OnError(Exception error) => _ = _operationCompletion.TrySetException(error);

        /// <inheritdoc />
        public void OnNext(SyncState value)
        {
            lock (_gate)
            {
                _latestSyncStatus = value.Status;
            }
        }

        /// <inheritdoc />
        public void OnNext(SyncOperationStatus value)
        {
            lock (_gate)
            {
                if (_operationId is { } operationId && operationId != value.OperationId)
                {
                    return;
                }

                RetainOperationState(value);
                CompleteIfTrackedTerminal(value);
            }
        }

        /// <inheritdoc />
        public void OnNext(OccasionallyConnectedFault value)
        {
            lock (_gate)
            {
                _faultCount = Math.Min(_faultCount + 1, MaxRetainedFaultCount);
                _latestFault = value;
                CompleteIfTrackedFault(value);
            }
        }

        /// <summary>Waits for the tracked operation to reach a terminal state or fault.</summary>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The terminal publish outcome.</returns>
        internal async Task<PublishTerminalResult> WaitForTerminalOperationAsync(CancellationToken cancellationToken)
        {
            var completed = await Task.WhenAny(_operationCompletion.Task, _faultCompletion.Task)
                .WaitAsync(cancellationToken)
                .ConfigureAwait(false);
            if (ReferenceEquals(completed, _operationCompletion.Task))
            {
                return new(await _operationCompletion.Task.ConfigureAwait(false));
            }

            _ = await _faultCompletion.Task.ConfigureAwait(false);
            return new(null);
        }

        /// <summary>Captures a thread-safe diagnostic snapshot.</summary>
        /// <returns>The diagnostic snapshot.</returns>
        internal PublishDiagnosticsSnapshot Snapshot()
        {
            lock (_gate)
            {
                return new(_latestSyncStatus, _faultCount, _latestFault);
            }
        }

        /// <summary>Tracks the command operation id.</summary>
        /// <param name="operationId">The operation id.</param>
        internal void TrackOperation(OperationId operationId)
        {
            lock (_gate)
            {
                _operationId = operationId;
                RemoveUntrackedOperationStates(operationId);
                for (var index = _operationStates.Count - 1; index >= 0; index--)
                {
                    CompleteIfTrackedTerminal(_operationStates[index]);
                    if (_operationCompletion.Task.IsCompleted)
                    {
                        return;
                    }
                }

                if (_latestFault is { } fault)
                {
                    CompleteIfTrackedFault(fault);
                }
            }
        }

        /// <summary>Retains one operation state in a bounded window.</summary>
        /// <param name="value">The operation state.</param>
        private void RetainOperationState(SyncOperationStatus value)
        {
            if (_operationStates.Count == MaxRetainedOperationStates)
            {
                _operationStates.RemoveAt(0);
            }

            _operationStates.Add(value);
        }

        /// <summary>Removes operation states not related to the tracked command.</summary>
        /// <param name="operationId">The tracked operation id.</param>
        private void RemoveUntrackedOperationStates(OperationId operationId)
        {
            for (var index = _operationStates.Count - 1; index >= 0; index--)
            {
                if (_operationStates[index].OperationId != operationId)
                {
                    _operationStates.RemoveAt(index);
                }
            }
        }

        /// <summary>Completes the operation wait if the state is terminal.</summary>
        /// <param name="value">The operation state.</param>
        private void CompleteIfTrackedTerminal(SyncOperationStatus value)
        {
            static bool isTerminalOperationState(SyncOperationState state) =>
                state is SyncOperationState.Synchronized
                    or SyncOperationState.Conflict
                    or SyncOperationState.Rejected
                    or SyncOperationState.DeadLettered
                    or SyncOperationState.Ambiguous
                    or SyncOperationState.GuaranteeExpired;

            if (_operationId == value.OperationId && isTerminalOperationState(value.State))
            {
                _ = _operationCompletion.TrySetResult(value);
            }
        }

        /// <summary>Completes the operation wait when a matching fault arrives.</summary>
        /// <param name="value">The fault.</param>
        private void CompleteIfTrackedFault(OccasionallyConnectedFault value)
        {
            static bool isTerminalFault(OccasionallyConnectedFault fault) =>
                !fault.IsTransient && fault.Severity is FaultSeverity.Error or FaultSeverity.Critical;

            if (!isTerminalFault(value))
            {
                return;
            }

            if (_operationId == value.OperationId || value.OperationId is null)
            {
                _ = _faultCompletion.TrySetResult(value);
            }
        }
    }

    /// <summary>Prints activity views as they change.</summary>
    internal sealed class PrintingActivityObserver : IObserver<ActivityView>
    {
        /// <summary>The output writer.</summary>
        private readonly TextWriter _output;

        /// <summary>Initializes a new instance of the <see cref="PrintingActivityObserver"/> class.</summary>
        /// <param name="output">The output writer.</param>
        internal PrintingActivityObserver(TextWriter output) => _output = output;

        /// <inheritdoc />
        public void OnCompleted()
        {
        }

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => _output.WriteLine("fault: ActivityObservationFailed");

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(ActivityView value) =>
            _output.WriteLine($"{value.Status} | {value.Title ?? string.Empty} | {value.Details ?? string.Empty} | {value.AcceptedVersion}");
    }

    /// <summary>Captures a terminal publish outcome.</summary>
    /// <param name="Status">The terminal operation status.</param>
    internal sealed record PublishTerminalResult(SyncOperationStatus? Status);

    /// <summary>Captures diagnostics needed for bounded command output.</summary>
    /// <param name="LatestSyncStatus">The latest sync lifecycle status.</param>
    /// <param name="FaultCount">The bounded fault count.</param>
    /// <param name="LatestFault">The latest fault.</param>
    internal sealed record PublishDiagnosticsSnapshot(
        SyncLifecycleStatus? LatestSyncStatus,
        int FaultCount,
        OccasionallyConnectedFault? LatestFault);
}
