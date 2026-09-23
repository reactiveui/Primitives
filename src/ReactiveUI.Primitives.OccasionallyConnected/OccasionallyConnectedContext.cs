// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Coordinates synchronization and typed local-first stream lifetimes for one client context.</summary>
[DebuggerDisplay("Streams={_registrations.Count,nq}; Startup={StartupTask.Status,nq}")]
public sealed class OccasionallyConnectedContext : IOccasionallyConnectedContext
{
    /// <summary>Protects stream registry and context lifecycle flags.</summary>
    private readonly Lock _gate = new();

    /// <summary>Stores immutable context options.</summary>
    private readonly OccasionallyConnectedContextOptions _options;

    /// <summary>Coordinates shared context start/stop requests.</summary>
    private readonly LifecycleTransitionCoordinator _lifecycle;

    /// <summary>Stores registered stream facades by stream identifier.</summary>
    private readonly Dictionary<StreamId, ContextStreamRegistration> _registrations = [];

    /// <summary>Stores asynchronous stream starts launched after the context is already running.</summary>
    private readonly List<ContextStreamStartOperation> _streamStartTasks = [];

    /// <summary>Stores context lifecycle intent while the context gate is held.</summary>
    private readonly ContextLifecycleIntent _lifecycleIntent = new();

    /// <summary>Tracks registry mutations during a start sweep.</summary>
    private long _registrationVersion;

    /// <summary>Stores the first asynchronously observed stream start failure.</summary>
    private Exception? _streamStartFailure;

    /// <summary>Stores the shared disposal task after the first disposal call.</summary>
    private Task? _disposeTask;

    /// <summary>Tracks whether the context has been disposed.</summary>
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="OccasionallyConnectedContext"/> class.</summary>
    /// <param name="options">The context options.</param>
    /// <exception cref="ArgumentNullException"><paramref name="options"/> is null.</exception>
    /// <exception cref="InvalidOperationException"><paramref name="options"/> is malformed.</exception>
    internal OccasionallyConnectedContext(OccasionallyConnectedContextOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        options.Validate();
        _options = options;
        _lifecycle = new(StartCoreAsync, StopCoreAsync);
        StartupTask = options.AutoStart ? ScheduleStartup(this) : Task.CompletedTask;
    }

    /// <summary>Gets the asynchronously scheduled startup outcome when AutoStart is enabled.</summary>
    public Task StartupTask { get; }

    /// <inheritdoc />
    public ISyncEngine SyncEngine => _options.Engine;

    /// <inheritdoc />
    public IObservable<SyncState> SyncStates => _options.Engine.SyncStates;

    /// <inheritdoc />
    public IOccasionallyConnectedStream<TState, TInput> GetOrCreateStream<TState, TInput>(
        StreamDefinition<TState, TInput> definition)
    {
        ArgumentExceptionHelper.ThrowIfNull(definition);
        definition.Validate(supportsCustomPolicy: false, _options.Options.MinimumPriority, _options.Options.MaximumPriority);
        var typedInput = definition.TypedInput ?? throw new InvalidOperationException("TypedInput must be supplied for public context stream registration.");
        ValidateSchemaRegistration(definition);

        OccasionallyConnectedStream<TState, TInput> stream;
        ContextStreamStartOperation? streamStartOperation = null;
        lock (_gate)
        {
            ThrowIfDisposedLocked();
            if (_registrations.TryGetValue(definition.StreamId, out var existing))
            {
                return GetCompatibleStream(existing, definition, typedInput);
            }

            if (_registrations.Count >= _options.RegistryCapacity)
            {
                throw new InvalidOperationException("The context stream registry capacity has been reached.");
            }

            stream = CreateStream(definition, typedInput);
            _registrations.Add(definition.StreamId, ContextStreamRegistration.Create(definition, typedInput, stream));
            _registrationVersion++;
            var startDecision = _lifecycleIntent.CanStartLateStream();
            if (startDecision.CanStart && startDecision.Generation is { } generation)
            {
                streamStartOperation = CreateStreamStartOperation(stream, generation.Token);
                _streamStartTasks.Add(streamStartOperation);
            }
        }

        if (streamStartOperation is not null)
        {
            streamStartOperation.Start(TaskScheduler.Default);
            _ = ObserveStreamStartAsync(streamStartOperation);
        }

        return stream;
    }

    /// <inheritdoc />
    public async ValueTask StartAsync(CancellationToken cancellationToken)
    {
        Task? stopTask;
        lock (_gate)
        {
            stopTask = _lifecycleIntent.SnapshotAcceptedStopTask();
        }

        if (stopTask?.IsCompleted == false)
        {
            await stopTask.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
        }

        await _lifecycle.StartAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask StopAsync(CancellationToken cancellationToken)
    {
        Task stopTask;
        TaskCompletionSource<bool>? completion = null;
        cancellationToken.ThrowIfCancellationRequested();
        lock (_gate)
        {
            ThrowIfDisposedLocked();
            var stopDecision = _lifecycleIntent.TryAcceptStop();
            stopTask = stopDecision.StopTask;
            completion = stopDecision.Completion;
        }

        if (completion is not null)
        {
            _ = RunAcceptedStopAsync(completion);
        }

        await stopTask.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        TaskCompletionSource<bool>? completion = null;
        Task task;
        lock (_gate)
        {
            if (_disposeTask is null)
            {
                _disposed = true;
                completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                _disposeTask = completion.Task;
            }

            task = _disposeTask;
        }

        if (completion is not null)
        {
            var startCancellation = CancelStartAsync();
            var engineStop = _options.Engine.StopAsync(CancellationToken.None).AsTask();
            _ = RunDisposeAsync(completion, startCancellation, engineStop);
        }

        return new(task);
    }

    /// <summary>Schedules context startup so caller-supplied dependencies cannot synchronously block construction.</summary>
    /// <param name="context">The context to start.</param>
    /// <returns>The scheduled startup task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static Task ScheduleStartup(OccasionallyConnectedContext context)
    {
        var task = Task.Factory.StartNew(
                context.StartContextAsync,
                CancellationToken.None,
                TaskCreationOptions.DenyChildAttach,
                TaskScheduler.Default)
            .Unwrap();
        _ = ObserveStartupAsync(task);
        return task;
    }

    /// <summary>Observes scheduled startup failure while preserving the original task for callers.</summary>
    /// <param name="task">The startup task.</param>
    /// <returns>The observer task.</returns>
    private static async Task ObserveStartupAsync(Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch
        {
            _ = task.Exception;
        }
    }

    /// <summary>Captures a cleanup failure while preserving the first observed failure.</summary>
    /// <param name="action">The cleanup action.</param>
    /// <param name="failure">The current first failure.</param>
    /// <returns>The preserved first failure.</returns>
    private static async ValueTask<Exception?> CaptureFailureAsync(Func<Task> action, Exception? failure)
    {
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failure ??= exception;
        }

        return failure;
    }

    /// <summary>Disposes a completed start generation outside the context gate.</summary>
    /// <param name="generation">The generation to dispose.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void DisposeStartGeneration(StartGeneration? generation) => generation?.Dispose();

    /// <summary>Creates an owned stream-start operation so admission can be recorded before user code runs.</summary>
    /// <typeparam name="TState">The stream state type.</typeparam>
    /// <typeparam name="TInput">The stream input type.</typeparam>
    /// <param name="stream">The stream to start.</param>
    /// <param name="generationToken">The admitted start generation token.</param>
    /// <returns>The owned stream-start operation.</returns>
    private static ContextStreamStartOperation CreateStreamStartOperation<TState, TInput>(
        OccasionallyConnectedStream<TState, TInput> stream,
        CancellationToken generationToken) =>
        new(() => stream.StartAsync(CancellationToken.None).AsTask(), generationToken);

    /// <summary>Returns a compatible existing stream facade or rejects the definition.</summary>
    /// <typeparam name="TState">The state type.</typeparam>
    /// <typeparam name="TInput">The input type.</typeparam>
    /// <param name="existing">The existing registration.</param>
    /// <param name="definition">The requested definition.</param>
    /// <param name="typedInput">The requested typed input options.</param>
    /// <returns>The compatible existing stream facade.</returns>
    /// <exception cref="InvalidOperationException">The existing registration is incompatible.</exception>
    private static IOccasionallyConnectedStream<TState, TInput> GetCompatibleStream<TState, TInput>(
        ContextStreamRegistration existing,
        StreamDefinition<TState, TInput> definition,
        TypedInputOptions typedInput)
    {
        if (existing.IsCompatible(definition, typedInput))
        {
            return (IOccasionallyConnectedStream<TState, TInput>)existing.Stream;
        }

        throw new InvalidOperationException("A stream with the same identifier has an incompatible definition.");
    }

    /// <summary>Starts this context from the scheduled startup driver.</summary>
    /// <returns>The startup task.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Task StartContextAsync() => StartAsync(CancellationToken.None).AsTask();

    /// <summary>Creates a typed stream facade from a validated definition.</summary>
    /// <typeparam name="TState">The state type.</typeparam>
    /// <typeparam name="TInput">The input type.</typeparam>
    /// <param name="definition">The stream definition.</param>
    /// <param name="typedInput">The validated typed input options.</param>
    /// <returns>The typed stream facade.</returns>
    private OccasionallyConnectedStream<TState, TInput> CreateStream<TState, TInput>(
        StreamDefinition<TState, TInput> definition,
        TypedInputOptions typedInput) => new(new OccasionallyConnectedStreamOptions<TState, TInput>
        {
            Definition = definition,
            Store = _options.Store,
            Serializer = _options.Serializer,
            TimeProvider = _options.TimeProvider,
            OperationIdSource = _options.OperationIdSource,
            Coordinator = _options.Engine,
            LocalStateSnapshotFactory = DeserializeStateAsync<TState>,
            RemoteInputSnapshotFactory = DeserializeInputAsync<TInput>,
            NotificationScheduler = _options.NotificationScheduler,
            WorkCapacity = (int)Math.Min(typedInput.BufferCapacity, typedInput.BufferCapacityBytes / typedInput.MaximumRetainedInputBytes),
            LocalAdmissionRetainedBytes = typedInput.MaximumRetainedInputBytes,
            ClientId = _options.Client.ClientId,
            MinimumPriority = _options.Options.MinimumPriority,
            MaximumPriority = _options.Options.MaximumPriority,
        });

    /// <summary>Starts shared engine work and all currently registered streams.</summary>
    /// <returns>The start operation.</returns>
    /// <exception cref="OperationCanceledException">A stop or dispose request cancels startup before it completes.</exception>
    private async ValueTask StartCoreAsync()
    {
        var generation = BeginStart();
        var cancellationToken = generation.Token;
        try
        {
            await _options.Engine.StartAsync(cancellationToken).ConfigureAwait(false);
            while (true)
            {
                var streams = SnapshotStreams(out var version);
                for (var i = 0; i < streams.Length; i++)
                {
                    var stream = streams[i];
                    await stream.StartAsync(cancellationToken).ConfigureAwait(false);
                }

                lock (_gate)
                {
                    if (!_lifecycleIntent.TryCommitRunning(generation, _registrationVersion != version))
                    {
                        continue;
                    }

                    return;
                }
            }
        }
        catch (Exception exception)
        {
            var lease = CompleteStartGeneration(generation);
            _ = await CaptureFailureAsync(() => StopStartedResourcesAfterStartFailureAsync(), exception).ConfigureAwait(false);
            _ = await CaptureFailureAsync(() => lease.CancellationTask, exception).ConfigureAwait(false);
            DisposeStartGeneration(lease.Generation);
            throw;
        }
    }

    /// <summary>Stops all registered streams and shared engine work.</summary>
    /// <returns>The stop operation.</returns>
    private async ValueTask StopCoreAsync()
    {
        var startCancellation = CancelStartAsync();
        Exception? failure = null;
        failure = await CaptureFailureAsync(() => startCancellation, failure).ConfigureAwait(false);
        failure = await AwaitTrackedStreamStartsAsync(failure).ConfigureAwait(false);
        var streams = SnapshotStreams();
        for (var i = 0; i < streams.Length; i++)
        {
            var stream = streams[i];
            failure = await CaptureFailureAsync(() => stream.StopAsync(CancellationToken.None).AsTask(), failure).ConfigureAwait(false);
        }

        failure = await CaptureFailureAsync(() => _options.Engine.StopAsync(CancellationToken.None).AsTask(), failure).ConfigureAwait(false);
        DisposeStartGeneration(MarkNotRunning());
        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>Stops resources that may have been started before context startup failed.</summary>
    /// <returns>The cleanup task.</returns>
    private async Task StopStartedResourcesAfterStartFailureAsync()
    {
        Exception? failure = null;
        var streams = SnapshotStreams();
        for (var i = 0; i < streams.Length; i++)
        {
            var stream = streams[i];
            failure = await CaptureFailureAsync(() => stream.StopAsync(CancellationToken.None).AsTask(), failure).ConfigureAwait(false);
        }

        failure = await CaptureFailureAsync(() => _options.Engine.StopAsync(CancellationToken.None).AsTask(), failure).ConfigureAwait(false);
        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>Runs context disposal outside the context lock and completes the shared disposal task.</summary>
    /// <param name="completion">The shared disposal completion source.</param>
    /// <param name="startCancellation">The start cancellation task.</param>
    /// <param name="engineStop">The direct engine stop task.</param>
    /// <returns>The disposal driver task.</returns>
    private async Task RunDisposeAsync(TaskCompletionSource<bool> completion, Task startCancellation, Task engineStop)
    {
        try
        {
            await DisposeCoreAsync(startCancellation, engineStop).ConfigureAwait(false);
            _ = completion.TrySetResult(true);
        }
        catch (Exception exception)
        {
            _ = completion.TrySetException(exception);
        }
    }

    /// <summary>Runs accepted stop independently from any single caller wait.</summary>
    /// <param name="completion">The shared accepted stop completion.</param>
    /// <returns>The accepted stop driver task.</returns>
    private async Task RunAcceptedStopAsync(TaskCompletionSource<bool> completion)
    {
        try
        {
            var startCancellation = CancelStartAsync();
            var engineStop = _options.Engine.StopAsync(CancellationToken.None).AsTask();
            var lifecycleStop = _lifecycle.StopAsync(CancellationToken.None);
            Exception? failure = null;
            failure = await CaptureFailureAsync(() => startCancellation, failure).ConfigureAwait(false);
            failure = await CaptureFailureAsync(() => engineStop, failure).ConfigureAwait(false);
            failure = await CaptureFailureAsync(() => lifecycleStop, failure).ConfigureAwait(false);
            if (failure is not null)
            {
                throw failure;
            }

            ClearAcceptedStopIntent();
            _ = completion.TrySetResult(true);
        }
        catch (Exception exception)
        {
            _ = completion.TrySetException(exception);
        }
    }

    /// <summary>Disposes context-owned lifecycle, streams, and engine.</summary>
    /// <param name="startCancellation">The start cancellation task.</param>
    /// <param name="engineStop">The direct engine stop task.</param>
    /// <returns>The disposal task.</returns>
    private async Task DisposeCoreAsync(Task startCancellation, Task engineStop)
    {
        Exception? failure = null;
        failure = await CaptureFailureAsync(() => startCancellation, failure).ConfigureAwait(false);
        failure = await CaptureFailureAsync(() => engineStop, failure).ConfigureAwait(false);
        failure = await CaptureLifecycleDisposeFailureAsync(failure).ConfigureAwait(false);

        var streams = SnapshotStreams();
        for (var i = 0; i < streams.Length; i++)
        {
            var stream = streams[i];
            failure = await CaptureFailureAsync(() => stream.DisposeAsync().AsTask(), failure).ConfigureAwait(false);
        }

        failure = await CaptureFailureAsync(() => _options.Engine.DisposeAsync().AsTask(), failure).ConfigureAwait(false);
        DisposeStartGeneration(MarkNotRunning());
        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>Runs lifecycle disposal and preserves the first failure.</summary>
    /// <param name="failure">The current first failure.</param>
    /// <returns>The first failure, if any.</returns>
    private async ValueTask<Exception?> CaptureLifecycleDisposeFailureAsync(Exception? failure)
    {
        try
        {
            await _lifecycle.DisposeAsync().ConfigureAwait(false);
            return failure;
        }
        catch (Exception exception)
        {
            return failure ?? exception;
        }
    }

    /// <summary>Observes an asynchronous stream start and removes it from the tracked set when it completes.</summary>
    /// <param name="operation">The tracked stream-start operation.</param>
    /// <returns>The observer task.</returns>
    private async Task ObserveStreamStartAsync(ContextStreamStartOperation operation)
    {
        try
        {
            var failure = await operation.CaptureFailureAsync(null).ConfigureAwait(false);
            if (failure is not null)
            {
                RecordStreamStartFailure(failure);
            }
        }
        finally
        {
            lock (_gate)
            {
                _ = _streamStartTasks.Remove(operation);
            }

            operation.Dispose();
        }
    }

    /// <summary>Waits for tracked stream starts while preserving the first failure.</summary>
    /// <param name="failure">The current first failure.</param>
    /// <returns>The preserved first failure.</returns>
    private async ValueTask<Exception?> AwaitTrackedStreamStartsAsync(Exception? failure)
    {
        var drain = CaptureStreamStartDrain();
        failure ??= drain.Failure;
        for (var i = 0; i < drain.Operations.Length; i++)
        {
            var operation = drain.Operations[i];
            failure = await operation.CaptureFailureAsync(failure).ConfigureAwait(false);
            operation.Dispose();
        }

        failure ??= TakeStreamStartFailure();
        return failure;
    }

    /// <summary>Validates JSON schema allowlist registration when a registry snapshot was supplied.</summary>
    /// <typeparam name="TState">The state type.</typeparam>
    /// <typeparam name="TInput">The input type.</typeparam>
    /// <param name="definition">The stream definition.</param>
    /// <exception cref="PayloadSchemaException">A configured JSON schema registry does not allow the definition contracts.</exception>
    private void ValidateSchemaRegistration<TState, TInput>(StreamDefinition<TState, TInput> definition)
    {
        var registry = _options.SchemaRegistry;
        if (registry is null)
        {
            return;
        }

        if (!registry.IsRegistered(definition.InputContractId, definition.InputSchemaVersion, typeof(TInput)))
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.TypeNotAllowed, "The input type is not allowlisted for the configured contract and schema version.");
        }

        if (!registry.IsRegistered(definition.StateContractId, definition.StateSchemaVersion, typeof(TState)))
        {
            throw new PayloadSchemaException(PayloadSchemaFailureReason.TypeNotAllowed, "The state type is not allowlisted for the configured contract and schema version.");
        }
    }

    /// <summary>Deserializes a state payload through the context serializer.</summary>
    /// <typeparam name="TState">The target state type.</typeparam>
    /// <param name="envelope">The payload envelope.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The typed state.</returns>
    private async ValueTask<TState> DeserializeStateAsync<TState>(PayloadEnvelope envelope, CancellationToken cancellationToken) =>
        (TState)await _options.Serializer.DeserializeAsync(envelope, typeof(TState), cancellationToken).ConfigureAwait(false);

    /// <summary>Deserializes an input payload through the context serializer.</summary>
    /// <typeparam name="TInput">The target input type.</typeparam>
    /// <param name="envelope">The payload envelope.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>The typed input.</returns>
    private async ValueTask<TInput> DeserializeInputAsync<TInput>(PayloadEnvelope envelope, CancellationToken cancellationToken) =>
        (TInput)await _options.Serializer.DeserializeAsync(envelope, typeof(TInput), cancellationToken).ConfigureAwait(false);

    /// <summary>Gets the current stream lifecycle snapshot.</summary>
    /// <returns>The registered stream lifecycles.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IOccasionallyConnectedStreamLifecycle[] SnapshotStreams() => SnapshotStreams(out _);

    /// <summary>Gets the current stream lifecycle snapshot and registration version.</summary>
    /// <param name="version">The registration version observed with the snapshot.</param>
    /// <returns>The registered stream lifecycles.</returns>
    private IOccasionallyConnectedStreamLifecycle[] SnapshotStreams(out long version)
    {
        lock (_gate)
        {
            var streams = new IOccasionallyConnectedStreamLifecycle[_registrations.Count];
            var index = 0;
            foreach (var registration in _registrations.Values)
            {
                streams[index] = registration.Stream;
                index++;
            }

            version = _registrationVersion;
            return streams;
        }
    }

    /// <summary>Records the first asynchronously observed stream start failure.</summary>
    /// <param name="exception">The observed exception.</param>
    private void RecordStreamStartFailure(Exception exception)
    {
        lock (_gate)
        {
            _streamStartFailure ??= exception;
        }
    }

    /// <summary>Takes the first asynchronously observed stream start failure.</summary>
    /// <returns>The observed failure, or <see langword="null"/>.</returns>
    private Exception? TakeStreamStartFailure()
    {
        lock (_gate)
        {
            var failure = _streamStartFailure;
            _streamStartFailure = null;
            return failure;
        }
    }

    /// <summary>Atomically captures recorded stream start failure and tracked start tasks.</summary>
    /// <returns>The recorded failure and tracked tasks observed under one context gate acquisition.</returns>
    private StreamStartDrain CaptureStreamStartDrain()
    {
        lock (_gate)
        {
            var failure = _streamStartFailure;
            _streamStartFailure = null;
            return new(failure, [.. _streamStartTasks]);
        }
    }

    /// <summary>Begins a shared start attempt and returns its context-owned cancellation generation.</summary>
    /// <returns>The generation canceled by stop or dispose.</returns>
    private StartGeneration BeginStart()
    {
        StartGenerationLease lease;
        lock (_gate)
        {
            ThrowIfDisposedLocked();
            lease = _lifecycleIntent.BeginStart();
        }

        lease.LaunchCancellationIfNeeded();
        return lease.Generation;
    }

    /// <summary>Cancels an active start attempt after leaving the context gate.</summary>
    /// <returns>The shared cancellation completion task.</returns>
    private Task CancelStartAsync()
    {
        StartCancellationDecision decision;
        lock (_gate)
        {
            decision = _lifecycleIntent.CancelStart();
        }

        decision.LaunchCancellationIfNeeded();
        return decision.CancellationTask;
    }

    /// <summary>Clears stale accepted-stop intent when no start generation owns it.</summary>
    private void ClearAcceptedStopIntent()
    {
        lock (_gate)
        {
            _lifecycleIntent.ClearAcceptedStopIntent();
        }
    }

    /// <summary>Completes a start generation and atomically captures its cancellation drain before detaching it.</summary>
    /// <param name="generation">The completed generation.</param>
    /// <returns>The detached generation lease.</returns>
    private StartCancellationLease CompleteStartGeneration(StartGeneration generation)
    {
        StartCancellationLease lease;
        lock (_gate)
        {
            lease = _lifecycleIntent.CompleteStartGeneration(generation);
        }

        return lease;
    }

    /// <summary>Marks the context as not running after stop or disposal.</summary>
    /// <returns>The generation to dispose after active users have observed cancellation.</returns>
    private StartGeneration? MarkNotRunning()
    {
        StartGeneration? completed;
        lock (_gate)
        {
            completed = _lifecycleIntent.MarkNotRunning();
        }

        return completed;
    }

    /// <summary>Throws when the context has been disposed while the caller holds the gate.</summary>
    /// <exception cref="ObjectDisposedException">The context has been disposed.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposedLocked()
    {
        if (!_disposed)
        {
            return;
        }

        throw new ObjectDisposedException(nameof(OccasionallyConnectedContext));
    }

    /// <summary>Stores an atomically captured stream start drain snapshot.</summary>
    /// <param name="Failure">The first recorded stream start failure.</param>
    /// <param name="Operations">The tracked stream start operations.</param>
    private sealed record StreamStartDrain(Exception? Failure, ContextStreamStartOperation[] Operations);
}
