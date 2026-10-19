// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Web.Tests;

/// <summary>A controlled context and engine for deterministic lifecycle tests.</summary>
internal sealed class BrowserContext : IOccasionallyConnectedContext, ISyncEngine
{
    /// <summary>The current number of active lifecycle calls.</summary>
    private int _concurrency;

    /// <summary>Gets the number of startup calls.</summary>
    public int StartCount { get; private set; }

    /// <summary>Gets the number of shutdown calls.</summary>
    public int StopCount { get; private set; }

    /// <summary>Gets the number of trigger calls.</summary>
    public int TriggerCount { get; private set; }

    /// <summary>Gets the number of disposal calls.</summary>
    public int DisposeCount { get; private set; }

    /// <summary>Gets the largest observed lifecycle concurrency.</summary>
    public int MaximumConcurrency { get; private set; }

    /// <summary>Gets or sets whether startup waits for cancellation.</summary>
    public bool BlockStart { get; set; }

    /// <summary>Gets or sets whether sync waits for cancellation.</summary>
    public bool BlockTrigger { get; set; }

    /// <summary>Gets or sets whether startup fails.</summary>
    public bool FailStart { get; set; }

    /// <summary>Gets or sets whether shutdown fails.</summary>
    public bool FailStop { get; set; }

    /// <summary>Gets or sets whether a context cancellation callback fails.</summary>
    public bool ThrowOnCancellation { get; set; }

    /// <summary>Gets or sets a reentrant startup callback.</summary>
    public Func<Task>? OnStart { get; set; }

    /// <summary>Gets the startup entry marker.</summary>
    public TaskCompletionSource StartEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Gets the trigger entry marker.</summary>
    public TaskCompletionSource TriggerEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <inheritdoc/>
    public ISyncEngine SyncEngine => this;

    /// <inheritdoc/>
    public IObservable<SyncState> SyncStates => throw new NotSupportedException();

    /// <inheritdoc/>
    public IObservable<SyncOperationStatus> OperationStates => throw new NotSupportedException();

    /// <inheritdoc/>
    public IObservable<OccasionallyConnectedFault> Faults => throw new NotSupportedException();

    /// <inheritdoc/>
    public async ValueTask StartAsync(CancellationToken cancellationToken)
    {
        Enter();
        StartCount++;
        try
        {
            await using var registration = ThrowOnCancellation
                ? cancellationToken.Register(static () => throw new InvalidOperationException("Controlled cancellation failure."))
                : default;
            _ = StartEntered.TrySetResult();
            if (FailStart)
            {
                throw new InvalidOperationException("Controlled startup failure.");
            }

            if (OnStart is not null)
            {
                await OnStart();
            }

            if (BlockStart)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
        }
        finally
        {
            _concurrency--;
        }
    }

    /// <inheritdoc/>
    public ValueTask StopAsync(CancellationToken cancellationToken)
    {
        Enter();
        StopCount++;
        _concurrency--;
        return FailStop
            ? ValueTask.FromException(new InvalidOperationException("Controlled shutdown failure."))
            : ValueTask.CompletedTask;
    }

    /// <inheritdoc/>
    public async ValueTask TriggerSyncAsync(CancellationToken cancellationToken)
    {
        Enter();
        TriggerCount++;
        _ = TriggerEntered.TrySetResult();
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (BlockTrigger)
            {
                await Task.Delay(Timeout.Infinite, cancellationToken);
            }
        }
        finally
        {
            _concurrency--;
        }
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        DisposeCount++;
        return ValueTask.CompletedTask;
    }

    /// <inheritdoc/>
    public IOccasionallyConnectedStream<TState, TInput> GetOrCreateStream<TState, TInput>(
        StreamDefinition<TState, TInput> definition) => throw new NotSupportedException();

    /// <inheritdoc/>
    public ValueTask<PublishReceipt> EnqueueOperationAsync(SyncOperation operation, CancellationToken cancellationToken) =>
        throw new NotSupportedException();

    /// <inheritdoc/>
    public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken) =>
        throw new NotSupportedException();

    /// <summary>Records entry to a lifecycle call.</summary>
    private void Enter()
    {
        _concurrency++;
        MaximumConcurrency = Math.Max(MaximumConcurrency, _concurrency);
    }
}
