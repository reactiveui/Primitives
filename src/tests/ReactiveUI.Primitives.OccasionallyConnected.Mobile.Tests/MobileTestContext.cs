// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile.Tests;

/// <summary>Records adapter calls without implementing another synchronization engine.</summary>
internal sealed class MobileTestContext : IOccasionallyConnectedContext, ISyncEngine
{
    /// <summary>The number of start calls.</summary>
    private int _starts;

    /// <summary>The number of stop calls.</summary>
    private int _stops;

    /// <summary>The number of trigger calls.</summary>
    private int _triggers;

    /// <inheritdoc/>
    public ISyncEngine SyncEngine => this;

    /// <inheritdoc/>
    public IObservable<SyncState> SyncStates => throw new NotSupportedException();

    /// <inheritdoc/>
    public IObservable<SyncOperationStatus> OperationStates => throw new NotSupportedException();

    /// <inheritdoc/>
    public IObservable<OccasionallyConnectedFault> Faults => throw new NotSupportedException();

    /// <summary>Gets the start count.</summary>
    internal int Starts => Volatile.Read(ref _starts);

    /// <summary>Gets the stop count.</summary>
    internal int Stops => Volatile.Read(ref _stops);

    /// <summary>Gets the trigger count.</summary>
    internal int Triggers => Volatile.Read(ref _triggers);

    /// <summary>Gets a value indicating whether the borrowed context was disposed.</summary>
    internal bool Disposed { get; private set; }

    /// <summary>Gets or sets the start hook.</summary>
    internal Func<CancellationToken, ValueTask>? Start { get; set; }

    /// <summary>Gets or sets the stop hook.</summary>
    internal Func<CancellationToken, ValueTask>? Stop { get; set; }

    /// <summary>Gets or sets the trigger hook.</summary>
    internal Func<CancellationToken, ValueTask>? Trigger { get; set; }

    /// <inheritdoc/>
    public IOccasionallyConnectedStream<TState, TInput> GetOrCreateStream<TState, TInput>(
        StreamDefinition<TState, TInput> definition) => throw new NotSupportedException();

    /// <inheritdoc/>
    public ValueTask<PublishReceipt> EnqueueOperationAsync(SyncOperation operation, CancellationToken cancellationToken) =>
        throw new NotSupportedException();

    /// <inheritdoc/>
    public ValueTask<SyncOperationStatus?> GetOperationStatusAsync(OperationId operationId, CancellationToken cancellationToken) =>
        throw new NotSupportedException();

    /// <inheritdoc/>
    public ValueTask StartAsync(CancellationToken cancellationToken)
    {
        _ = Interlocked.Increment(ref _starts);
        return Start?.Invoke(cancellationToken) ?? ValueTask.CompletedTask;
    }

    /// <inheritdoc/>
    public ValueTask StopAsync(CancellationToken cancellationToken)
    {
        _ = Interlocked.Increment(ref _stops);
        return Stop?.Invoke(cancellationToken) ?? ValueTask.CompletedTask;
    }

    /// <inheritdoc/>
    public ValueTask TriggerSyncAsync(CancellationToken cancellationToken)
    {
        _ = Interlocked.Increment(ref _triggers);
        return Trigger?.Invoke(cancellationToken) ?? ValueTask.CompletedTask;
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        Disposed = true;
        return ValueTask.CompletedTask;
    }
}
