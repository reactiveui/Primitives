// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Wraps an observable source around a context-owned occasionally connected stream.</summary>
/// <typeparam name="TState">The state type.</typeparam>
/// <typeparam name="TInput">The input type.</typeparam>
internal sealed class SourceOccasionallyConnectedStream<TState, TInput> :
    IOccasionallyConnectedStream<TState, TInput>,
    IOccasionallyConnectedSerializedInputPublisher,
    IOccasionallyConnectedCommittedStateQueueSnapshots<TState>
{
    /// <summary>Guards wrapper ownership state.</summary>
    private readonly Lock _gate = new();

    /// <summary>Serializes source lifecycle transitions.</summary>
    private readonly SemaphoreSlim _lifecycle = new(1, 1);

    /// <summary>The local input source.</summary>
    private readonly IObservable<TInput> _source;

    /// <summary>The context-owned stream.</summary>
    private readonly IOccasionallyConnectedStream<TState, TInput> _inner;

    /// <summary>The stream definition.</summary>
    private readonly StreamDefinition<TState, TInput> _definition;

    /// <summary>The serialized publisher of the inner stream.</summary>
    private readonly IOccasionallyConnectedSerializedInputPublisher _publisher;

    /// <summary>The required paired committed-state and queue snapshot facet.</summary>
    private readonly IOccasionallyConnectedCommittedStateQueueSnapshots<TState> _snapshots;

    /// <summary>The validated input admission configuration.</summary>
    private readonly ObserverInputOptions _inputOptions;

    /// <summary>The validated owned input capture provider.</summary>
    private readonly IOccasionallyConnectedInputCapture<TInput> _capture;

    /// <summary>The subscription owned while started.</summary>
    private IDisposable? _sourceSubscription;

    /// <summary>The producer owned while started.</summary>
    private IOccasionallyConnectedInputProducer<TInput>? _sourceProducer;

    /// <summary>Whether the wrapper was disposed.</summary>
    private volatile bool _disposed;

    /// <summary>The shared disposal task.</summary>
    private Task? _disposeTask;

    /// <summary>The accepted start and stop callers that may still use the lifecycle semaphore.</summary>
    private int _lifecycleCallers;

    /// <summary>Signals when all accepted lifecycle callers have left the semaphore.</summary>
    private TaskCompletionSource<bool> _lifecycleCallersDrained = CreateCompletedLifecycleCallerSignal();

    /// <summary>Initializes a new instance of the <see cref="SourceOccasionallyConnectedStream{TState,TInput}"/> class.</summary>
    /// <param name="source">The local input source.</param>
    /// <param name="inner">The context-owned stream.</param>
    /// <param name="definition">The stream definition.</param>
    /// <param name="publisher">The serialized publisher.</param>
    /// <exception cref="ArgumentNullException">A required collaborator is missing.</exception>
    /// <exception cref="NotSupportedException">The stream lacks paired committed-state and queue snapshots.</exception>
    /// <exception cref="InvalidOperationException">The input definition lacks owned producer dependencies.</exception>
    internal SourceOccasionallyConnectedStream(
        IObservable<TInput> source,
        IOccasionallyConnectedStream<TState, TInput> inner,
        StreamDefinition<TState, TInput> definition,
        IOccasionallyConnectedSerializedInputPublisher publisher)
    {
        ArgumentExceptionHelper.ThrowIfNull(source);
        ArgumentExceptionHelper.ThrowIfNull(inner);
        ArgumentExceptionHelper.ThrowIfNull(definition);
        ArgumentExceptionHelper.ThrowIfNull(publisher);
        if (inner is not IOccasionallyConnectedCommittedStateQueueSnapshots<TState> snapshots)
        {
            throw new NotSupportedException("This convenience helper supports ReactiveUI.Primitives.OccasionallyConnected library stream implementations only.");
        }

        if (definition.Input is not { } input || definition.InputCapture is not { } capture)
        {
            throw new InvalidOperationException("Observer input options and an owned input capture provider are required.");
        }

        _source = source;
        _inner = inner;
        _definition = definition;
        _publisher = publisher;
        _snapshots = snapshots;
        _inputOptions = input;
        _capture = capture;
    }

    /// <inheritdoc />
    public StreamId StreamId => _inner.StreamId;

    /// <inheritdoc />
    public SubscriptionId SubscriptionId => _inner.SubscriptionId;

    /// <inheritdoc />
    public IObservable<TState> Local => _inner.Local;

    /// <inheritdoc />
    public IObservable<RemoteMessage<TInput>> Remote => _inner.Remote;

    /// <inheritdoc />
    public IObservable<SyncState> SyncStates => _inner.SyncStates;

    /// <inheritdoc />
    public IObservable<SyncOperationStatus> OperationStates => _inner.OperationStates;

    /// <inheritdoc />
    public IObservable<OccasionallyConnectedFault> Faults => _inner.Faults;

    /// <inheritdoc />
    public IObserver<TInput> Input => _inner.Input;

    /// <inheritdoc />
    public IObservable<OccasionallyConnectedCommittedStateQueueSnapshot<TState>> CommittedStateQueueSnapshots =>
        _snapshots.CommittedStateQueueSnapshots;

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<PublishReceipt> PublishAsync(TInput value, RemotePublishOptions? options, CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
        }

        return _inner.PublishAsync(value, options, cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask StartAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
            if (_lifecycleCallers == 0)
            {
                _lifecycleCallersDrained = new(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            _lifecycleCallers++;
        }

        var entered = false;
        try
        {
            await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
            entered = true;
            ThrowIfDisposed();
            if (_sourceSubscription is not null)
            {
                return;
            }

            await _inner.StartAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await StartSourceSubscriptionAsync().ConfigureAwait(false);
            }
            catch (Exception sourceException)
            {
                try
                {
                    await _inner.StopAsync(CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception cleanupException)
                {
                    throw new AggregateException(sourceException, cleanupException);
                }

                throw;
            }
        }
        finally
        {
            if (entered)
            {
                _ = _lifecycle.Release();
            }

            CompleteLifecycleCaller();
        }
    }

    /// <inheritdoc />
    public async ValueTask StopAsync(CancellationToken cancellationToken)
    {
        Task? disposal;
        lock (_gate)
        {
            disposal = _disposeTask;
            if (disposal is null)
            {
                if (_lifecycleCallers == 0)
                {
                    _lifecycleCallersDrained = new(TaskCreationOptions.RunContinuationsAsynchronously);
                }

                _lifecycleCallers++;
            }
        }

        if (disposal is not null)
        {
            await disposal.ConfigureAwait(false);
            return;
        }

        var entered = false;
        try
        {
            await _lifecycle.WaitAsync(cancellationToken).ConfigureAwait(false);
            entered = true;
            await StopOwnedAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            if (entered)
            {
                _ = _lifecycle.Release();
            }

            CompleteLifecycleCaller();
        }
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
            _ = CompleteDisposeAsync(completion);
        }

        return new(task);
    }

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask<PublishReceipt> PublishSerializedInputAsync(
        PayloadEnvelope payload,
        RemotePublishOptions? options,
        CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            ThrowIfDisposed();
        }

        return _publisher.PublishSerializedInputAsync(payload, options, cancellationToken);
    }

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void PublishInputFault(string code, string message, OperationId? operationId, Exception exception) =>
        _publisher.PublishInputFault(code, message, operationId, exception);

    /// <summary>Creates the completed caller-drain signal used before any lifecycle caller is admitted.</summary>
    /// <returns>The completed lifecycle caller signal.</returns>
    private static TaskCompletionSource<bool> CreateCompletedLifecycleCallerSignal()
    {
        var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = completion.TrySetResult(true);
        return completion;
    }

    /// <summary>Subscribes to the source with a fresh producer.</summary>
    /// <returns>A task that completes after subscription cleanup if source registration fails.</returns>
    /// <exception cref="AggregateException">Source registration and producer cleanup both fail.</exception>
    private async ValueTask StartSourceSubscriptionAsync()
    {
        var producer = CreateSourceProducer();
        try
        {
            var subscription = _source.Subscribe(producer.Observer);
            _sourceProducer = producer;
            _sourceSubscription = subscription;
        }
        catch (Exception subscribeException)
        {
            try
            {
                await producer.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception cleanupException)
            {
                throw new AggregateException(subscribeException, cleanupException);
            }

            throw;
        }
    }

    /// <summary>Disposes the current source subscription and producer.</summary>
    /// <returns>A task that completes when the producer is disposed.</returns>
    /// <exception cref="AggregateException">Subscription and producer cleanup both fail.</exception>
    private async ValueTask StopSourceSubscriptionAsync()
    {
        var subscription = _sourceSubscription;
        var producer = _sourceProducer;
        _sourceSubscription = null;
        _sourceProducer = null;
        Exception? failure = null;
        try
        {
            subscription?.Dispose();
        }
        catch (Exception exception)
        {
            failure = exception;
        }

        if (producer is not null)
        {
            try
            {
                await producer.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                if (failure is not null)
                {
                    throw new AggregateException(failure, exception);
                }

                throw;
            }
        }

        if (failure is not null)
        {
            ExceptionDispatchInfo.Capture(failure).Throw();
        }
    }

    /// <summary>Stops the source and inner lifecycle while preserving both cleanup failures.</summary>
    /// <param name="cancellationToken">The inner stop token.</param>
    /// <returns>A task that completes after both cleanup paths finish.</returns>
    /// <exception cref="AggregateException">Both the source and inner stream cleanup fail.</exception>
    private async ValueTask StopOwnedAsync(CancellationToken cancellationToken)
    {
        Exception? failure = null;
        try
        {
            await StopSourceSubscriptionAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failure = exception;
        }

        try
        {
            await _inner.StopAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            if (failure is not null)
            {
                throw new AggregateException(failure, exception);
            }

            throw;
        }

        if (failure is not null)
        {
            ExceptionDispatchInfo.Capture(failure).Throw();
        }
    }

    /// <summary>Completes shared disposal after the current lifecycle transition finishes.</summary>
    /// <param name="completion">The shared completion source.</param>
    /// <returns>The asynchronous completion task.</returns>
    private async Task CompleteDisposeAsync(TaskCompletionSource<bool> completion)
    {
        Exception? failure = null;
        await _lifecycle.WaitAsync().ConfigureAwait(false);
        try
        {
            await StopOwnedAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            failure = exception;
        }
        finally
        {
            _ = _lifecycle.Release();
        }

        Task callersDrained;
        lock (_gate)
        {
            callersDrained = _lifecycleCallersDrained.Task;
        }

        await callersDrained.ConfigureAwait(false);
        _lifecycle.Dispose();
        if (failure is null)
        {
            _ = completion.TrySetResult(true);
        }
        else
        {
            _ = completion.TrySetException(failure);
        }
    }

    /// <summary>Releases one accepted lifecycle caller after it leaves the semaphore.</summary>
    private void CompleteLifecycleCaller()
    {
        TaskCompletionSource<bool>? drained = null;
        lock (_gate)
        {
            _lifecycleCallers--;
            if (_lifecycleCallers == 0)
            {
                drained = _lifecycleCallersDrained;
            }
        }

        _ = drained?.TrySetResult(true);
    }

    /// <summary>Creates a new producer with owned input capture.</summary>
    /// <returns>The producer.</returns>
    private OccasionallyConnectedInputProducer<TInput> CreateSourceProducer() =>
        new(new()
        {
            StreamId = StreamId,
            Admission = _inputOptions,
            Capture = _capture,
            PublishAsync = _publisher.PublishSerializedInputAsync,
            PublishFault = _publisher.PublishInputFault,
            PublishOptions = _definition.Publish,
        });

    /// <summary>Rejects use after disposal.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed() => ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);
}
