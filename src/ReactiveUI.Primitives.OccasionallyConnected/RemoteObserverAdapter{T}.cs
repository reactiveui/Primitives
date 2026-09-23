// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Adapts a context stream to a disposable remote observer bridge.</summary>
/// <typeparam name="T">The remote input type.</typeparam>
[DebuggerDisplay("StreamId = {_streamId}, Disposed = {_disposed}")]
public sealed class RemoteObserverAdapter<T> : IRemoteObserver<T>, IAsyncDisposable
{
    /// <summary>Guards adapter ownership state.</summary>
    private readonly Lock _gate = new();

    /// <summary>The target stream identity.</summary>
    private readonly StreamId _streamId;

    /// <summary>The typed publish callback.</summary>
    private readonly Func<T, RemotePublishOptions?, CancellationToken, ValueTask<PublishReceipt>> _publishAsync;

    /// <summary>The default input admission settings.</summary>
    private readonly ObserverInputOptions _inputOptions;

    /// <summary>Captures caller input into owned payloads.</summary>
    private readonly IOccasionallyConnectedInputCapture<T> _capture;

    /// <summary>The serialized publisher.</summary>
    private readonly IOccasionallyConnectedSerializedInputPublisher _publisher;

    /// <summary>Creates independently owned input producers.</summary>
    private readonly Func<OccasionallyConnectedInputProducerOptions<T>, IOccasionallyConnectedInputProducer<T>?> _createProducer;

    /// <summary>The owned remote subscription.</summary>
    private readonly IDisposable _remoteSubscription;

    /// <summary>The independently owned observer producers.</summary>
    private readonly List<IOccasionallyConnectedInputProducer<T>> _producers = [];

    /// <summary>Tracks completed producers until their accepted work drains.</summary>
    private readonly List<Task> _releasingProducers = [];

    /// <summary>Signals when in-progress producer creation has finished.</summary>
    private TaskCompletionSource<bool> _producerCreationDrained = CreateCompletedProducerCreationSignal();

    /// <summary>The number of producer factory callbacks currently running.</summary>
    private int _creatingProducers;

    /// <summary>Whether this adapter was disposed.</summary>
    private bool _disposed;

    /// <summary>The shared adapter cleanup task.</summary>
    private Task? _disposeTask;

    /// <summary>Initializes a new instance of the <see cref="RemoteObserverAdapter{T}"/> class.</summary>
    /// <param name="dependencies">The ownership and publication dependencies.</param>
    /// <exception cref="ArgumentNullException">A required collaborator is missing.</exception>
    /// <exception cref="ArgumentException">The stream identifier is empty.</exception>
    internal RemoteObserverAdapter(RemoteObserverAdapterDependencies<T> dependencies)
    {
        ArgumentExceptionHelper.ThrowIfNull(dependencies);
        ArgumentExceptionHelper.ThrowIfNull(dependencies.Observer);
        ArgumentExceptionHelper.ThrowIfNull(dependencies.RemoteMessages);
        ArgumentExceptionHelper.ThrowIfNull(dependencies.InputOptions);
        ArgumentExceptionHelper.ThrowIfNull(dependencies.Capture);
        ArgumentExceptionHelper.ThrowIfNull(dependencies.Publisher);
        ArgumentExceptionHelper.ThrowIfNull(dependencies.PublishAsync);
        ArgumentExceptionHelper.ThrowIfNull(dependencies.CreateProducer);
        if (dependencies.StreamId == default)
        {
            throw new ArgumentException("The stream identifier must not be empty.", nameof(dependencies));
        }

        _streamId = dependencies.StreamId;
        _inputOptions = dependencies.InputOptions;
        _capture = dependencies.Capture;
        _publisher = dependencies.Publisher;
        _publishAsync = dependencies.PublishAsync;
        _createProducer = dependencies.CreateProducer;
        _remoteSubscription = dependencies.RemoteMessages.Subscribe(new RemoteMessageObserver(dependencies.Observer));
    }

    /// <inheritdoc />
    public ValueTask<PublishReceipt> PublishAsync(
        T value,
        RemotePublishOptions options,
        CancellationToken cancellationToken)
    {
        ValidatePublishOptions(options);
        lock (_gate)
        {
            ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);
        }

        return _publishAsync(value, options, cancellationToken);
    }

    /// <inheritdoc />
    public IObserver<T> AsObserver(RemotePublishOptions options, ObserverInputOptions? inputOptions)
    {
        ValidatePublishOptions(options);
        var admission = inputOptions ?? _inputOptions;
        admission.Validate();
        lock (_gate)
        {
            ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);
            if (_creatingProducers == 0)
            {
                _producerCreationDrained = new(TaskCreationOptions.RunContinuationsAsynchronously);
            }

            _creatingProducers++;
        }

        IOccasionallyConnectedInputProducer<T>? producer = null;
        var disposed = false;
        try
        {
            producer = _createProducer(new()
            {
                StreamId = options.StreamId,
                Admission = admission,
                Capture = _capture,
                PublishAsync = _publisher.PublishSerializedInputAsync,
                PublishFault = _publisher.PublishInputFault,
                PublishOptions = options,
            }) ?? throw new InvalidOperationException("The producer factory returned null.");
        }
        finally
        {
            lock (_gate)
            {
                if (producer is not null)
                {
                    _producers.Add(producer);
                }

                disposed = _disposed;
                _creatingProducers--;
                if (_creatingProducers == 0)
                {
                    _ = _producerCreationDrained.TrySetResult(true);
                }
            }
        }

        ObjectDisposedExceptionHelper.ThrowIf(disposed, this);
        return new AdapterInputObserver(this, producer);
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

    /// <summary>Creates a settled creation signal before the first factory is admitted.</summary>
    /// <returns>The completed producer creation signal.</returns>
    private static TaskCompletionSource<bool> CreateCompletedProducerCreationSignal()
    {
        var completion = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        _ = completion.TrySetResult(true);
        return completion;
    }

    /// <summary>Finishes owned cleanup and settles the shared disposal task.</summary>
    /// <param name="completion">The shared completion source.</param>
    /// <returns>The asynchronous cleanup task.</returns>
    private async Task CompleteDisposeAsync(TaskCompletionSource<bool> completion)
    {
        Exception? failure = null;
        try
        {
            _remoteSubscription.Dispose();
        }
        catch (Exception exception)
        {
            failure = exception;
        }

        Task creationDrain;
        lock (_gate)
        {
            creationDrain = _producerCreationDrained.Task;
        }

        await creationDrain.ConfigureAwait(false);

        IOccasionallyConnectedInputProducer<T>[] producers;
        Task[] releasing;
        lock (_gate)
        {
            producers = _producers.ToArray();
            _producers.Clear();
            releasing = _releasingProducers.ToArray();
        }

        for (var i = 0; i < producers.Length; i++)
        {
            try
            {
                await producers[i].DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                failure ??= exception;
            }
        }

        await Task.WhenAll(releasing).ConfigureAwait(false);

        if (failure is null)
        {
            _ = completion.TrySetResult(true);
        }
        else
        {
            _ = completion.TrySetException(failure);
        }
    }

    /// <summary>Transfers a completed producer from active ownership to tracked cleanup.</summary>
    /// <param name="producer">The completed producer.</param>
    private void BeginReleaseProducer(IOccasionallyConnectedInputProducer<T> producer)
    {
        TaskCompletionSource<bool> completion;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _ = _producers.Remove(producer);
            completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            _releasingProducers.Add(completion.Task);
        }

        _ = ReleaseProducerAsync(producer, completion);
    }

    /// <summary>Releases a completed producer after its accepted work drains.</summary>
    /// <param name="producer">The completed producer.</param>
    /// <param name="completion">The tracked cleanup completion.</param>
    /// <returns>The asynchronous release task.</returns>
    private async Task ReleaseProducerAsync(IOccasionallyConnectedInputProducer<T> producer, TaskCompletionSource<bool> completion)
    {
        try
        {
            await producer.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            try
            {
                _publisher.PublishInputFault("OC.Input.CompletedProducer", "A completed adapter producer failed to drain.", null, exception);
            }
            catch (Exception reportException)
            {
                // Diagnostic callback failures cannot escape detached producer cleanup.
                _ = reportException;
            }
        }
        finally
        {
            lock (_gate)
            {
                _ = _releasingProducers.Remove(completion.Task);
            }

            _ = completion.TrySetResult(true);
        }
    }

    /// <summary>Validates that publish options target this adapter stream.</summary>
    /// <param name="options">The publish options.</param>
    /// <exception cref="ArgumentNullException">The options are missing.</exception>
    /// <exception cref="InvalidOperationException">The options target another stream.</exception>
    private void ValidatePublishOptions(RemotePublishOptions options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        options.Validate();
        if (options.StreamId == _streamId)
        {
            return;
        }

        throw new InvalidOperationException("Remote observer publish options StreamId must match the adapter stream.");
    }

    /// <summary>Forwards committed remote values to the raw observer.</summary>
    /// <param name="observer">The downstream observer.</param>
    private sealed class RemoteMessageObserver(IObserver<T> observer) : IObserver<RemoteMessage<T>>
    {
        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => observer.OnCompleted();

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => observer.OnError(error);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(RemoteMessage<T> value) => observer.OnNext(value.Value);
    }

    /// <summary>Forwards input and releases a completed independent producer.</summary>
    /// <param name="owner">The adapter owner.</param>
    /// <param name="producer">The owned producer.</param>
    private sealed class AdapterInputObserver(RemoteObserverAdapter<T> owner, IOccasionallyConnectedInputProducer<T> producer) : IObserver<T>
    {
        /// <summary>Prevents repeated terminal signals from repeating cleanup.</summary>
        private int _terminated;

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(T value) => producer.Observer.OnNext(value);

        /// <inheritdoc />
        public void OnCompleted()
        {
            if (Interlocked.Exchange(ref _terminated, 1) != 0)
            {
                return;
            }

            try
            {
                producer.Observer.OnCompleted();
            }
            finally
            {
                owner.BeginReleaseProducer(producer);
            }
        }

        /// <inheritdoc />
        public void OnError(Exception error)
        {
            if (Interlocked.Exchange(ref _terminated, 1) != 0)
            {
                return;
            }

            try
            {
                producer.Observer.OnError(error);
            }
            finally
            {
                owner.BeginReleaseProducer(producer);
            }
        }
    }
}
