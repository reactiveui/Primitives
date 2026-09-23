// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Provides helpers for observing occasionally connected operations.</summary>
public static class OccasionallyConnectedExtensions
{
    /// <summary>Wraps an observable input source as a startable occasionally connected stream.</summary>
    /// <typeparam name="TInput">The stream input type.</typeparam>
    /// <param name="localSource">The local input source.</param>
    extension<TInput>(IObservable<TInput> localSource)
    {
        /// <summary>Creates an occasionally connected stream wrapper that subscribes to the source only while started.</summary>
        /// <typeparam name="TState">The stream state type.</typeparam>
        /// <param name="context">The context that owns the stream.</param>
        /// <param name="definition">The stream definition, including observer input capture options.</param>
        /// <returns>The wrapped stream.</returns>
        /// <exception cref="ArgumentNullException">An argument is missing.</exception>
        /// <exception cref="InvalidOperationException">The definition is incomplete.</exception>
        /// <exception cref="NotSupportedException">The resolved stream is not a supported library stream implementation.</exception>
        /// <remarks>The wrapper owns its source subscription and producer. The context owns the underlying stream; its <c>Input</c> observer retains that context lifetime.</remarks>
        public IOccasionallyConnectedStream<TState, TInput> ToOccasionallyConnected<TState>(
            IOccasionallyConnectedContext context,
            StreamDefinition<TState, TInput> definition)
        {
            ArgumentExceptionHelper.ThrowIfNull(localSource);
            ArgumentExceptionHelper.ThrowIfNull(context);
            ArgumentExceptionHelper.ThrowIfNull(definition);
            if (definition.Input is not { } input || definition.InputCapture is not { })
            {
                throw new InvalidOperationException("Observer input options and an owned input capture provider are required.");
            }

            input.Validate();
            definition.Validate();
            var stream = context.GetOrCreateStream(definition);
            var publisher = RequireSerializedPublisher(stream);
            return new SourceOccasionallyConnectedStream<TState, TInput>(localSource, stream, definition, publisher);
        }
    }

    /// <summary>Creates remote observer adapters from raw observers.</summary>
    /// <typeparam name="TInput">The remote input type.</typeparam>
    /// <param name="observer">The raw observer that receives decoded committed remote values.</param>
    extension<TInput>(IObserver<TInput> observer)
    {
        /// <summary>Creates a concrete disposable remote observer adapter for a context stream.</summary>
        /// <typeparam name="TState">The local stream state type.</typeparam>
        /// <param name="context">The context that owns the stream.</param>
        /// <param name="definition">The stream definition, including observer input capture options.</param>
        /// <param name="options">The publish options validated against the stream definition.</param>
        /// <returns>The disposable remote observer adapter.</returns>
        /// <exception cref="ArgumentNullException">An argument is missing.</exception>
        /// <exception cref="InvalidOperationException">The definition or options are inconsistent.</exception>
        /// <exception cref="NotSupportedException">The resolved stream is not a supported library stream implementation.</exception>
        public RemoteObserverAdapter<TInput> ToRemoteObserver<TState>(
            IOccasionallyConnectedContext context,
            StreamDefinition<TState, TInput> definition,
            RemotePublishOptions options)
        {
            ArgumentExceptionHelper.ThrowIfNull(observer);
            ArgumentExceptionHelper.ThrowIfNull(context);
            ArgumentExceptionHelper.ThrowIfNull(definition);
            ArgumentExceptionHelper.ThrowIfNull(options);
            if (definition.Input is not { } input || definition.InputCapture is not { } capture)
            {
                throw new InvalidOperationException("Observer input options and an owned input capture provider are required.");
            }

            input.Validate();
            definition.Validate();
            options.Validate();
            if (definition.StreamId != options.StreamId)
            {
                throw new InvalidOperationException("Remote observer publish options StreamId must match the definition StreamId.");
            }

            var stream = context.GetOrCreateStream(definition);
            var publisher = RequireSerializedPublisher(stream);
            return new(new RemoteObserverAdapterDependencies<TInput>
            {
                Observer = observer,
                StreamId = definition.StreamId,
                RemoteMessages = stream.Remote,
                InputOptions = input,
                Capture = capture,
                Publisher = publisher,
                PublishAsync = stream.PublishAsync,
                CreateProducer = static options => new OccasionallyConnectedInputProducer<TInput>(options),
            });
        }
    }

    /// <summary>Provides stream observation helpers.</summary>
    /// <typeparam name="TState">The stream state type.</typeparam>
    /// <typeparam name="TInput">The stream input type.</typeparam>
    /// <param name="stream">The stream.</param>
    extension<TState, TInput>(IOccasionallyConnectedStream<TState, TInput> stream)
    {
        /// <summary>Observes committed local states only when the paired durable queue snapshot has no pending work.</summary>
        /// <returns>The synchronized state observable.</returns>
        /// <exception cref="ArgumentNullException">The stream is missing.</exception>
        /// <exception cref="NotSupportedException">The stream is not a supported library stream implementation.</exception>
        public IObservable<TState> WhereSynchronized()
        {
            ArgumentExceptionHelper.ThrowIfNull(stream);
            var snapshots = RequireCommittedStateQueueSnapshots(stream);
            return new SynchronizedStateObservable<TState>(snapshots.CommittedStateQueueSnapshots);
        }

        /// <summary>Observes paired per-stream pending queue summaries captured at durable mutation boundaries.</summary>
        /// <returns>The pending summary observable.</returns>
        /// <exception cref="ArgumentNullException">The stream is missing.</exception>
        /// <exception cref="NotSupportedException">The stream is not a supported library stream implementation.</exception>
        public IObservable<PendingSyncSummary> ObservePending()
        {
            ArgumentExceptionHelper.ThrowIfNull(stream);
            var snapshots = RequireCommittedStateQueueSnapshots(stream);
            return new PendingSummaryObservable<TState>(snapshots.CommittedStateQueueSnapshots);
        }
    }

    /// <summary>Provides synchronization waiting helpers.</summary>
    /// <param name="engine">The synchronization engine.</param>
    extension(ISyncEngine engine)
    {
        /// <summary>Waits for an operation to synchronize using the system clock.</summary>
        /// <param name="operationId">The operation to observe.</param>
        /// <param name="timeout">The maximum wait, or <see cref="Timeout.InfiniteTimeSpan"/>.</param>
        /// <returns>The synchronization wait.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask AwaitSynchronizedAsync(OperationId operationId, TimeSpan timeout) =>
            engine.AwaitSynchronizedAsync(operationId, timeout, TimeProvider.System, CancellationToken.None);

        /// <summary>Waits for an operation to synchronize using the system clock.</summary>
        /// <param name="operationId">The operation to observe.</param>
        /// <param name="timeout">The maximum wait, or <see cref="Timeout.InfiniteTimeSpan"/>.</param>
        /// <param name="cancellationToken">The token canceling only the wait.</param>
        /// <returns>The synchronization wait.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask AwaitSynchronizedAsync(OperationId operationId, TimeSpan timeout, CancellationToken cancellationToken) =>
            engine.AwaitSynchronizedAsync(operationId, timeout, TimeProvider.System, cancellationToken);

        /// <summary>Waits for an operation to synchronize using the supplied clock.</summary>
        /// <param name="operationId">The operation to observe.</param>
        /// <param name="timeout">The maximum wait, or <see cref="Timeout.InfiniteTimeSpan"/>.</param>
        /// <param name="timeProvider">The clock used to measure the wait.</param>
        /// <returns>The synchronization wait.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public ValueTask AwaitSynchronizedAsync(OperationId operationId, TimeSpan timeout, TimeProvider timeProvider) =>
            engine.AwaitSynchronizedAsync(operationId, timeout, timeProvider, CancellationToken.None);

        /// <summary>Waits for a persisted or newly reported synchronized result.</summary>
        /// <param name="operationId">The operation to observe.</param>
        /// <param name="timeout">The maximum wait, or <see cref="Timeout.InfiniteTimeSpan"/>.</param>
        /// <param name="timeProvider">The clock used to measure the wait.</param>
        /// <param name="cancellationToken">The token canceling only the wait.</param>
        /// <returns>The synchronization wait.</returns>
        /// <exception cref="ArgumentNullException">The engine or clock is missing.</exception>
        /// <exception cref="ArgumentException">The operation identifier is empty.</exception>
        /// <exception cref="ArgumentOutOfRangeException">The timeout is invalid.</exception>
        /// <exception cref="OperationCanceledException">The caller cancels the wait.</exception>
        /// <exception cref="TimeoutException">The wait times out.</exception>
        /// <exception cref="InvalidOperationException">The operation fails or its status source closes without a terminal result.</exception>
        /// <remarks>
        /// Subscribes before querying persisted status so results arriving during lookup are not lost. Timeout and
        /// cancellation release the waiting subscription; they do not stop the engine or cancel the durable operation.
        /// Rejected, dead-lettered, ambiguous and guarantee-expired results fail the wait. Conflict remains pending
        /// until resolution produces a terminal result. The status lookup observes the supplied cancellation token;
        /// a lookup still in progress after timeout may finish independently, and its failure is observed.
        /// </remarks>
        public ValueTask AwaitSynchronizedAsync(
            OperationId operationId,
            TimeSpan timeout,
            TimeProvider timeProvider,
            CancellationToken cancellationToken)
        {
            ArgumentExceptionHelper.ThrowIfNull(engine);
            return new(OperationSynchronizationWaiter.WaitAsync(
                engine.OperationStates,
                token => engine.GetOperationStatusAsync(operationId, token),
                operationId,
                timeout,
                timeProvider,
                cancellationToken));
        }
    }

    /// <summary>Gets the serialized publisher facet for a supported library stream.</summary>
    /// <typeparam name="TState">The state type.</typeparam>
    /// <typeparam name="TInput">The input type.</typeparam>
    /// <param name="stream">The stream.</param>
    /// <returns>The serialized publisher facet.</returns>
    private static IOccasionallyConnectedSerializedInputPublisher RequireSerializedPublisher<TState, TInput>(
        IOccasionallyConnectedStream<TState, TInput> stream) =>
        stream is IOccasionallyConnectedSerializedInputPublisher publisher
            ? publisher
            : throw CreateUnsupportedStreamException();

    /// <summary>Gets the paired snapshot facet for a supported library stream.</summary>
    /// <typeparam name="TState">The state type.</typeparam>
    /// <typeparam name="TInput">The input type.</typeparam>
    /// <param name="stream">The stream.</param>
    /// <returns>The paired snapshot facet.</returns>
    private static IOccasionallyConnectedCommittedStateQueueSnapshots<TState> RequireCommittedStateQueueSnapshots<TState, TInput>(
        IOccasionallyConnectedStream<TState, TInput> stream) =>
        stream is IOccasionallyConnectedCommittedStateQueueSnapshots<TState> snapshots
            ? snapshots
            : throw CreateUnsupportedStreamException();

    /// <summary>Creates the stable unsupported-stream exception used by convenience helpers.</summary>
    /// <returns>The exception.</returns>
    private static NotSupportedException CreateUnsupportedStreamException() =>
        new("This convenience helper supports ReactiveUI.Primitives.OccasionallyConnected library stream implementations only.");
}
