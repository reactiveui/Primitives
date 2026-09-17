// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Captures observer input into owned serialized payloads and publishes them asynchronously.</summary>
/// <typeparam name="TInput">The input value type.</typeparam>
internal sealed partial class OccasionallyConnectedInputProducer<TInput> : IOccasionallyConnectedInputProducer<TInput>
{
    /// <summary>Protects admission, ticket order, running work, and terminal state.</summary>
    private readonly Lock _gate = new();

    /// <summary>Stores immutable producer options.</summary>
    private readonly OccasionallyConnectedInputProducerOptions<TInput> _options;

    /// <summary>Creates the observer exposed to the public stream facade.</summary>
    private readonly Lazy<InputObserver> _observer;

    /// <summary>Stores accepted tickets in producer admission order.</summary>
    private readonly LinkedList<InputTicket> _tickets = [];

    /// <summary>Stores the shared disposal completion for admitted work draining.</summary>
    private readonly TaskCompletionSource<bool> _disposeCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Tracks the admitted active capture, queued, and in-flight publish count.</summary>
    private int _admittedCount;

    /// <summary>Tracks fault callbacks that must drain before disposal completes.</summary>
    private int _callbackCount;

    /// <summary>Tracks observer calls that own caller input before admission or rejection completes.</summary>
    private int _observerCallCount;

    /// <summary>Tracks declared retained bytes for active capture, queued, and in-flight publish work.</summary>
    private long _admittedBytes;

    /// <summary>Tracks whether producer input is closed to later OnNext calls.</summary>
    private bool _closed;

    /// <summary>Tracks whether the ordered publish pump is running.</summary>
    private bool _pumpRunning;

    /// <summary>Stores a terminal pump failure that must be observed by disposal.</summary>
    private Exception? _terminalFailure;

    /// <summary>Initializes a new instance of the input producer.</summary>
    /// <param name="options">The producer options.</param>
    internal OccasionallyConnectedInputProducer(OccasionallyConnectedInputProducerOptions<TInput> options)
    {
        ArgumentExceptionHelper.ThrowIfNull(options);
        options.Validate();
        _options = options;
        _observer = new(CreateObserver);
    }

    /// <summary>Describes the result of admission reservation.</summary>
    private enum ReservationResult
    {
        /// <summary>The input was admitted.</summary>
        Admitted = 0,

        /// <summary>The producer was already closed.</summary>
        Closed = 1,

        /// <summary>The producer buffer was full.</summary>
        Overflow = 2,
    }

    /// <inheritdoc />
    public IObserver<TInput> Observer => _observer.Value;

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            _closed = true;
            CompleteDisposeIfDrained();
        }

        return new(_disposeCompletion.Task);
    }

    /// <summary>Creates the observer lazily after construction has completed.</summary>
    /// <returns>The input observer.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private InputObserver CreateObserver() => new(this);

    /// <summary>Closes the producer input path after a completion signal.</summary>
    private void Complete()
    {
        lock (_gate)
        {
            _closed = true;
            CompleteDisposeIfDrained();
        }
    }

    /// <summary>Closes the producer input path after an error signal and publishes a bounded fault.</summary>
    /// <param name="error">The observer error.</param>
    private void Fail(Exception error)
    {
        ArgumentExceptionHelper.ThrowIfNull(error);
        bool shouldPublish;
        lock (_gate)
        {
            shouldPublish = !_closed;
            _closed = true;
            if (shouldPublish)
            {
                AddFaultCallbackOwner();
            }

            CompleteDisposeIfDrained();
        }

        if (!shouldPublish)
        {
            return;
        }

        PublishOwnedFaultSafely(
            InputProducerFaultCode,
            "The observer input producer was terminated by OnError.",
            null,
            error);
    }

    /// <summary>Captures one caller input synchronously after reserving retained capacity.</summary>
    /// <param name="value">The caller input.</param>
    private void PublishInput(TInput value)
    {
        if (!TryEnterObserverCall())
        {
            return;
        }

        try
        {
            if (!TryGetDeclaredRetainedByteCount(value, out var declaredBytes))
            {
                return;
            }

            var reservation = TryReserve(declaredBytes, out var reservedTicket);
            if (reservation == ReservationResult.Closed)
            {
                return;
            }

            if (reservation == ReservationResult.Overflow)
            {
                PublishFaultSafely(
                    InputOverflowFaultCode,
                    "The observer input producer rejected input before capture because its retained buffer is full.",
                    null,
                    new InvalidOperationException("The observer input producer retained buffer is full."));
                return;
            }

            ArgumentExceptionHelper.ThrowIfNull(reservedTicket);
            CaptureReservedInput(value, reservedTicket);
        }
        finally
        {
            ExitObserverCall();
        }
    }

    /// <summary>Captures one input for an admitted FIFO ticket.</summary>
    /// <param name="value">The caller input.</param>
    /// <param name="ticket">The reserved FIFO ticket.</param>
    /// <exception cref="InvalidOperationException">The captured payload is invalid.</exception>
    private void CaptureReservedInput(TInput value, InputTicket ticket)
    {
        PayloadEnvelope payload;
        try
        {
            payload = _options.Capture.Capture(value)
                ?? throw new InvalidOperationException("The input capture provider returned a null payload envelope.");

            if (GetRetainedEnvelopeByteCount(payload) > ticket.DeclaredBytes)
            {
                throw new InvalidOperationException("The captured envelope exceeded its declared retained-byte bound.");
            }
        }
        catch (Exception exception)
        {
            FailTicketWithFault(ticket, exception);
            return;
        }

        CompleteTicketCapture(ticket, payload);
    }

    /// <summary>Attempts to get a positive retained-byte declaration before reserving admission.</summary>
    /// <param name="value">The caller input.</param>
    /// <param name="declaredBytes">The declared retained-byte count.</param>
    /// <returns><see langword="true"/> when a valid declaration was read; otherwise, <see langword="false"/>.</returns>
    private bool TryGetDeclaredRetainedByteCount(TInput value, out long declaredBytes)
    {
        try
        {
            declaredBytes = _options.Capture.GetRetainedByteCount(value);
        }
        catch (Exception exception)
        {
            declaredBytes = 0;
            PublishFaultSafely(
                InputCaptureFaultCode,
                "The observer input capture provider failed to size input.",
                null,
                exception);
            return false;
        }

        if (declaredBytes >= MinimumRetainedInputBytes)
        {
            return true;
        }

        PublishFaultSafely(
            InputCaptureFaultCode,
            "The observer input capture provider declared an invalid retained-byte bound.",
            null,
            new InvalidOperationException("The declared retained-byte bound must be positive."));
        return false;
    }

    /// <summary>Attempts to reserve retained capacity before capture can copy caller-owned input.</summary>
    /// <param name="declaredBytes">The declared retained byte count.</param>
    /// <param name="ticket">The reserved FIFO ticket.</param>
    /// <returns>The reservation result.</returns>
    private ReservationResult TryReserve(long declaredBytes, out InputTicket? ticket)
    {
        lock (_gate)
        {
            if (_closed)
            {
                ticket = null;
                return ReservationResult.Closed;
            }

            if (declaredBytes > _options.Admission.BufferCapacityBytes)
            {
                ticket = null;
                return ReservationResult.Overflow;
            }

            while (!HasCapacityFor(declaredBytes) && ShouldTryDropOldest())
            {
                if (!TryDropOldestQueuedNonDurableTicket())
                {
                    break;
                }
            }

            if (!HasCapacityFor(declaredBytes))
            {
                ticket = null;
                return ReservationResult.Overflow;
            }

            ticket = new(declaredBytes, IsDurablePublish());
            ticket.Node = _tickets.AddLast(ticket);
            _admittedCount++;
            _admittedBytes += declaredBytes;
            return ReservationResult.Admitted;
        }
    }

    /// <summary>Marks a reserved ticket as failed and owns its fault callback before releasing disposal.</summary>
    /// <param name="ticket">The failed ticket.</param>
    /// <param name="exception">The capture failure.</param>
    private void FailTicketWithFault(InputTicket ticket, Exception exception)
    {
        lock (_gate)
        {
            RemoveTicketNode(ticket);
            ReleaseTicket(ticket);
            AddFaultCallbackOwner();
            CompleteDisposeIfDrained();
        }

        SchedulePump();
        PublishOwnedFaultSafely(InputCaptureFaultCode, "The observer input capture provider failed.", null, exception);
    }

    /// <summary>Marks a reserved ticket ready with its owned serialized payload.</summary>
    /// <param name="ticket">The reserved ticket.</param>
    /// <param name="payload">The owned serialized payload.</param>
    private void CompleteTicketCapture(InputTicket ticket, PayloadEnvelope payload)
    {
        lock (_gate)
        {
            if (_terminalFailure is not null)
            {
                RemoveTicketNode(ticket);
                ReleaseTicket(ticket);
                CompleteDisposeIfDrained();
                return;
            }

            ticket.Payload = payload;
            ticket.Ready = true;
        }

        SchedulePump();
    }

    /// <summary>Schedules the ordered publish pump when needed.</summary>
    private void SchedulePump()
    {
        lock (_gate)
        {
            if (_pumpRunning || _terminalFailure is not null)
            {
                return;
            }

            _pumpRunning = true;
            _ = Task.Run(RunPumpAsync);
        }
    }

    /// <summary>Publishes captured tickets in FIFO order and releases ownership before fault callbacks.</summary>
    /// <returns>The pump task.</returns>
    private async Task RunPumpAsync()
    {
        try
        {
            while (true)
            {
                var ticket = TryTakeReadyTicket();
                if (ticket is null)
                {
                    return;
                }

                await PublishTicketAsync(ticket).ConfigureAwait(false);
            }
        }
        catch (Exception exception)
        {
            ReportTerminalPumpFailure(exception);
        }
    }

    /// <summary>Gets the next ready ticket in FIFO order or releases the pump when the head is not ready.</summary>
    /// <returns>The ready ticket, or null when the pump should stop.</returns>
    private InputTicket? TryTakeReadyTicket()
    {
        lock (_gate)
        {
            var node = _tickets.First;
            if (node is null)
            {
                _pumpRunning = false;
                CompleteDisposeIfDrained();
                return null;
            }

            var ticket = node.Value;
            if (!ticket.Ready)
            {
                _pumpRunning = false;
                CompleteDisposeIfDrained();
                return null;
            }

            _tickets.Remove(node);
            ticket.Node = null;
            ticket.Publishing = true;
            return ticket;
        }
    }

    /// <summary>Publishes one captured ticket and releases ownership before reporting publish failure.</summary>
    /// <param name="ticket">The captured ticket.</param>
    /// <returns>The publish task.</returns>
    /// <exception cref="ArgumentNullException">The ready ticket is missing its payload.</exception>
    private async ValueTask PublishTicketAsync(InputTicket ticket)
    {
        Exception? publishFailure = null;
        try
        {
            var payload = ticket.Payload;
            ArgumentExceptionHelper.ThrowIfNull(payload);

            _ = await _options.PublishAsync(payload, _options.PublishOptions, CancellationToken.None)
                .ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            publishFailure = exception;
        }

        var ownsFaultCallback = publishFailure is not null;
        lock (_gate)
        {
            ticket.Publishing = false;
            ReleaseTicket(ticket);
            if (ownsFaultCallback)
            {
                AddFaultCallbackOwner();
            }

            CompleteDisposeIfDrained();
        }

        if (publishFailure is not null)
        {
            PublishOwnedFaultSafely(
                InputPublishFaultCode,
                "The observer input producer failed to publish captured input.",
                null,
                publishFailure);
        }
    }

    /// <summary>Attempts to evict the oldest captured non-durable ticket waiting behind the FIFO head.</summary>
    /// <returns><see langword="true"/> when a ticket was evicted; otherwise, <see langword="false"/>.</returns>
    private bool TryDropOldestQueuedNonDurableTicket()
    {
        var node = _tickets.First;
        while (node is not null)
        {
            var current = node;
            node = node.Next;
            var ticket = current.Value;
            if (!ticket.Ready || ticket.Publishing || ticket.Durable)
            {
                continue;
            }

            _tickets.Remove(current);
            ticket.Node = null;
            ReleaseTicket(ticket);
            return true;
        }

        return false;
    }

    /// <summary>Removes a ticket from the linked FIFO list when it still owns a node.</summary>
    /// <param name="ticket">The ticket to remove.</param>
    private void RemoveTicketNode(InputTicket ticket)
    {
        var node = ticket.Node;
        Debug.Assert(node is not null, "Only linked tickets can be removed from the producer queue.");
        _tickets.Remove(node);
        ticket.Node = null;
    }

    /// <summary>Releases a retained admission charge exactly once.</summary>
    /// <param name="ticket">The completed ticket.</param>
    private void ReleaseTicket(InputTicket ticket)
    {
        Debug.Assert(!ticket.Released, "Producer admission tickets must be released exactly once.");
        ticket.Released = true;
        _admittedCount--;
        _admittedBytes -= ticket.DeclaredBytes;
    }

    /// <summary>Completes disposal when all admitted work, callbacks, and pump work have drained.</summary>
    private void CompleteDisposeIfDrained()
    {
        if (IsDisposeWaitingForDrain())
        {
            return;
        }

        CompleteDispose();
    }

    /// <summary>Determines whether disposal must wait for producer work to drain.</summary>
    /// <returns><see langword="true"/> when producer work is still active.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsDisposeWaitingForDrain() =>
        !_closed
        || _admittedCount != 0
        || _callbackCount != 0
        || _observerCallCount != 0
        || _pumpRunning;

    /// <summary>Completes disposal successfully or with the retained terminal pump failure.</summary>
    private void CompleteDispose()
    {
        if (_terminalFailure is { } failure)
        {
            _ = _disposeCompletion.TrySetException(failure);
            return;
        }

        _ = _disposeCompletion.TrySetResult(true);
    }

    /// <summary>Records a terminal pump failure after diagnostic fault ownership has been registered.</summary>
    /// <param name="exception">The pump failure.</param>
    private void ReportTerminalPumpFailure(Exception exception)
    {
        ArgumentExceptionHelper.ThrowIfNull(exception);
        var terminalFailure = exception;
        lock (_gate)
        {
            _closed = true;
            AddFaultCallbackOwner();
        }

        try
        {
            PublishOwnedFaultSafely(
                InputPublishFaultCode,
                "The observer input producer publish pump failed.",
                null,
                exception);
        }
        catch (Exception callbackException) when (IsFatalException(callbackException))
        {
            terminalFailure = callbackException;
        }

        lock (_gate)
        {
            _terminalFailure ??= terminalFailure;
            _pumpRunning = false;
            ReleaseReadyPendingTickets();
            CompleteDisposeIfDrained();
        }
    }

    /// <summary>Releases captured tickets abandoned after a terminal pump failure.</summary>
    private void ReleaseReadyPendingTickets()
    {
        var node = _tickets.First;
        while (node is not null)
        {
            var current = node;
            node = node.Next;
            var ticket = current.Value;
            if (!ticket.Ready)
            {
                continue;
            }

            _tickets.Remove(current);
            ticket.Node = null;
            ticket.Publishing = false;
            ReleaseTicket(ticket);
        }
    }

    /// <summary>Determines whether a new ticket can fit within the retained input capacity.</summary>
    /// <param name="declaredBytes">The declared retained byte count.</param>
    /// <returns><see langword="true"/> when the item fits.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool HasCapacityFor(long declaredBytes) =>
        _admittedCount < _options.Admission.BufferCapacity
        && declaredBytes <= _options.Admission.BufferCapacityBytes - _admittedBytes;

    /// <summary>Determines whether this reserve attempt may evict a queued non-durable ticket.</summary>
    /// <returns><see langword="true"/> when DropOldest eviction is enabled.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool ShouldTryDropOldest() => _options.Admission.BufferStrategy == BufferStrategy.DropOldest;

    /// <summary>Determines whether producer publish options represent durable work.</summary>
    /// <returns><see langword="true"/> when captured input must be preserved once admitted.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool IsDurablePublish() => _options.PublishOptions is not { Durable: false };

    /// <summary>Attempts to own one observer call before sizing or admission can observe caller input.</summary>
    /// <returns><see langword="true"/> when the observer call was accepted.</returns>
    private bool TryEnterObserverCall()
    {
        lock (_gate)
        {
            if (_closed)
            {
                return false;
            }

            _observerCallCount++;
            return true;
        }
    }

    /// <summary>Releases observer-call ownership and completes disposal when this was the last activity.</summary>
    private void ExitObserverCall()
    {
        lock (_gate)
        {
            _observerCallCount--;
            CompleteDisposeIfDrained();
        }
    }

    /// <summary>Publishes a bounded producer fault without letting callbacks escape.</summary>
    /// <param name="code">The stable fault code.</param>
    /// <param name="message">The fault message.</param>
    /// <param name="operationId">The optional operation identifier.</param>
    /// <param name="exception">The observed exception.</param>
    private void PublishFaultSafely(string code, string message, OperationId? operationId, Exception exception)
    {
        lock (_gate)
        {
            AddFaultCallbackOwner();
        }

        PublishOwnedFaultSafely(code, message, operationId, exception);
    }

    /// <summary>Publishes a bounded producer fault using a callback owner already counted under the gate.</summary>
    /// <param name="code">The stable fault code.</param>
    /// <param name="message">The fault message.</param>
    /// <param name="operationId">The optional operation identifier.</param>
    /// <param name="exception">The observed exception.</param>
    /// <exception cref="Exception">Fatal callback failures are allowed to propagate after ownership is released.</exception>
    private void PublishOwnedFaultSafely(string code, string message, OperationId? operationId, Exception exception)
    {
        try
        {
            _options.PublishFault(code, message, operationId, CreateDiagnosticException(exception));
        }
        catch (Exception callbackException) when (!IsFatalException(callbackException))
        {
            // Fault publication is diagnostic only; application callback exceptions cannot escape the producer.
        }
        finally
        {
            lock (_gate)
            {
                _callbackCount--;
                CompleteDisposeIfDrained();
            }
        }
    }

    /// <summary>Registers ownership of a fault callback while the producer gate is held.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AddFaultCallbackOwner() => _callbackCount++;

    /// <summary>Forwards observer calls to the owning input producer.</summary>
    /// <param name="owner">The owning input producer.</param>
    private sealed class InputObserver(OccasionallyConnectedInputProducer<TInput> owner) : IObserver<TInput>
    {
        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnCompleted() => owner.Complete();

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnError(Exception error) => owner.Fail(error);

        /// <inheritdoc />
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void OnNext(TInput value) => owner.PublishInput(value);
    }

    /// <summary>Stores one admitted producer ticket.</summary>
    /// <param name="declaredBytes">The declared retained-byte charge.</param>
    /// <param name="durable">Whether this ticket represents durable, non-evictable work.</param>
    private sealed class InputTicket(long declaredBytes, bool durable)
    {
        /// <summary>Gets the declared retained-byte charge.</summary>
        public long DeclaredBytes { get; } = declaredBytes;

        /// <summary>Gets a value indicating whether the ticket represents durable work.</summary>
        public bool Durable { get; } = durable;

        /// <summary>Gets or sets the linked FIFO node owned by this ticket.</summary>
        public LinkedListNode<InputTicket>? Node { get; set; }

        /// <summary>Gets or sets the owned serialized payload.</summary>
        public PayloadEnvelope? Payload { get; set; }

        /// <summary>Gets or sets a value indicating whether capture completed for this ticket.</summary>
        public bool Ready { get; set; }

        /// <summary>Gets or sets a value indicating whether the ticket is being published.</summary>
        public bool Publishing { get; set; }

        /// <summary>Gets or sets a value indicating whether the retained charge was released.</summary>
        public bool Released { get; set; }
    }
}
