// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Storage.Sqlite;

/// <summary>Runs bounded synchronous SQLite commands on a single FIFO worker.</summary>
internal sealed class SqliteSynchronousCommandWorker : IAsyncDisposable
{
    /// <summary>Protects admission, queue, and lifecycle state.</summary>
    private readonly object _gate = new();

    /// <summary>Stores admitted commands in FIFO order.</summary>
    private readonly LinkedList<WorkItem> _commands = [];

    /// <summary>Runs admitted commands serially.</summary>
    private readonly Task _processingTask;

    /// <summary>The maximum admitted command count, including the active command.</summary>
    private readonly int _capacity;

    /// <summary>The maximum caller-declared retained bytes, including the active command.</summary>
    private readonly long _capacityBytes;

    /// <summary>Completes when the full disposal transition has finished.</summary>
    private TaskCompletionSource<bool>? _disposeCompletion;

    /// <summary>The currently admitted command count, including the active command.</summary>
    private int _admittedCount;

    /// <summary>The caller-declared retained bytes for currently admitted commands, including the active command.</summary>
    private long _admittedBytes;

    /// <summary>A value indicating whether admission has closed.</summary>
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="SqliteSynchronousCommandWorker"/> class.</summary>
    /// <param name="capacity">The maximum admitted command count, including the active command.</param>
    /// <param name="capacityBytes">The maximum caller-declared retained bytes, including the active command.</param>
    /// <exception cref="ArgumentOutOfRangeException">A capacity value is not positive.</exception>
    internal SqliteSynchronousCommandWorker(int capacity, long capacityBytes)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(capacity);
        ThrowIfNegativeOrZero(capacityBytes, nameof(capacityBytes));
        _capacity = capacity;
        _capacityBytes = capacityBytes;
        _processingTask = Task.Factory.StartNew(ProcessCommands, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        var queued = StartDisposal(out var completion);
        if (queued is not null)
        {
            _ = CompleteDisposalAsync(queued, completion);
        }

        return new(completion.Task);
    }

    /// <summary>Runs a synchronous command when it reaches the FIFO head.</summary>
    /// <typeparam name="TResult">The command result type.</typeparam>
    /// <param name="command">The command to run.</param>
    /// <param name="retainedBytes">The caller-declared retained bytes for bounded admission.</param>
    /// <param name="cancellationToken">The caller cancellation token.</param>
    /// <returns>The command result task.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="command"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="retainedBytes"/> is not positive.</exception>
    /// <exception cref="ObjectDisposedException">Admission has closed.</exception>
    /// <exception cref="OperationCanceledException">The command is canceled before dispatch.</exception>
    /// <exception cref="QueueCapacityExceededException">The command does not fit current worker bounds.</exception>
    internal Task<TResult> ExecuteAsync<TResult>(
        Func<CancellationToken, TResult> command,
        long retainedBytes,
        CancellationToken cancellationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(command);
        ThrowIfNegativeOrZero(retainedBytes, nameof(retainedBytes));
        cancellationToken.ThrowIfCancellationRequested();

        var completion = new CommandCompletion<TResult>(command);
        var item = new WorkItem(
            this,
            completion.Invoke,
            completion.Complete,
            completion.CompleteCanceled,
            completion.CompleteDisposed,
            retainedBytes,
            cancellationToken);
        lock (_gate)
        {
            ThrowIfDisposed();
            cancellationToken.ThrowIfCancellationRequested();
            if (!CanAdmit(retainedBytes))
            {
                throw CreateCapacityException(retainedBytes);
            }

            item.Node = _commands.AddLast(item);
            _admittedCount++;
            _admittedBytes += retainedBytes;
        }

        item.RegisterCancellation();
        lock (_gate)
        {
            Monitor.Pulse(_gate);
        }

        return completion.Task;
    }

    /// <summary>Completes commands removed during disposal.</summary>
    /// <param name="queued">The removed commands.</param>
    private static void CompleteQueuedAsDisposed(RemovedWorkItem[] queued)
    {
        var exception = new ObjectDisposedException(nameof(SqliteSynchronousCommandWorker));
        for (var i = 0; i < queued.Length; i++)
        {
            queued[i].DisposeRegistrationIfNeeded();
            queued[i].Item.CompleteDisposed(exception);
        }
    }

    /// <summary>Throws when a long value is not positive.</summary>
    /// <param name="value">The value to validate.</param>
    /// <param name="paramName">The parameter name.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="value"/> is not positive.</exception>
    private static void ThrowIfNegativeOrZero(long value, string paramName)
    {
        if (value > 0)
        {
            return;
        }

        throw new ArgumentOutOfRangeException(paramName, value, null);
    }

    /// <summary>Disposes a detached registration when one exists.</summary>
    /// <param name="registration">The detached registration.</param>
    /// <param name="disposeRegistration">Whether the registration should be disposed.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void DisposeRegistrationIfNeeded(
        CancellationTokenRegistration registration,
        bool disposeRegistration)
    {
        if (!disposeRegistration)
        {
            return;
        }

        registration.Dispose();
    }

    /// <summary>Creates an immediate capacity rejection.</summary>
    /// <param name="retainedBytes">The rejected caller-declared retained byte count.</param>
    /// <returns>The capacity exception.</returns>
    private QueueCapacityExceededException CreateCapacityException(long retainedBytes) =>
        new("The SQLite command worker has reached its configured capacity.", retainedBytes <= _capacityBytes);

    /// <summary>Returns whether a command fits the current admitted count and byte bounds.</summary>
    /// <param name="retainedBytes">The caller-declared retained bytes.</param>
    /// <returns><see langword="true"/> when the command can be admitted; otherwise, <see langword="false"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool CanAdmit(long retainedBytes) => _admittedCount < _capacity && retainedBytes <= _capacityBytes - _admittedBytes;

    /// <summary>Starts disposal once and returns queued commands for the winning caller to complete.</summary>
    /// <param name="completion">The shared disposal completion.</param>
    /// <returns>The queued commands for the winning caller, or null when disposal already started.</returns>
    private RemovedWorkItem[]? StartDisposal(out TaskCompletionSource<bool> completion)
    {
        lock (_gate)
        {
            if (_disposeCompletion is not null)
            {
                completion = _disposeCompletion;
                return null;
            }

            _disposeCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);
            completion = _disposeCompletion;
            _disposed = true;
            var queued = new RemovedWorkItem[_commands.Count];
            var index = 0;
            for (var node = _commands.First; node is not null; node = node.Next)
            {
                var item = node.Value;
                item.Node = null;
                ReleaseAdmittedCore(item);
                var disposeRegistration = item.TryMarkCompletionStarted(out var registration);
                queued[index] = new(item, registration, disposeRegistration);
                index++;
            }

            _commands.Clear();
            Monitor.Pulse(_gate);
            return queued;
        }
    }

    /// <summary>Completes queued work, joins the processing task, and settles shared disposal.</summary>
    /// <param name="queued">The queued commands removed by disposal.</param>
    /// <param name="completion">The shared disposal completion.</param>
    /// <returns>The asynchronous disposal task.</returns>
    private async Task CompleteDisposalAsync(RemovedWorkItem[] queued, TaskCompletionSource<bool> completion)
    {
        CompleteQueuedAsDisposed(queued);
        await _processingTask.ConfigureAwait(false);
        _ = completion.TrySetResult(true);
    }

    /// <summary>Processes queued commands until disposal drains the worker.</summary>
    private void ProcessCommands()
    {
        while (true)
        {
            var item = WaitForNextCommand();
            if (item is null)
            {
                return;
            }

            item.Invoke();
            FinishInvoked(item);
            item.Complete();
        }
    }

    /// <summary>Waits for and removes the next command to dispatch.</summary>
    /// <returns>The next command, or null when disposal has drained the queue.</returns>
    private WorkItem? WaitForNextCommand()
    {
        lock (_gate)
        {
            while (_commands.First is null)
            {
                if (_disposed)
                {
                    return null;
                }

                _ = Monitor.Wait(_gate);
            }

            var item = _commands.First.Value;
            _commands.RemoveFirst();
            item.Node = null;
            return item;
        }
    }

    /// <summary>Releases active command capacity and marks completion outside the gate.</summary>
    /// <param name="item">The command whose capacity should be released.</param>
    private void FinishInvoked(WorkItem item)
    {
        bool disposeRegistration;
        CancellationTokenRegistration registration;
        lock (_gate)
        {
            ReleaseAdmittedCore(item);
            disposeRegistration = item.TryMarkCompletionStarted(out registration);
        }

        DisposeRegistrationIfNeeded(registration, disposeRegistration);
    }

    /// <summary>Releases admitted count and byte state while the gate is held.</summary>
    /// <param name="item">The command whose capacity should be released.</param>
    private void ReleaseAdmittedCore(WorkItem item)
    {
        _admittedCount--;
        _admittedBytes -= item.RetainedBytes;
    }

    /// <summary>Tries to cancel a command that has not reached dispatch.</summary>
    /// <param name="item">The command to cancel.</param>
    /// <returns><see langword="true"/> when queued work was removed; otherwise, <see langword="false"/>.</returns>
    private bool TryCancelQueued(WorkItem item)
    {
        bool disposeRegistration;
        CancellationTokenRegistration registration;
        lock (_gate)
        {
            if (item.Node is null)
            {
                return false;
            }

            _commands.Remove(item.Node);
            item.Node = null;
            ReleaseAdmittedCore(item);
            disposeRegistration = item.TryMarkCompletionStarted(out registration);
        }

        DisposeRegistrationIfNeeded(registration, disposeRegistration);

        item.CompleteCanceled();
        return true;
    }

    /// <summary>Throws when admission has closed.</summary>
    /// <exception cref="ObjectDisposedException">The worker has been disposed.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ThrowIfDisposed() => ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);

    /// <summary>Stores a queued command removed before dispatch.</summary>
    private readonly struct RemovedWorkItem
    {
        /// <summary>Initializes a new instance of the <see cref="RemovedWorkItem"/> struct.</summary>
        /// <param name="item">The removed command.</param>
        /// <param name="registration">The cancellation registration to dispose.</param>
        /// <param name="disposeRegistration">Whether the registration should be disposed.</param>
        internal RemovedWorkItem(
            WorkItem item,
            CancellationTokenRegistration registration,
            bool disposeRegistration)
        {
            Item = item;
            Registration = registration;
            DisposeRegistration = disposeRegistration;
        }

        /// <summary>Gets the removed command.</summary>
        internal WorkItem Item { get; }

        /// <summary>Gets the cancellation registration to dispose.</summary>
        private CancellationTokenRegistration Registration { get; }

        /// <summary>Gets a value indicating whether the registration should be disposed.</summary>
        private bool DisposeRegistration { get; }

        /// <summary>Disposes the cancellation registration when one was detached.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void DisposeRegistrationIfNeeded() =>
            SqliteSynchronousCommandWorker.DisposeRegistrationIfNeeded(Registration, DisposeRegistration);
    }

    /// <summary>Stores a possibly-null command result without nullable suppression.</summary>
    /// <typeparam name="T">The result type.</typeparam>
    private readonly struct OptionalResult<T>
    {
        /// <summary>Initializes a new instance of the <see cref="OptionalResult{T}"/> struct.</summary>
        /// <param name="value">The command result value.</param>
        internal OptionalResult(T value) => Value = value;

        /// <summary>Gets the command result value.</summary>
        internal T Value { get; }
    }

    /// <summary>Represents one admitted worker command.</summary>
    private sealed class WorkItem
    {
        /// <summary>Invokes the command.</summary>
        private readonly Action<CancellationToken> _invoke;

        /// <summary>Completes the command task.</summary>
        private readonly Action<CancellationToken> _complete;

        /// <summary>Completes the command task as canceled before dispatch.</summary>
        private readonly Action<CancellationToken> _completeCanceled;

        /// <summary>Completes the command task as disposed before dispatch.</summary>
        private readonly Action<ObjectDisposedException> _completeDisposed;

        /// <summary>Initializes a new instance of the <see cref="WorkItem"/> class.</summary>
        /// <param name="owner">The owning worker.</param>
        /// <param name="invoke">Invokes the command.</param>
        /// <param name="complete">Completes the command task.</param>
        /// <param name="completeCanceled">Completes the command task as canceled.</param>
        /// <param name="completeDisposed">Completes the command task as disposed.</param>
        /// <param name="retainedBytes">The caller-declared retained bytes.</param>
        /// <param name="cancellationToken">The caller cancellation token.</param>
        internal WorkItem(
            SqliteSynchronousCommandWorker owner,
            Action<CancellationToken> invoke,
            Action<CancellationToken> complete,
            Action<CancellationToken> completeCanceled,
            Action<ObjectDisposedException> completeDisposed,
            long retainedBytes,
            CancellationToken cancellationToken)
        {
            Owner = owner;
            RetainedBytes = retainedBytes;
            CancellationToken = cancellationToken;
            _invoke = invoke;
            _complete = complete;
            _completeCanceled = completeCanceled;
            _completeDisposed = completeDisposed;
        }

        /// <summary>Gets the caller-declared retained bytes.</summary>
        internal long RetainedBytes { get; }

        /// <summary>Gets or sets the queued node.</summary>
        internal LinkedListNode<WorkItem>? Node { get; set; }

        /// <summary>Gets the owning worker.</summary>
        private SqliteSynchronousCommandWorker Owner { get; }

        /// <summary>Gets the caller cancellation token.</summary>
        private CancellationToken CancellationToken { get; }

        /// <summary>Gets or sets the cancellation registration.</summary>
        private CancellationTokenRegistration CancellationRegistration { get; set; }

        /// <summary>Gets or sets a value indicating whether cancellation registration is active.</summary>
        private bool HasCancellationRegistration { get; set; }

        /// <summary>Gets or sets a value indicating whether the command has started final completion.</summary>
        private bool CompletionStarted { get; set; }

        /// <summary>Registers cancellation for queued-only removal.</summary>
        internal void RegisterCancellation()
        {
            if (!CancellationToken.CanBeCanceled)
            {
                return;
            }

#if NET8_0_OR_GREATER
            var registration = CancellationToken.UnsafeRegister(CancelQueued, this);
#else
            var registration = CancellationToken.Register(CancelQueued, this);
#endif
            var disposeRegistration = false;
            lock (Owner._gate)
            {
                CancellationRegistration = registration;
                disposeRegistration = CompletionStarted;
                HasCancellationRegistration = !disposeRegistration;
            }

            DisposeRegistrationIfNeeded(registration, disposeRegistration);
        }

        /// <summary>Invokes the synchronous command.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Invoke() => _invoke(CancellationToken);

        /// <summary>Completes the caller task with the stored command result.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Complete() => _complete(CancellationToken);

        /// <summary>Completes the caller task as canceled before dispatch.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void CompleteCanceled() => _completeCanceled(CancellationToken);

        /// <summary>Completes the caller task as rejected during disposal.</summary>
        /// <param name="exception">The disposal exception.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void CompleteDisposed(ObjectDisposedException exception) => _completeDisposed(exception);

        /// <summary>Marks the command as completing and detaches queued cancellation registration.</summary>
        /// <param name="registration">The cancellation registration to dispose after releasing the worker gate.</param>
        /// <returns><see langword="true"/> when a registration should be disposed; otherwise, <see langword="false"/>.</returns>
        internal bool TryMarkCompletionStarted(out CancellationTokenRegistration registration)
        {
            CompletionStarted = true;
            if (!HasCancellationRegistration)
            {
                registration = default;
                return false;
            }

            HasCancellationRegistration = false;
            registration = CancellationRegistration;
            return true;
        }

        /// <summary>Cancels a queued command from a cancellation registration.</summary>
        /// <param name="state">The registered command.</param>
        private static void CancelQueued(object? state)
        {
            ArgumentExceptionHelper.ThrowIfNull(state);
            _ = ((WorkItem)state).Owner.TryCancelQueued((WorkItem)state);
        }
    }

    /// <summary>Completes one typed command.</summary>
    /// <typeparam name="TResult">The command result type.</typeparam>
    private sealed class CommandCompletion<TResult>
    {
        /// <summary>The synchronous command delegate.</summary>
        private readonly Func<CancellationToken, TResult> _command;

        /// <summary>The command result completion.</summary>
        private readonly TaskCompletionSource<TResult> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The command result.</summary>
        private OptionalResult<TResult> _result;

        /// <summary>The command exception.</summary>
        private Exception? _exception;

        /// <summary>Initializes a new instance of the <see cref="CommandCompletion{TResult}"/> class.</summary>
        /// <param name="command">The synchronous command delegate.</param>
        internal CommandCompletion(Func<CancellationToken, TResult> command) => _command = command;

        /// <summary>Gets the command result task.</summary>
        internal Task<TResult> Task => _completion.Task;

        /// <summary>Invokes the command.</summary>
        /// <param name="cancellationToken">The caller cancellation token.</param>
        internal void Invoke(CancellationToken cancellationToken)
        {
            try
            {
                _result = new(_command(cancellationToken));
            }
            catch (Exception exception)
            {
                _exception = exception;
            }
        }

        /// <summary>Completes the command task.</summary>
        /// <param name="cancellationToken">The caller cancellation token.</param>
        internal void Complete(CancellationToken cancellationToken)
        {
            if (_exception is OperationCanceledException && cancellationToken.IsCancellationRequested)
            {
                _ = _completion.TrySetCanceled(cancellationToken);
                return;
            }

            if (_exception is not null)
            {
                _ = _completion.TrySetException(_exception);
                return;
            }

            _ = _completion.TrySetResult(_result.Value);
        }

        /// <summary>Completes the command task as canceled before dispatch.</summary>
        /// <param name="cancellationToken">The caller cancellation token.</param>
        internal void CompleteCanceled(CancellationToken cancellationToken) => _ = _completion.TrySetCanceled(cancellationToken);

        /// <summary>Completes the command task as disposed before dispatch.</summary>
        /// <param name="exception">The disposal exception.</param>
        internal void CompleteDisposed(ObjectDisposedException exception) => _ = _completion.TrySetException(exception);
    }
}
