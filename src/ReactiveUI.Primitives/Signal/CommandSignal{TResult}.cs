// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Minimal reactive command that gates execution and publishes result, fault, and running state streams.
/// </summary>
/// <typeparam name="TResult">The command result type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class CommandSignal<TResult> : IObservable<TResult>, IDisposable
{
    /// <summary>
    /// Stores asynchronous command execution.
    /// </summary>
    private readonly Func<CancellationToken, Task<TResult>>? _executeAsync;

    /// <summary>
    /// Stores synchronous command execution.
    /// </summary>
    private readonly Func<TResult>? _executeSync;

    /// <summary>
    /// Stores null, a single result observer, or an observer array.
    /// </summary>
    private object? _resultObservers;

    /// <summary>
    /// Lazily created fault stream.
    /// </summary>
    private Signal<Exception>? _faults;

    /// <summary>
    /// Lazily created running state stream.
    /// </summary>
    private StateSignal<bool>? _isRunningState;

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private IDisposable? _canRunSubscription;

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private bool _canRun;

    /// <summary>
    /// Stores the current running flag without forcing the public state stream to allocate.
    /// </summary>
    private bool _isRunning;

    /// <summary>
    /// Non-zero while an execution is active.
    /// </summary>
    private int _running;

    /// <summary>
    /// Stores disposal state.
    /// </summary>
    private int _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="CommandSignal{TResult}"/> class.
    /// </summary>
    /// <param name="execute">The async operation to execute.</param>
    public CommandSignal(Func<CancellationToken, Task<TResult>> execute)
        : this(execute, null)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="CommandSignal{TResult}"/> class.
    /// </summary>
    /// <param name="execute">The async operation to execute.</param>
    /// <param name="canRun">Gating signal. When null, execution is always allowed.</param>
    public CommandSignal(Func<CancellationToken, Task<TResult>> execute, IObservable<bool>? canRun)
    {
        _executeAsync = execute ?? throw new ArgumentNullException(nameof(execute));
        InitializeCanRun(canRun);
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="CommandSignal{TResult}"/> class.
    /// </summary>
    /// <param name="execute">The synchronous operation to execute.</param>
    public CommandSignal(Func<TResult> execute)
        : this(execute, null)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="CommandSignal{TResult}"/> class.
    /// </summary>
    /// <param name="execute">The synchronous operation to execute.</param>
    /// <param name="canRun">Gating signal. When null, execution is always allowed.</param>
    public CommandSignal(Func<TResult> execute, IObservable<bool>? canRun)
    {
        _executeSync = execute ?? throw new ArgumentNullException(nameof(execute));
        InitializeCanRun(canRun);
    }

    /// <summary>
    /// Gets the successful command results.
    /// </summary>
    public IObservable<TResult> Results => this;

    /// <summary>
    /// Gets command execution failures as values before the returned task rethrows them.
    /// </summary>
    public IObservable<Exception> Faults => FaultsSignal;

    /// <summary>
    /// Gets a state signal that is true while an execution is in flight.
    /// </summary>
    public StateSignal<bool> IsRunning => IsRunningSignal;

    /// <summary>
    /// Gets a value indicating whether the command can currently run.
    /// </summary>
    public bool CanRun => Volatile.Read(ref _canRun);

    /// <summary>
    /// Gets the lazily allocated fault stream.
    /// </summary>
    private Signal<Exception> FaultsSignal => EnsureSignal(ref _faults);

    /// <summary>
    /// Gets the lazily allocated running state stream.
    /// </summary>
    private StateSignal<bool> IsRunningSignal
    {
        get
        {
            var signal = Volatile.Read(ref _isRunningState);
            if (signal != null)
            {
                return signal;
            }

            signal = new StateSignal<bool>(Volatile.Read(ref _isRunning));
            var current = Interlocked.CompareExchange(ref _isRunningState, signal, null);
            if (current == null)
            {
                return signal;
            }

            signal.Dispose();
            return current;
        }
    }

    /// <summary>
    /// Gets the debugger display text.
    /// </summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>
    /// Executes the command if allowed and publishes the result or fault.
    /// </summary>
    /// <returns>The command result.</returns>
    public CommandExecution<TResult> ExecuteAsync() => ExecuteAsync(CancellationToken.None);

    /// <summary>
    /// Executes the command if allowed and publishes the result or fault.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>The command result.</returns>
    public CommandExecution<TResult> ExecuteAsync(CancellationToken cancellationToken)
    {
        try
        {
            ThrowIfDisposed();

            return _executeSync != null
                ? new CommandExecution<TResult>(ExecuteSync(cancellationToken))
                : new CommandExecution<TResult>(ExecuteAsyncCore(cancellationToken));
        }
        catch (Exception error)
        {
            return new CommandExecution<TResult>(error);
        }
    }

    /// <summary>
    /// Executes the Dispose operation.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _canRunSubscription?.Dispose();
        ClearResults();
        Interlocked.Exchange(ref _faults, null)?.Dispose();
        Interlocked.Exchange(ref _isRunningState, null)?.Dispose();
    }

    /// <summary>
    /// Subscribes to successful command results.
    /// </summary>
    /// <param name="observer">Observer to notify.</param>
    /// <returns>A subscription.</returns>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        if (observer == null)
        {
            throw new ArgumentNullException(nameof(observer));
        }

        ThrowIfDisposed();
        AddResult(observer);
        return new ResultSubscription(this, observer);
    }

    /// <summary>
    /// Creates a stream exactly once.
    /// </summary>
    /// <typeparam name="T">The signal value type.</typeparam>
    /// <param name="field">The field to initialize.</param>
    /// <returns>The initialized signal.</returns>
    private static Signal<T> EnsureSignal<T>(ref Signal<T>? field)
    {
        var signal = Volatile.Read(ref field);
        if (signal != null)
        {
            return signal;
        }

        signal = new Signal<T>();
        var current = Interlocked.CompareExchange(ref field, signal, null);
        if (current == null)
        {
            return signal;
        }

        signal.Dispose();
        return current;
    }

    /// <summary>
    /// Gets the observer snapshot that should replace the current snapshot.
    /// </summary>
    /// <param name="current">Current observer snapshot.</param>
    /// <param name="observer">Observer to remove.</param>
    /// <param name="next">Next observer snapshot.</param>
    /// <returns><see langword="true"/> when the current snapshot contains the observer.</returns>
    private static bool TryGetRemoveResultNext(object? current, IObserver<TResult> observer, out object? next)
    {
        if (ReferenceEquals(current, observer))
        {
            next = null;
            return true;
        }

        if (current is not IObserver<TResult>[] many)
        {
            next = null;
            return false;
        }

        var index = Array.IndexOf(many, observer);
        if (index < 0)
        {
            next = null;
            return false;
        }

        if (many.Length == 2)
        {
            next = many[index == 0 ? 1 : 0];
            return true;
        }

        var copy = new IObserver<TResult>[many.Length - 1];
        Array.Copy(many, 0, copy, 0, index);
        Array.Copy(many, index + 1, copy, index, many.Length - index - 1);
        next = copy;
        return true;
    }

    /// <summary>
    /// Initializes optional execution gating.
    /// </summary>
    /// <param name="canRun">Gating signal.</param>
    private void InitializeCanRun(IObservable<bool>? canRun)
    {
        _canRun = canRun == null;

        if (canRun == null)
        {
            return;
        }

        _canRunSubscription = canRun.Subscribe(value => _canRun = value, FaultsSignal.OnNext);
    }

    /// <summary>
    /// Executes the synchronous command path without allocating a task.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>The command result.</returns>
    private TResult ExecuteSync(CancellationToken cancellationToken)
    {
        BeginExecution();
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            var result = _executeSync!();
            PublishResult(result);
            return result;
        }
        catch (Exception error)
        {
            PublishFault(error);
            throw;
        }
        finally
        {
            SetRunning(false);
        }
    }

    /// <summary>
    /// Executes the asynchronous command path.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>The command result.</returns>
    private async Task<TResult> ExecuteAsyncCore(CancellationToken cancellationToken)
    {
        BeginExecution();
        try
        {
            var result = await _executeAsync!(cancellationToken).ConfigureAwait(false);
            PublishResult(result);
            return result;
        }
        catch (Exception error)
        {
            PublishFault(error);
            throw;
        }
        finally
        {
            SetRunning(false);
        }
    }

    /// <summary>
    /// Enters the running state after validating gate state.
    /// </summary>
    private void BeginExecution()
    {
        if (!CanRun || Interlocked.CompareExchange(ref _running, 1, 0) != 0)
        {
            throw new InvalidOperationException("Command cannot run.");
        }

        SetRunning(true);
    }

    /// <summary>
    /// Updates running state and notifies the optional public state stream.
    /// </summary>
    /// <param name="value">The running state.</param>
    private void SetRunning(bool value)
    {
        Volatile.Write(ref _isRunning, value);
        var state = Volatile.Read(ref _isRunningState);
        if (state != null)
        {
            state.Value = value;
        }

        if (value)
        {
            return;
        }

        Volatile.Write(ref _running, 0);
    }

    /// <summary>
    /// Publishes a successful result when the results surface has been requested.
    /// </summary>
    /// <param name="result">The command result.</param>
    private void PublishResult(TResult result)
    {
        var snapshot = Volatile.Read(ref _resultObservers);
        if (snapshot is IObserver<TResult> single)
        {
            single.OnNext(result);
            return;
        }

        if (snapshot is not IObserver<TResult>[] many)
        {
            return;
        }

        for (var i = 0; i < many.Length; i++)
        {
            many[i].OnNext(result);
        }
    }

    /// <summary>
    /// Publishes a fault when the faults surface has been requested.
    /// </summary>
    /// <param name="error">The command error.</param>
    private void PublishFault(Exception error)
    {
        Volatile.Read(ref _faults)?.OnNext(error);
    }

    /// <summary>
    /// Executes the ThrowIfDisposed operation.
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) == 0)
        {
            return;
        }

        throw new ObjectDisposedException(nameof(CommandSignal<TResult>));
    }

    /// <summary>
    /// Adds a result subscriber.
    /// </summary>
    /// <param name="observer">Observer to add.</param>
    private void AddResult(IObserver<TResult> observer)
    {
        while (true)
        {
            var current = Volatile.Read(ref _resultObservers);
            object next;
            if (current is IObserver<TResult>[] many)
            {
                var copy = new IObserver<TResult>[many.Length + 1];
                Array.Copy(many, copy, many.Length);
                copy[many.Length] = observer;
                next = copy;
            }
            else if (current is IObserver<TResult> single)
            {
                next = new[] { single, observer };
            }
            else if (Interlocked.CompareExchange(ref _resultObservers, observer, null) == null)
            {
                return;
            }
            else
            {
                continue;
            }

            if (ReferenceEquals(Interlocked.CompareExchange(ref _resultObservers, next, current), current))
            {
                return;
            }
        }
    }

    /// <summary>
    /// Clears result subscribers.
    /// </summary>
    private void ClearResults() => Volatile.Write(ref _resultObservers, null);

    /// <summary>
    /// Removes a result subscriber.
    /// </summary>
    /// <param name="observer">Observer to remove.</param>
    private void RemoveResult(IObserver<TResult> observer)
    {
        while (true)
        {
            var current = Volatile.Read(ref _resultObservers);
            if (!TryGetRemoveResultNext(current, observer, out var next))
            {
                return;
            }

            if (ReferenceEquals(Interlocked.CompareExchange(ref _resultObservers, next, current), current))
            {
                return;
            }
        }
    }

    /// <summary>
    /// Result stream subscription.
    /// </summary>
    private sealed class ResultSubscription : IDisposable
    {
        /// <summary>
        /// Parent command.
        /// </summary>
        private CommandSignal<TResult>? _parent;

        /// <summary>
        /// Observer to remove on disposal.
        /// </summary>
        private IObserver<TResult>? _observer;

        /// <summary>
        /// Initializes a new instance of the <see cref="ResultSubscription"/> class.
        /// </summary>
        /// <param name="parent">Parent command.</param>
        /// <param name="observer">Observer to remove.</param>
        public ResultSubscription(CommandSignal<TResult> parent, IObserver<TResult> observer)
        {
            _parent = parent;
            _observer = observer;
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            var parent = Interlocked.Exchange(ref _parent, null);
            if (parent == null)
            {
                return;
            }

            parent.RemoveResult(_observer!);
            _observer = null;
        }
    }
}
