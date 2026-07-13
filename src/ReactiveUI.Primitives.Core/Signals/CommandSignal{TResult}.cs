// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Signals;

/// <summary>Minimal reactive command that gates execution and publishes result, fault, and running state streams.</summary>
/// <typeparam name="TResult">The command result type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class CommandSignal<TResult> : IObservable<TResult>, IDisposable
{
    /// <summary>Stores asynchronous command execution.</summary>
    private readonly Func<CancellationToken, Task<TResult>>? _executeAsync;

    /// <summary>Stores synchronous command execution.</summary>
    private readonly Func<TResult>? _executeSync;

    /// <summary>Serializes running-flag writes with running-state stream notifications so the two never diverge.</summary>
    private readonly Lock _runningGate = new();

    /// <summary>Stores null, a single result observer, or an observer array.</summary>
    private object? _resultObservers;

    /// <summary>Lazily created fault stream.</summary>
    private Signal<Exception>? _faults;

    /// <summary>Lazily created running state stream.</summary>
    private StateSignal<bool>? _isRunningState;

    /// <summary>Stores state for the signal implementation.</summary>
    private IDisposable? _canRunSubscription;

    /// <summary>Stores state for the signal implementation.</summary>
    private bool _canRun;

    /// <summary>Stores the current running flag without forcing the public state stream to allocate.</summary>
    private bool _isRunning;

    /// <summary>Non-zero while an execution is active.</summary>
    private int _running;

    /// <summary>Stores disposal state.</summary>
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="CommandSignal{TResult}"/> class.</summary>
    /// <param name="execute">The async operation to execute.</param>
    public CommandSignal(Func<CancellationToken, Task<TResult>> execute)
        : this(execute, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CommandSignal{TResult}"/> class.</summary>
    /// <param name="execute">The async operation to execute.</param>
    /// <param name="canRun">Gating signal. When null, execution is always allowed.</param>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Style",
        "IDE0001:Simplify Names",
        Justification = "The argument validation uses ArgumentExceptionHelper")]
    public CommandSignal(Func<CancellationToken, Task<TResult>> execute, IObservable<bool>? canRun)
    {
        _executeAsync = execute ?? throw new ArgumentNullException(nameof(execute));
        InitializeCanRun(canRun);
    }

    /// <summary>Initializes a new instance of the <see cref="CommandSignal{TResult}"/> class.</summary>
    /// <param name="execute">The synchronous operation to execute.</param>
    public CommandSignal(Func<TResult> execute)
        : this(execute, null)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="CommandSignal{TResult}"/> class.</summary>
    /// <param name="execute">The synchronous operation to execute.</param>
    /// <param name="canRun">Gating signal. When null, execution is always allowed.</param>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Style",
        "IDE0001:Simplify Names",
        Justification = "The argument validation uses ArgumentExceptionHelper")]
    public CommandSignal(Func<TResult> execute, IObservable<bool>? canRun)
    {
        _executeSync = execute ?? throw new ArgumentNullException(nameof(execute));
        InitializeCanRun(canRun);
    }

    /// <summary>Gets the successful command results.</summary>
    public IObservable<TResult> Results => this;

    /// <summary>Gets command execution failures as values before the returned task rethrows them.</summary>
    public IObservable<Exception> Faults => FaultsSignal;

    /// <summary>Gets a state signal that is true while an execution is in flight.</summary>
    public StateSignal<bool> IsRunning => IsRunningSignal;

    /// <summary>Gets a value indicating whether the command can currently run.</summary>
    public bool CanRun => Volatile.Read(ref _canRun);

    /// <summary>Gets the lazily allocated fault stream.</summary>
    private Signal<Exception> FaultsSignal
    {
        get
        {
            var signal = Volatile.Read(ref _faults);
            if (signal is not null)
            {
                return signal;
            }

            signal = new();
            var current = Interlocked.CompareExchange(ref _faults, signal, null);
            if (current is null)
            {
                return signal;
            }

            signal.Dispose();
            return current;
        }
    }

    /// <summary>Gets the lazily allocated running state stream.</summary>
    private StateSignal<bool> IsRunningSignal
    {
        get
        {
            var signal = Volatile.Read(ref _isRunningState);
            if (signal is not null)
            {
                return signal;
            }

            signal = new(Volatile.Read(ref _isRunning));
            var current = Interlocked.CompareExchange(ref _isRunningState, signal, null);
            if (current is null)
            {
                // The snapshot above may already be stale: a SetRunning call can run between it and
                // the install. Reconcile under the running gate so the just-installed stream cannot
                // latch a stale value, and so a concurrent SetRunning cannot lose its update to a
                // late seed write here.
                ReconcileRunningState();
                return signal;
            }

            signal.Dispose();
            return current;
        }
    }

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>Executes the command if allowed and publishes the result or fault.</summary>
    /// <returns>The command result.</returns>
    public CommandExecution<TResult> ExecuteAsync() => ExecuteAsync(CancellationToken.None);

    /// <summary>Executes the command if allowed and publishes the result or fault.</summary>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>The command result.</returns>
    public CommandExecution<TResult> ExecuteAsync(CancellationToken cancellationToken)
    {
        try
        {
            ThrowIfDisposed();

            return _executeSync is not null
                ? new(ExecuteSync(cancellationToken))
                : new CommandExecution<TResult>(ExecuteAsyncCore(cancellationToken));
        }
        catch (Exception error)
        {
            return new(error);
        }
    }

    /// <summary>Executes the Dispose operation.</summary>
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

    /// <summary>Subscribes to successful command results.</summary>
    /// <param name="observer">Observer to notify.</param>
    /// <returns>A subscription.</returns>
    public IDisposable Subscribe(IObserver<TResult> observer)
    {
        ArgumentExceptionHelper.ThrowIfNull(observer);

        ThrowIfDisposed();
        AddResult(observer);
        return new ResultSubscription(this, observer);
    }

    /// <summary>Gets the observer snapshot that should replace the current snapshot.</summary>
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

    /// <summary>Initializes optional execution gating.</summary>
    /// <param name="canRun">Gating signal.</param>
    private void InitializeCanRun(IObservable<bool>? canRun)
    {
        _canRun = canRun is null;

        if (canRun is null)
        {
            return;
        }

        _canRunSubscription = canRun.Subscribe(value => _canRun = value, FaultsSignal.OnNext);
    }

    /// <summary>Executes the synchronous command path without allocating a task.</summary>
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

    /// <summary>Executes the asynchronous command path.</summary>
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

    /// <summary>Enters the running state after validating gate state.</summary>
    private void BeginExecution()
    {
        if (!CanRun || Interlocked.CompareExchange(ref _running, 1, 0) != 0)
        {
            throw new InvalidOperationException("Command cannot run.");
        }

        SetRunning(true);
    }

    /// <summary>Updates running state and notifies the optional public state stream.</summary>
    /// <param name="value">The running state.</param>
    private void SetRunning(bool value)
    {
        // Set the flag and notify the stream through the same gated path the getter uses. Holding
        // the gate across the flag write and the notification keeps the flag and the stream value
        // observed together, so the lazy install and a concurrent transition cannot lose each
        // other's update.
        lock (_runningGate)
        {
            _isRunning = value;
            PublishRunningState();
        }

        if (value)
        {
            return;
        }

        Volatile.Write(ref _running, 0);
    }

    /// <summary>Seeds a just-installed stream from the authoritative flag without losing a concurrent update.</summary>
    private void ReconcileRunningState()
    {
        lock (_runningGate)
        {
            PublishRunningState();
        }
    }

    /// <summary>Pushes the authoritative running flag onto the stream when one is installed. Caller holds the gate.</summary>
    private void PublishRunningState() => Volatile.Read(ref _isRunningState)?.OnNext(_isRunning);

    /// <summary>Publishes a successful result when the results surface has been requested.</summary>
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

    /// <summary>Publishes a fault when the faults surface has been requested.</summary>
    /// <param name="error">The command error.</param>
    private void PublishFault(Exception error) => Volatile.Read(ref _faults)?.OnNext(error);

    /// <summary>Executes the ThrowIfDisposed operation.</summary>
    private void ThrowIfDisposed()
    {
        if (Volatile.Read(ref _disposed) == 0)
        {
            return;
        }

        throw new ObjectDisposedException(nameof(CommandSignal<>));
    }

    /// <summary>Adds a result subscriber.</summary>
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
            else if (Interlocked.CompareExchange(ref _resultObservers, observer, null) is null)
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

    /// <summary>Clears result subscribers.</summary>
    private void ClearResults() => Volatile.Write(ref _resultObservers, null);

    /// <summary>Removes a result subscriber.</summary>
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

    /// <summary>Result stream subscription.</summary>
    /// <param name="parent">Parent command.</param>
    /// <param name="observer">Observer to remove.</param>
    private sealed class ResultSubscription(CommandSignal<TResult> parent, IObserver<TResult> observer) : IDisposable
    {
        /// <summary>Parent command.</summary>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA2213:Disposable fields should be disposed", Justification = "Back-reference to the parent command; unsubscribed, not owned.")]
        private CommandSignal<TResult>? _parent = parent;

        /// <summary>Observer to remove on disposal.</summary>
        private IObserver<TResult>? _observer = observer;

        /// <inheritdoc/>
        public void Dispose()
        {
            var parentState = Interlocked.Exchange(ref _parent, null);
            if (parentState is null)
            {
                return;
            }

            parentState.RemoveResult(_observer!);
            _observer = null;
        }
    }
}
