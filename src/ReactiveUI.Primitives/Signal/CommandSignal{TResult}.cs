// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

#pragma warning disable SA1501

namespace ReactiveUI.Primitives.Signals;

/// <summary>
/// Minimal reactive command that gates execution and publishes result, fault, and running state streams.
/// </summary>
/// <typeparam name="TResult">The command result type.</typeparam>
public sealed class CommandSignal<TResult> : IDisposable
{
    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly Func<CancellationToken, Task<TResult>> _execute;

    /// <summary>
    /// Executes the new operation.
    /// </summary>
    /// <returns>The result.</returns>
    private readonly object _gate = new();

    /// <summary>
    /// Executes the new operation.
    /// </summary>
    /// <returns>The result.</returns>
    private readonly Signal<TResult> _results = new();

    /// <summary>
    /// Executes the new operation.
    /// </summary>
    /// <returns>The result.</returns>
    private readonly Signal<Exception> _faults = new();

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private readonly IDisposable? _canRunSubscription;

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private bool _canRun;

    /// <summary>
    /// Stores state for the signal implementation.
    /// </summary>
    private bool _disposed;

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
        _execute = execute ?? throw new ArgumentNullException(nameof(execute));
        _canRun = canRun == null;
        IsRunning = new StateSignal<bool>(false);

        if (canRun == null)
        {
            return;
        }

        _canRunSubscription = canRun.Subscribe(value => _canRun = value, _faults.OnNext);
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
        : this(_ => Task.FromResult((execute ?? throw new ArgumentNullException(nameof(execute)))()), canRun)
    {
    }

    /// <summary>
    /// Gets the successful command results.
    /// </summary>
    public IObservable<TResult> Results => _results;

    /// <summary>
    /// Gets command execution failures as values before the returned task rethrows them.
    /// </summary>
    public IObservable<Exception> Faults => _faults;

    /// <summary>
    /// Gets a state signal that is true while an execution is in flight.
    /// </summary>
    public StateSignal<bool> IsRunning { get; }

    /// <summary>
    /// Gets a value indicating whether the command can currently run.
    /// </summary>
    public bool CanRun => Volatile.Read(ref _canRun);

    /// <summary>
    /// Executes the command if allowed and publishes the result or fault.
    /// </summary>
    /// <returns>The command result.</returns>
    public Task<TResult> ExecuteAsync() => ExecuteAsync(CancellationToken.None);

    /// <summary>
    /// Executes the command if allowed and publishes the result or fault.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token for the operation.</param>
    /// <returns>The command result.</returns>
    public async Task<TResult> ExecuteAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        lock (_gate)
        {
            if (!CanRun || IsRunning.Value)
            {
                throw new InvalidOperationException("Command cannot run.");
            }

            IsRunning.Value = true;
        }

        try
        {
            var result = await _execute(cancellationToken).ConfigureAwait(false);
            _results.OnNext(result);
            return result;
        }
        catch (Exception error)
        {
            _faults.OnNext(error);
            throw;
        }
        finally
        {
            IsRunning.Value = false;
        }
    }

    /// <summary>
    /// Executes the Dispose operation.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _canRunSubscription?.Dispose();
        _results.Dispose();
        _faults.Dispose();
        IsRunning.Dispose();
    }

    /// <summary>
    /// Executes the ThrowIfDisposed operation.
    /// </summary>
    private void ThrowIfDisposed()
    {
        if (!_disposed)
        {
            return;
        }

        throw new ObjectDisposedException(nameof(CommandSignal<TResult>));
    }
}
