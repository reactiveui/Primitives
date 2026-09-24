// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.OccasionallyConnected.Collaboration.Client;

/// <summary>Owns one Ctrl+C subscription and its command cancellation source.</summary>
internal sealed class ConsoleCancellationScope : IAsyncDisposable
{
    /// <summary>Protects cancellation and disposal state.</summary>
    private readonly object _gate = new();

    /// <summary>The cancellation source passed to the command.</summary>
    private readonly CancellationTokenSource _source = new();

    /// <summary>The event removal callback.</summary>
    private readonly Action<ConsoleCancelEventHandler> _unsubscribe;

    /// <summary>The asynchronous cancellation operation.</summary>
    private readonly Func<CancellationTokenSource, Task> _cancelAsync;

    /// <summary>The subscribed event handler.</summary>
    private readonly ConsoleCancelEventHandler _handler;

    /// <summary>The first cancellation completion.</summary>
    private Task? _cancellationTask;

    /// <summary>Whether disposal has claimed the scope.</summary>
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="ConsoleCancellationScope"/> class.</summary>
    internal ConsoleCancellationScope()
        : this(
            static handler => Console.CancelKeyPress += handler,
            static handler => Console.CancelKeyPress -= handler)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ConsoleCancellationScope"/> class.</summary>
    /// <param name="subscribe">The event subscription callback.</param>
    /// <param name="unsubscribe">The event removal callback.</param>
    internal ConsoleCancellationScope(
        Action<ConsoleCancelEventHandler> subscribe,
        Action<ConsoleCancelEventHandler> unsubscribe)
        : this(subscribe, unsubscribe, static source => source.CancelAsync())
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ConsoleCancellationScope"/> class.</summary>
    /// <param name="subscribe">The event subscription callback.</param>
    /// <param name="unsubscribe">The event removal callback.</param>
    /// <param name="cancelAsync">The asynchronous cancellation operation.</param>
    internal ConsoleCancellationScope(
        Action<ConsoleCancelEventHandler> subscribe,
        Action<ConsoleCancelEventHandler> unsubscribe,
        Func<CancellationTokenSource, Task> cancelAsync)
    {
        ArgumentNullException.ThrowIfNull(subscribe);
        ArgumentNullException.ThrowIfNull(unsubscribe);
        ArgumentNullException.ThrowIfNull(cancelAsync);
        _unsubscribe = unsubscribe;
        _cancelAsync = cancelAsync;
        _handler = OnCancelKeyPress;
        try
        {
            subscribe(_handler);
        }
        catch
        {
            _source.Dispose();
            throw;
        }
    }

    /// <summary>Gets the token passed to the client command.</summary>
    internal CancellationToken Token => _source.Token;

    /// <summary>Gets a callback or unsubscribe failure observed during disposal.</summary>
    internal Exception? Failure { get; private set; }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        Task? cancellationTask;
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            cancellationTask = _cancellationTask;
        }

        Exception? unsubscribeFailure = null;
        try
        {
            _unsubscribe(_handler);
        }
        catch (Exception exception)
        {
            unsubscribeFailure = exception;
        }

        Exception? cancellationFailure = null;
        try
        {
            if (cancellationTask is not null)
            {
                await cancellationTask.ConfigureAwait(false);
            }
        }
        catch (Exception exception)
        {
            cancellationFailure = exception;
        }
        finally
        {
            _source.Dispose();
        }

        Failure = unsubscribeFailure is not null && cancellationFailure is not null
            ? new AggregateException("Console cancellation cleanup failed.", unsubscribeFailure, cancellationFailure)
            : unsubscribeFailure ?? cancellationFailure;
    }

    /// <summary>Requests cancellation once without running token callbacks under the state lock.</summary>
    /// <returns>The first cancellation task, or a completed task after disposal.</returns>
    internal Task RequestCancellation()
    {
        TaskCompletionSource? completion = null;
        Task task;
        lock (_gate)
        {
            if (_disposed)
            {
                return Task.CompletedTask;
            }

            if (_cancellationTask is null)
            {
                completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
                _cancellationTask = completion.Task;
            }

            task = _cancellationTask;
        }

        if (completion is not null)
        {
            _ = CompleteCancellationAsync(completion);
        }

        return task;
    }

    /// <summary>Requests command cancellation while preventing immediate console termination.</summary>
    /// <param name="sender">The console sender.</param>
    /// <param name="arguments">The console cancellation arguments.</param>
    private void OnCancelKeyPress(object? sender, ConsoleCancelEventArgs arguments)
    {
        arguments.Cancel = true;
        _ = RequestCancellation();
    }

    /// <summary>Completes the first cancellation request and retains any callback failure.</summary>
    /// <param name="completion">The first request completion.</param>
    /// <returns>The completion task.</returns>
    private async Task CompleteCancellationAsync(TaskCompletionSource completion)
    {
        try
        {
            await _cancelAsync(_source).ConfigureAwait(false);
            _ = completion.TrySetResult();
        }
        catch (Exception exception)
        {
            _ = completion.TrySetException(exception);
        }
    }
}
