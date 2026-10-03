// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;

namespace ReactiveUI.Primitives.OccasionallyConnected.Mobile;

/// <summary>Serializes mobile lifecycle intent through the existing context lifecycle and synchronization engine.</summary>
/// <remarks>
/// Requests coalesce to the latest intent. Cancellation cancels only a caller's wait after acceptance.
/// Suspension cancels startup and connectivity-trigger I/O, then stops with an uncancelled durable drain.
/// The session does not own the context or event sources. OS termination may prevent a lifecycle event or a drain.
/// </remarks>
[DebuggerDisplay("Disposed = {_disposed}, Resume = {_resume}")]
public sealed class MobileSyncSession : IAsyncDisposable
{
    /// <summary>Protects lifecycle intent and the shared worker.</summary>
    private readonly Lock _gate = new();

    /// <summary>The borrowed context.</summary>
    private readonly IOccasionallyConnectedContext _context;

    /// <summary>The borrowed lifecycle source.</summary>
    private readonly IMobileLifecycle _lifecycle;

    /// <summary>The borrowed network hint source.</summary>
    private readonly IMobileConnectivityHint _connectivity;

    /// <summary>The shared transition worker.</summary>
    private Task _transition = Task.CompletedTask;

    /// <summary>The shared disposal task.</summary>
    private Task? _disposal;

    /// <summary>The cancellation source for current foreground work.</summary>
    private CancellationTokenSource? _foreground;

    /// <summary>The latest accepted intent version.</summary>
    private long _version;

    /// <summary>Whether foreground work is currently desired.</summary>
    private bool _resume;

    /// <summary>Whether an accepted stop must drain before foreground work can resume.</summary>
    private bool _stopPending;

    /// <summary>Whether event admission is closed.</summary>
    private bool _disposed;

    /// <summary>Whether a worker still owns the current intent.</summary>
    private int _running;

    /// <summary>The latest observed event or cancellation callback failure.</summary>
    private Exception? _failure;

    /// <summary>Initializes a new instance of the <see cref="MobileSyncSession"/> class.</summary>
    /// <param name="context">The context to start and stop.</param>
    /// <param name="lifecycle">The mobile lifecycle source.</param>
    /// <param name="connectivity">The network hint source.</param>
    /// <exception cref="ArgumentNullException">A required argument is null.</exception>
    /// <remarks>Call <see cref="RefreshAsync"/> after attachment to reconcile the source's initial state.</remarks>
    public MobileSyncSession(
        IOccasionallyConnectedContext context,
        IMobileLifecycle lifecycle,
        IMobileConnectivityHint connectivity)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(lifecycle);
        ArgumentNullException.ThrowIfNull(connectivity);
        _context = context;
        _lifecycle = lifecycle;
        _connectivity = connectivity;
        _resume = !lifecycle.IsSuspended;
        lifecycle.Suspending += OnSuspending;
        lifecycle.Resuming += OnResuming;
        connectivity.Changed += OnConnectivityChanged;
    }

    /// <summary>Gets the most recent accepted transition, including failures from event-driven transitions.</summary>
    public Task Transition
    {
        get
        {
            lock (_gate)
            {
                return _transition;
            }
        }
    }

    /// <summary>Gets the latest observed transition or cancellation callback failure, without logging it.</summary>
    public Exception? LastFailure
    {
        get
        {
            lock (_gate)
            {
                return _failure;
            }
        }
    }

    /// <summary>Reconciles the current host lifecycle hint.</summary>
    /// <param name="cancellationToken">Cancels admission or the caller's wait, not an accepted drain.</param>
    /// <returns>The shared transition outcome.</returns>
    /// <exception cref="ObjectDisposedException">The session has closed admission.</exception>
    /// <exception cref="OperationCanceledException">The caller cancelled admission or its wait.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask RefreshAsync(CancellationToken cancellationToken) =>
        RequestAsync(!_lifecycle.IsSuspended, cancellationToken);

    /// <summary>Requests foreground synchronization.</summary>
    /// <param name="cancellationToken">Cancels admission or the caller's wait.</param>
    /// <returns>The shared transition outcome.</returns>
    /// <exception cref="ObjectDisposedException">The session has closed admission.</exception>
    /// <exception cref="OperationCanceledException">The caller cancelled admission or its wait.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask ResumeAsync(CancellationToken cancellationToken) => RequestAsync(true, cancellationToken);

    /// <summary>Cancels foreground I/O and requests a durable context stop.</summary>
    /// <param name="cancellationToken">Cancels admission or the caller's wait, not the stop operation.</param>
    /// <returns>The shared transition outcome.</returns>
    /// <exception cref="ObjectDisposedException">The session has closed admission.</exception>
    /// <exception cref="OperationCanceledException">The caller cancelled admission or its wait.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask SuspendAsync(CancellationToken cancellationToken) => RequestAsync(false, cancellationToken);

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        Task disposal;
        lock (_gate)
        {
            if (_disposal is null)
            {
                _disposed = true;
                _lifecycle.Suspending -= OnSuspending;
                _lifecycle.Resuming -= OnResuming;
                _connectivity.Changed -= OnConnectivityChanged;
                _resume = false;
                _stopPending = true;
                _version++;
                CancelForegroundLocked();
                StartWorkerLocked();
                _disposal = _transition;
            }

            disposal = _disposal;
        }

        return new(disposal);
    }

    /// <summary>Accepts intent and waits independently of the worker lifetime.</summary>
    /// <param name="resume">Whether foreground work is desired.</param>
    /// <param name="cancellationToken">The caller's cancellation token.</param>
    /// <returns>The transition wait.</returns>
    private ValueTask RequestAsync(bool resume, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        Task transition;
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            AcceptLocked(resume);
            transition = _transition;
        }

        return new(transition.WaitAsync(cancellationToken));
    }

    /// <summary>Updates intent while the state gate is held.</summary>
    /// <param name="resume">The desired foreground state.</param>
    private void AcceptLocked(bool resume)
    {
        _resume = resume;
        _version++;
        if (!resume)
        {
            _stopPending = true;
            CancelForegroundLocked();
        }

        StartWorkerLocked();
    }

    /// <summary>Cancels foreground work without synchronously invoking foreign cancellation callbacks under the gate.</summary>
    private void CancelForegroundLocked()
    {
        if (_foreground is not null)
        {
            _ = ObserveAsync(_foreground.CancelAsync());
        }
    }

    /// <summary>Starts a single bounded, coalescing transition worker.</summary>
    private void StartWorkerLocked()
    {
        if (Interlocked.Exchange(ref _running, 1) != 0)
        {
            return;
        }

        _transition = Task.Run(ReconcileAsync, CancellationToken.None);
        _ = ObserveAsync(_transition);
    }

    /// <summary>Runs accepted lifecycle intent until the latest version is settled.</summary>
    /// <returns>The transition outcome.</returns>
    private async Task ReconcileAsync()
    {
        while (true)
        {
            long version;
            bool resume;
            Exception? failure = null;
            using var foreground = new CancellationTokenSource();
            lock (_gate)
            {
                version = _version;
                resume = _resume && !_stopPending;
                if (!resume)
                {
                    _stopPending = false;
                }

                _foreground = resume ? foreground : null;
            }

            try
            {
                await ExecuteIntentAsync(resume, foreground.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (foreground.IsCancellationRequested)
            {
                // A newer suspend request owns the subsequent uncancelled stop.
            }
            catch (Exception exception)
            {
                failure = exception;
                lock (_gate)
                {
                    _failure = exception;
                }
            }
            finally
            {
                lock (_gate)
                {
                    _foreground = null;
                }
            }

            if (!resume && failure is not null)
            {
                FailDrain(failure);
            }

            if (CompleteVersion(version, resume, failure))
            {
                return;
            }
        }
    }

    /// <summary>Closes a failed worker without allowing a pending resume to bypass its failed drain.</summary>
    /// <param name="failure">The durable stop failure.</param>
    private void FailDrain(Exception failure)
    {
        lock (_gate)
        {
            _stopPending = true;
            _ = Interlocked.Exchange(ref _running, 0);
        }

        ExceptionDispatchInfo.Capture(failure).Throw();
    }

    /// <summary>Settles a version only when no newer intent or durable drain is pending.</summary>
    /// <param name="version">The version just executed.</param>
    /// <param name="resume">The foreground state just executed.</param>
    /// <param name="failure">An observed failure to propagate when settling.</param>
    /// <returns>Whether the worker has finished.</returns>
    private bool CompleteVersion(long version, bool resume, Exception? failure)
    {
        lock (_gate)
        {
            if (version != _version || resume != _resume || _stopPending)
            {
                return false;
            }

            _ = Interlocked.Exchange(ref _running, 0);
        }

        if (failure is not null)
        {
            ExceptionDispatchInfo.Capture(failure).Throw();
        }

        return true;
    }

    /// <summary>Executes foreground work or an uncancelled durable drain.</summary>
    /// <param name="resume">Whether foreground work is desired.</param>
    /// <param name="cancellationToken">The foreground cancellation token.</param>
    /// <returns>The context transition.</returns>
    private async ValueTask ExecuteIntentAsync(bool resume, CancellationToken cancellationToken)
    {
        if (!resume)
        {
            await _context.StopAsync(CancellationToken.None).ConfigureAwait(false);
            return;
        }

        await _context.StartAsync(cancellationToken).ConfigureAwait(false);
        if (_connectivity.NetworkAvailable)
        {
            await _context.SyncEngine.TriggerSyncAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    /// <summary>Observes event-driven failures while leaving them available on the transition task.</summary>
    /// <param name="task">The task to observe.</param>
    /// <returns>The observation completion.</returns>
    private async Task ObserveAsync(Task task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            lock (_gate)
            {
                _failure = exception;
            }
        }
    }

    /// <summary>Accepts a lifecycle event without using async void.</summary>
    /// <param name="resume">The foreground intent.</param>
    private void AcceptEvent(bool resume)
    {
        lock (_gate)
        {
            if (!_disposed)
            {
                AcceptLocked(resume);
            }
        }
    }

    /// <summary>Receives a suspend event.</summary>
    /// <param name="sender">The event source.</param>
    /// <param name="args">The event arguments.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnSuspending(object? sender, EventArgs args) => AcceptEvent(false);

    /// <summary>Receives a resume event.</summary>
    /// <param name="sender">The event source.</param>
    /// <param name="args">The event arguments.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnResuming(object? sender, EventArgs args) => AcceptEvent(true);

    /// <summary>Receives a connectivity hint without changing lifecycle intent.</summary>
    /// <param name="sender">The event source.</param>
    /// <param name="args">The event arguments.</param>
    private void OnConnectivityChanged(object? sender, EventArgs args)
    {
        lock (_gate)
        {
            if (!_disposed && _resume && _connectivity.NetworkAvailable)
            {
                AcceptLocked(true);
            }
        }
    }
}
