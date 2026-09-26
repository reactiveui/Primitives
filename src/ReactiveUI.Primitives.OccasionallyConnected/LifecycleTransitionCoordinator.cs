// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Coordinates asynchronous start and cleanup transitions for an owned runtime component.</summary>
/// <remarks>
/// The coordinator stores one driver, one reserved or current callback kind, one shared start wait, one shared cleanup wait,
/// and durable lifecycle flags. Calls matching the active callback join that callback, even when an opposite intent is pending.
/// Calls opposing the active callback update a coalesced desired state rather than enqueueing per-caller work. Caller
/// cancellation only cancels that caller's wait and is never forwarded to the callbacks. Startup failures require cleanup
/// before retry, cleanup failures leave cleanup required, and disposal permanently prevents startup while still joining or
/// running cleanup.
/// </remarks>
#if NET11_0_OR_GREATER
internal sealed class LifecycleTransitionCoordinator : IAsyncDisposable, IUnion
#else
internal sealed class LifecycleTransitionCoordinator : IAsyncDisposable
#endif
{
#if NET11_0_OR_GREATER
    object IUnion.Value => this;
#endif
    /// <summary>Protects lifecycle state.</summary>
    private readonly Lock _gate = new();

    /// <summary>The application startup callback.</summary>
    private readonly Func<ValueTask> _start;

    /// <summary>The application cleanup callback.</summary>
    private readonly Func<ValueTask> _cleanup;

    /// <summary>The shared wait for the current or pending startup request.</summary>
    private TaskCompletionSource<bool>? _startWait;

    /// <summary>The shared wait for the current or pending cleanup request.</summary>
    private TaskCompletionSource<bool>? _cleanupWait;

    /// <summary>The kind of callback currently being executed by the driver.</summary>
    private TransitionKind _activeKind;

    /// <summary>Tracks whether a callback is currently executing.</summary>
    private int _driverRunning;

    /// <summary>Tracks whether startup completed and cleanup has not yet completed.</summary>
    private bool _started;

    /// <summary>Tracks resources that must be cleaned before the next successful startup.</summary>
    private bool _cleanupRequired;

    /// <summary>Tracks the coalesced requested final state.</summary>
    private bool _desiredStarted;

    /// <summary>Tracks whether disposal has permanently closed the startup path.</summary>
    private bool _disposed;

    /// <summary>Initializes a new instance of the <see cref="LifecycleTransitionCoordinator"/> class.</summary>
    /// <param name="start">The callback that starts the owned component.</param>
    /// <param name="cleanup">The callback that cleans the owned component after success or partial startup failure.</param>
    /// <exception cref="ArgumentNullException">A required callback is missing.</exception>
    internal LifecycleTransitionCoordinator(Func<ValueTask> start, Func<ValueTask> cleanup)
    {
        ArgumentExceptionHelper.ThrowIfNull(start);
        ArgumentExceptionHelper.ThrowIfNull(cleanup);
        _start = start;
        _cleanup = cleanup;
    }

    /// <summary>The callback direction currently owned by the driver.</summary>
    private enum TransitionKind
    {
        /// <summary>No callback is currently selected.</summary>
        None = 0,

        /// <summary>The startup callback is running.</summary>
        Start = 1,

        /// <summary>The cleanup callback is running.</summary>
        Cleanup = 2,
    }

    /// <inheritdoc/>
    public ValueTask DisposeAsync()
    {
        Task? wait;
        TaskCompletionSource<bool>? disposedStartWait = null;
        bool launch;

        lock (_gate)
        {
            _disposed = true;
            _desiredStarted = false;
            if (_activeKind != TransitionKind.Start)
            {
                disposedStartWait = TakeStartWait();
            }

            if (!_started && !_cleanupRequired && _driverRunning == 0)
            {
                wait = Task.CompletedTask;
                launch = false;
            }
            else
            {
                _cleanupWait ??= CreateCompletion();
                wait = _cleanupWait.Task;
                launch = EnsureDriverLocked();
                if (launch)
                {
                    _activeKind = TransitionKind.Cleanup;
                }
            }
        }

        LaunchDriver(launch);
        CompleteWait(disposedStartWait, new ObjectDisposedException(nameof(LifecycleTransitionCoordinator)));
        return new(wait);
    }

    /// <summary>Starts the owned component, sharing any in-flight or pending startup with other callers.</summary>
    /// <param name="cancellationToken">The token that cancels only this caller's wait.</param>
    /// <returns>The asynchronous startup task.</returns>
    /// <exception cref="ObjectDisposedException">The coordinator has been disposed.</exception>
    /// <exception cref="OperationCanceledException">The caller cancels this wait.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal Task StartAsync(CancellationToken cancellationToken) => StartCoreAsync(cancellationToken);

    /// <summary>Stops the owned component, sharing any in-flight or pending cleanup with other callers.</summary>
    /// <param name="cancellationToken">The token that cancels only this caller's wait.</param>
    /// <returns>The asynchronous cleanup task.</returns>
    /// <exception cref="OperationCanceledException">The caller cancels this wait.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal Task StopAsync(CancellationToken cancellationToken) => StopCoreAsync(cancellationToken);

    /// <summary>Creates a completion source whose continuations cannot execute inside the lifecycle lock.</summary>
    /// <returns>The new completion source.</returns>
    private static TaskCompletionSource<bool> CreateCompletion() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>Completes one shared wait outside callback execution.</summary>
    /// <param name="wait">The wait to complete.</param>
    /// <param name="failure">The failure to publish, or <see langword="null"/> for success.</param>
    private static void CompleteWait(TaskCompletionSource<bool>? wait, Exception? failure)
    {
        if (wait is null)
        {
            return;
        }

        if (failure is null)
        {
            _ = wait.TrySetResult(true);
            return;
        }

        _ = wait.TrySetException(failure);
        _ = wait.Task.Exception;
    }

    /// <summary>Waits for a shared transition while letting cancellation affect only the caller.</summary>
    /// <param name="transition">The shared transition task.</param>
    /// <param name="cancellationToken">The caller wait token.</param>
    /// <returns>The caller wait task.</returns>
    private static Task WaitForCallerAsync(Task transition, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return transition.IsCompleted || !cancellationToken.CanBeCanceled
            ? transition
            : transition.WaitAsync(Timeout.InfiniteTimeSpan, cancellationToken);
    }

    /// <summary>Starts the driver outside the lifecycle lock when this caller accepted new work.</summary>
    /// <param name="launch">Whether the driver should be launched.</param>
    private void LaunchDriver(bool launch)
    {
        if (!launch)
        {
            return;
        }

        _ = RunDriverAsync();
    }

    /// <summary>Takes and clears the shared cleanup wait.</summary>
    /// <returns>The cleanup wait to complete outside the lock.</returns>
    private TaskCompletionSource<bool>? TakeCleanupWait()
    {
        var wait = _cleanupWait;
        _cleanupWait = null;
        return wait;
    }

    /// <summary>Takes and clears the shared startup wait.</summary>
    /// <returns>The startup wait to complete outside the lock.</returns>
    private TaskCompletionSource<bool>? TakeStartWait()
    {
        var wait = _startWait;
        _startWait = null;
        return wait;
    }

    /// <summary>Ensures a driver is scheduled while the lifecycle lock is held.</summary>
    /// <returns><see langword="true"/> when the caller should launch the driver.</returns>
    private bool EnsureDriverLocked() => Interlocked.Exchange(ref _driverRunning, 1) == 0;

    /// <summary>Starts the owned component or returns the shared startup wait.</summary>
    /// <param name="cancellationToken">The caller wait token.</param>
    /// <returns>The caller startup wait.</returns>
    private Task StartCoreAsync(CancellationToken cancellationToken)
    {
        Task wait;
        var completed = false;
        var launch = false;

        lock (_gate)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ObjectDisposedExceptionHelper.ThrowIf(_disposed, this);
            if (_activeKind == TransitionKind.Start && _startWait is not null)
            {
                wait = _startWait.Task;
            }
            else if (_started && _driverRunning == 0)
            {
                wait = Task.CompletedTask;
                completed = true;
            }
            else
            {
                _desiredStarted = true;
                _startWait ??= CreateCompletion();
                wait = _startWait.Task;
                launch = EnsureDriverLocked();
                if (launch)
                {
                    _activeKind = _cleanupRequired ? TransitionKind.Cleanup : TransitionKind.Start;
                }
            }
        }

        if (completed)
        {
            return Task.CompletedTask;
        }

        LaunchDriver(launch);
        return WaitForCallerAsync(wait, cancellationToken);
    }

    /// <summary>Stops the owned component or returns the shared cleanup wait.</summary>
    /// <param name="cancellationToken">The caller wait token.</param>
    /// <returns>The caller cleanup wait.</returns>
    private Task StopCoreAsync(CancellationToken cancellationToken)
    {
        Task wait;
        var completed = false;
        var launch = false;

        lock (_gate)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_activeKind == TransitionKind.Cleanup && _cleanupWait is not null)
            {
                wait = _cleanupWait.Task;
            }
            else if (!_started && !_cleanupRequired && _driverRunning == 0)
            {
                wait = Task.CompletedTask;
                completed = true;
            }
            else
            {
                _desiredStarted = false;
                _cleanupWait ??= CreateCompletion();
                wait = _cleanupWait.Task;
                launch = EnsureDriverLocked();
                if (launch)
                {
                    _activeKind = TransitionKind.Cleanup;
                }
            }
        }

        if (completed)
        {
            return Task.CompletedTask;
        }

        LaunchDriver(launch);
        return WaitForCallerAsync(wait, cancellationToken);
    }

    /// <summary>Applies a cleanup result while the lifecycle lock is held.</summary>
    /// <param name="failure">The cleanup failure, if any.</param>
    /// <returns>The waits that must be completed after releasing the lock.</returns>
    private CompletionSet ApplyCleanupResultLocked(Exception? failure)
    {
        _activeKind = TransitionKind.None;
        _started = false;
        _cleanupRequired = failure is not null;
        var completions = new CompletionSet(TakeCleanupWait(), failure, null, null);

        if (failure is not null)
        {
            _desiredStarted = false;
            completions = completions with { StartWait = TakeStartWait(), StartFailure = failure };
            Volatile.Write(ref _driverRunning, 0);
            return completions;
        }

        return AddInactiveStartAfterCleanup(completions);
    }

    /// <summary>Adds a pending startup completion when cleanup left no startup intent to run.</summary>
    /// <param name="completions">The current completion set.</param>
    /// <returns>The updated completion set.</returns>
    private CompletionSet AddInactiveStartAfterCleanup(in CompletionSet completions)
    {
        if (_disposed)
        {
            return completions with
            {
                StartWait = TakeStartWait(),
                StartFailure = new ObjectDisposedException(nameof(LifecycleTransitionCoordinator)),
            };
        }

        return _desiredStarted
            ? completions
            : completions with
        {
            StartWait = TakeStartWait(),
            StartFailure = new InvalidOperationException("Startup was superseded by cleanup."),
        };
    }

    /// <summary>Applies a startup result while the lifecycle lock is held.</summary>
    /// <param name="failure">The startup failure, if any.</param>
    /// <returns>The waits that must be completed after releasing the lock.</returns>
    private CompletionSet ApplyStartResultLocked(Exception? failure)
    {
        _activeKind = TransitionKind.None;
        _started = failure is null;
        _cleanupRequired = true;
        var completions = new CompletionSet(null, null, TakeStartWait(), failure);

        if (failure is null)
        {
            return completions;
        }

        _desiredStarted = false;
        return completions;
    }

    /// <summary>Determines whether cleanup must run before any other accepted transition.</summary>
    /// <returns><see langword="true"/> when required cleanup should run.</returns>
    private bool ShouldRunRequiredCleanup() => !_started && _cleanupRequired && (_desiredStarted || _disposed || _cleanupWait is not null);

    /// <summary>Determines whether cleanup should run to satisfy a stop request.</summary>
    /// <returns><see langword="true"/> when stop cleanup should run.</returns>
    private bool ShouldRunStopCleanup() => _started && !_desiredStarted && _cleanupWait is not null;

    /// <summary>Determines whether startup should run to satisfy a start request.</summary>
    /// <returns><see langword="true"/> when startup should run.</returns>
    private bool ShouldRunStartup() => !_started && _desiredStarted && !_disposed;

    /// <summary>Selects cleanup as the active callback.</summary>
    /// <returns>The cleanup transition kind.</returns>
    private TransitionKind SelectCleanup()
    {
        _activeKind = TransitionKind.Cleanup;
        return TransitionKind.Cleanup;
    }

    /// <summary>Selects startup as the active callback.</summary>
    /// <returns>The startup transition kind.</returns>
    private TransitionKind SelectStartup()
    {
        _activeKind = TransitionKind.Start;
        return TransitionKind.Start;
    }

    /// <summary>Stops the lifecycle driver until another request arrives.</summary>
    /// <returns>The no-transition kind.</returns>
    private DriverSelection StopDriver()
    {
        _activeKind = TransitionKind.None;
        TaskCompletionSource<bool>? cleanupWait = null;
        if (!_started && !_cleanupRequired)
        {
            cleanupWait = TakeCleanupWait();
        }

        var completions = AddInactiveStartAfterCleanup(new(cleanupWait, null, null, null));
        Volatile.Write(ref _driverRunning, 0);
        return new(TransitionKind.None, completions);
    }

    /// <summary>Selects the next callback while the lifecycle lock is held.</summary>
    /// <returns>The selected transition kind.</returns>
    private DriverSelection SelectNextTransitionLocked()
    {
        if (_disposed)
        {
            _desiredStarted = false;
        }

        if (ShouldRunRequiredCleanup() || ShouldRunStopCleanup())
        {
            return new(SelectCleanup(), default);
        }

        return ShouldRunStartup() ? new(SelectStartup(), default) : StopDriver();
    }

    /// <summary>Runs callbacks until the coalesced desired state is reached or progress requires a later request.</summary>
    /// <returns>The driver task.</returns>
    private async Task RunDriverAsync()
    {
        TransitionKind kind;

        lock (_gate)
        {
            kind = _activeKind;
        }

        while (kind != TransitionKind.None)
        {
            var failure = await RunSelectedCallbackAsync(kind).ConfigureAwait(false);
            CompletionSet completions;
            var stopDriver = false;

            lock (_gate)
            {
                if (kind == TransitionKind.Start)
                {
                    completions = ApplyStartResultLocked(failure);
                }
                else
                {
                    completions = ApplyCleanupResultLocked(failure);
                    if (failure is not null)
                    {
                        stopDriver = true;
                    }
                }

                if (!stopDriver)
                {
                    var selected = SelectNextTransitionLocked();
                    kind = selected.Kind;
                    var selectedCompletions = selected.Completions;
                    completions = completions.Merge(in selectedCompletions);
                }
            }

            completions.Complete();
            if (stopDriver)
            {
                return;
            }
        }
    }

    /// <summary>Runs the selected application callback.</summary>
    /// <param name="kind">The callback kind.</param>
    /// <returns>The callback failure, if one occurred.</returns>
    private async Task<Exception?> RunSelectedCallbackAsync(TransitionKind kind)
    {
        try
        {
            if (kind == TransitionKind.Start)
            {
                await _start().ConfigureAwait(false);
            }
            else
            {
                await _cleanup().ConfigureAwait(false);
            }

            return null;
        }
        catch (Exception exception)
        {
            return exception;
        }
    }

    /// <summary>Stores waits selected under lock for completion after state mutation.</summary>
    /// <param name="CleanupWait">The cleanup wait, if one was selected.</param>
    /// <param name="CleanupFailure">The cleanup failure, if any.</param>
    /// <param name="StartWait">The startup wait, if one was selected.</param>
    /// <param name="StartFailure">The startup failure, if any.</param>
    private readonly record struct CompletionSet(
        TaskCompletionSource<bool>? CleanupWait,
        Exception? CleanupFailure,
        TaskCompletionSource<bool>? StartWait,
        Exception? StartFailure)
    {
        /// <summary>Completes selected waits.</summary>
        internal void Complete()
        {
            CompleteWait(CleanupWait, CleanupFailure);
            CompleteWait(StartWait, StartFailure);
        }

        /// <summary>Merges another completion set into this one.</summary>
        /// <param name="other">The additional completions.</param>
        /// <returns>The merged completion set.</returns>
        internal CompletionSet Merge(in CompletionSet other) =>
            new(
                CleanupWait ?? other.CleanupWait,
                CleanupFailure ?? other.CleanupFailure,
                StartWait ?? other.StartWait,
                StartFailure ?? other.StartFailure);
    }

    /// <summary>Stores a selected next driver action and waits to complete.</summary>
    /// <param name="Kind">The selected transition kind.</param>
    /// <param name="Completions">The waits selected by the transition decision.</param>
    private readonly record struct DriverSelection(TransitionKind Kind, CompletionSet Completions);
}
