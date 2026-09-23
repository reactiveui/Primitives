// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Owns a late stream-start operation admitted by an occasionally connected context.</summary>
internal sealed class ContextStreamStartOperation : IDisposable
{
    /// <summary>Stores the cold stream-start delegate.</summary>
    private readonly Func<Task> _start;

    /// <summary>Stores the generation token captured when the operation was admitted.</summary>
    private readonly CancellationToken _generationToken;

    /// <summary>Stores the cold driver so admission can complete before stream startup runs.</summary>
    private readonly Task<Task> _driver;

    /// <summary>Tracks whether a deferred driver-disposal continuation has been registered.</summary>
    private int _disposeContinuationRegistered;

    /// <summary>Tracks whether the cold driver has been disposed after completion.</summary>
    private int _driverDisposed;

    /// <summary>Initializes a new instance of the <see cref="ContextStreamStartOperation"/> class.</summary>
    /// <param name="start">The cold stream-start delegate.</param>
    /// <param name="generationToken">The start generation token captured at admission.</param>
    /// <exception cref="ArgumentNullException"><paramref name="start"/> is null.</exception>
    internal ContextStreamStartOperation(Func<Task> start, CancellationToken generationToken)
    {
        ArgumentExceptionHelper.ThrowIfNull(start);
        _start = start;
        _generationToken = generationToken;
        _driver = new(StartCore);
        Task = _driver.Unwrap();
    }

    /// <summary>Gets the tracked stream-start task.</summary>
    internal Task Task { get; }

    /// <inheritdoc />
    public void Dispose()
    {
        if (Task.IsCompleted)
        {
            DisposeCompletedDriverOnce();
            return;
        }

        if (Interlocked.Exchange(ref _disposeContinuationRegistered, 1) == 0)
        {
            _ = Task.ContinueWith(
                DisposeAfterCompletion,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }

    /// <summary>Starts the admitted stream operation on the supplied scheduler.</summary>
    /// <param name="scheduler">The scheduler that runs the cold driver.</param>
    /// <exception cref="ArgumentNullException"><paramref name="scheduler"/> is null.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void Start(TaskScheduler scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);
        _driver.Start(scheduler);
    }

    /// <summary>Captures the operation failure while preserving expected generation cancellation.</summary>
    /// <param name="failure">The current first failure.</param>
    /// <returns>The preserved first failure.</returns>
    internal async ValueTask<Exception?> CaptureFailureAsync(Exception? failure)
    {
        try
        {
            await Task.ConfigureAwait(false);
        }
        catch (Exception exception) when (ContextStreamStartFailurePolicy.IsExpectedCancellation(exception, _generationToken))
        {
        }
        catch (Exception exception)
        {
            failure ??= exception;
        }
        finally
        {
            DisposeCompletedDriverOnce();
        }

        return failure;
    }

    /// <summary>Disposes the completed driver after the tracked operation finishes.</summary>
    /// <param name="_">The completed tracked operation.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void DisposeAfterCompletion(Task _) => DisposeCompletedDriverOnce();

    /// <summary>Disposes the completed cold driver exactly once.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void DisposeCompletedDriverOnce()
    {
        if (Interlocked.Exchange(ref _driverDisposed, 1) == 0)
        {
            _driver.Dispose();
        }
    }

    /// <summary>Runs the admitted start after checking the captured generation token.</summary>
    /// <returns>The stream-start task.</returns>
    private Task StartCore()
    {
        _generationToken.ThrowIfCancellationRequested();
        return _start();
    }
}
