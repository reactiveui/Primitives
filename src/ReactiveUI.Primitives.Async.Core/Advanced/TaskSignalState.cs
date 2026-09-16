// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Async.Advanced;

/// <summary>The run and dispose state of a subscription that runs one cancellable asynchronous job feeding a single observer.</summary>
/// <remarks>
/// A job that throws completes the observer with a failure. <see cref="DisposeAsync"/> cancels the job and waits for it to
/// finish, except when called from inside the job, where waiting would deadlock; <see cref="Dispose"/> cancels without
/// waiting. The running job's task is the completion signal, so the state costs no allocation beyond the job itself.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("TaskSignalState: Disposed = {_disposed}, Started = {_run != null}")]
public sealed class TaskSignalState : IAsyncDisposable, IDisposable
{
    /// <summary>Cancels the job on disposal.</summary>
    private readonly CancellationTokenSource _cancellation = new();

    /// <summary>Set while the job runs, so a disposal from inside it skips the join.</summary>
    private readonly AsyncLocal<bool> _executing = new();

    /// <summary>The running job, or <see langword="null"/> before <see cref="Start{TJob, T}"/>.</summary>
    private Task? _run;

    /// <summary>Set on the first disposal so later calls are no-ops.</summary>
    private int _disposed;

    /// <summary>Runs a job, completing the observer with a failure when the job throws.</summary>
    /// <typeparam name="TJob">The job type.</typeparam>
    /// <typeparam name="T">The type of the elements the job delivers.</typeparam>
    /// <param name="job">The job to run.</param>
    /// <param name="observer">The observer that receives notifications.</param>
    /// <param name="cancellationToken">A token that cancels the job.</param>
    /// <returns>A task representing the asynchronous job.</returns>
    public static async ValueTask ExecuteAsync<TJob, T>(TJob job, IObserverAsync<T> observer, CancellationToken cancellationToken)
        where TJob : ITaskSignalJob<T>
    {
        try
        {
            await job.ExecuteAsync(observer, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception e)
        {
            await CompleteWithFailureAsync(observer, e).ConfigureAwait(false);
        }
    }

    /// <summary>Forwards a failure result, reporting a throwing completion handler to the unhandled exception handler.</summary>
    /// <typeparam name="T">The type of the elements the observer receives.</typeparam>
    /// <param name="observer">The observer to complete.</param>
    /// <param name="error">The original exception.</param>
    /// <returns>A task representing the operation.</returns>
    public static async ValueTask CompleteWithFailureAsync<T>(IObserverAsync<T> observer, Exception error)
    {
        try
        {
            await observer.OnCompletedAsync(Result.Failure(error)).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            UnhandledExceptionHandler.ReportUnhandledException(exception);
        }
    }

    /// <summary>Starts the job and returns without waiting for it to finish.</summary>
    /// <typeparam name="TJob">The job type.</typeparam>
    /// <typeparam name="T">The type of the elements the job delivers.</typeparam>
    /// <param name="job">The job to run.</param>
    /// <param name="observer">The observer that receives notifications.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Start<TJob, T>(TJob job, IObserverAsync<T> observer)
        where TJob : ITaskSignalJob<T> =>
        Volatile.Write(ref _run, RunAsync(job, observer, _cancellation.Token).AsTask());

    /// <summary>Cancels the job and waits for it to finish, unless called from inside the job; later calls do nothing.</summary>
    /// <returns>A task representing the asynchronous dispose operation.</returns>
    public ValueTask DisposeAsync() =>
        Interlocked.Exchange(ref _disposed, 1) != 0 ? default : JoinAsync();

    /// <summary>Cancels the job without waiting for it, releasing the cancellation source once the job finishes; later calls do nothing.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _cancellation.Cancel();
        var run = Volatile.Read(ref _run);
        if (run is null)
        {
            _cancellation.Dispose();
            return;
        }

        _ = run.ContinueWith(
            static (_, state) => ((CancellationTokenSource)state!).Dispose(),
            _cancellation,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <summary>Runs the job with the executing flag set.</summary>
    /// <typeparam name="TJob">The job type.</typeparam>
    /// <typeparam name="T">The type of the elements the job delivers.</typeparam>
    /// <param name="job">The job to run.</param>
    /// <param name="observer">The observer that receives notifications.</param>
    /// <param name="cancellationToken">A token that cancels the job.</param>
    /// <returns>A task representing the asynchronous job; it never faults.</returns>
    private async ValueTask RunAsync<TJob, T>(TJob job, IObserverAsync<T> observer, CancellationToken cancellationToken)
        where TJob : ITaskSignalJob<T>
    {
        _executing.Value = true;
        await ExecuteAsync(job, observer, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Cancels the job and joins it unless running inside it, then releases the cancellation source.</summary>
    /// <returns>A task representing the asynchronous dispose operation.</returns>
    private async ValueTask JoinAsync()
    {
        await _cancellation.CancelAsync().ConfigureAwait(false);
        var run = Volatile.Read(ref _run);
        if (run is not null && !_executing.Value)
        {
            await run.ConfigureAwait(false);
        }

        _cancellation.Dispose();
    }
}
