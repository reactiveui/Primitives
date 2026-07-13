// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions;

/// <summary>Coordinates phase synchronization between a lock holder and its continuation.</summary>
public class Continuation : IDisposable
{
    /// <summary>The barrier used to synchronize phases between the lock holder and the continuation.</summary>
    private readonly Barrier _phaseSync = new(2);

    /// <summary>A value indicating whether this instance has been disposed.</summary>
    private bool _disposedValue;

    /// <summary>A value indicating whether the continuation is currently locked.</summary>
    private bool _locked;

    /// <summary>Gets the number of completed phases.</summary>
    /// <value>
    /// The completed phases.
    /// </value>
    public long CompletedPhases => _phaseSync.CurrentPhaseNumber;

    /// <summary>Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.</summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>Locks this instance.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="item">The item.</param>
    /// <param name="observer">The observer.</param>
    /// <returns>
    /// A <see cref="Task" /> representing the asynchronous operation.
    /// </returns>
    public Task Lock<T>(T item, IObserver<(T value, IDisposable Sync)>? observer)
    {
        if (_locked)
        {
            return Task.CompletedTask;
        }

        _locked = true;
        observer?.OnNext((item, this));
        return ScheduleSignalPhase();
    }

    /// <summary>
    /// <see cref="ValueTask"/>-returning counterpart to <see cref="Lock{T}"/>. Use this at per-emission
    /// call sites where the returned task is awaited exactly once — saves the boxed <see cref="Task"/>
    /// wrapper allocation in the already-locked fast path.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="item">The item.</param>
    /// <param name="observer">The observer.</param>
    /// <returns>A <see cref="ValueTask"/> representing the asynchronous operation.</returns>
    public ValueTask LockValueTask<T>(T item, IObserver<(T value, IDisposable Sync)>? observer)
    {
        if (_locked)
        {
            return default;
        }

        _locked = true;
        observer?.OnNext((item, this));
        return new(ScheduleSignalPhase());
    }

    /// <summary>UnLocks this instance.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous operation.</returns>
    internal Task UnLock()
    {
        if (!_locked)
        {
            return Task.CompletedTask;
        }

        _locked = false;
        return ScheduleSignalPhase();
    }

    /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    protected virtual async void Dispose(bool disposing)
    {
        if (_disposedValue)
        {
            return;
        }

        if (disposing)
        {
            await UnLock().ConfigureAwait(false);
            _phaseSync.Dispose();
        }

        _disposedValue = true;
    }

    /// <summary>Static state-carrying signal callback; avoids the per-call closure allocation a captured lambda would produce.</summary>
    /// <param name="state">The owning <see cref="Continuation"/> instance.</param>
    private static void SignalPhaseSync(object? state) =>
        ((Continuation)state!)._phaseSync.SignalAndWait(CancellationToken.None);

    /// <summary>Schedules <see cref="SignalPhaseSync"/> on the default task scheduler. Hoisted
    /// out of the <see cref="Lock{T}"/> and <see cref="UnLock"/> call sites because cobertura
    /// tags the multi-argument <c>Task.Factory.StartNew(...)</c> call as a branch line — the
    /// per-call overload-resolution metadata is collapsed here so it counts once.</summary>
    /// <returns>The task representing the scheduled signal work.</returns>
    private Task ScheduleSignalPhase() =>
        Task.Factory.StartNew(
            SignalPhaseSync,
            this,
            CancellationToken.None,
            TaskCreationOptions.DenyChildAttach,
            TaskScheduler.Default);
}
