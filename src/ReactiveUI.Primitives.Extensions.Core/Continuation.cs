// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Extensions;

/// <summary>
/// Pairs an emitted item with a release handle so a producer can wait on its consumer: <see cref="Lock{T}"/> hands the
/// item and this instance to an observer and returns a task that completes once that handle is disposed. The barrier
/// behind it takes two participants and is torn down by <see cref="Dispose()"/>, so an instance gates one handoff.
/// </summary>
[System.Diagnostics.DebuggerDisplay("Continuation: Locked = {_locked}, CompletedPhases = {CompletedPhases}")]
public class Continuation : IDisposable
{
    /// <summary>The two-participant barrier that synchronizes phases between the gate holder and its continuation.</summary>
    private readonly Barrier _phaseSync = new(2);

    /// <summary>One once this instance has been disposed; otherwise zero.</summary>
    private int _disposedValue;

    /// <summary>One while the continuation is locked; otherwise zero.</summary>
    private int _locked;

    /// <summary>Gets the number of barrier phases that have completed.</summary>
    public long CompletedPhases => _phaseSync.CurrentPhaseNumber;

    /// <summary>Releases the gate, completing the task returned by <see cref="Lock{T}"/>, and tears down the barrier.</summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Takes the gate and hands <paramref name="item"/> to <paramref name="observer"/> paired with this instance as the
    /// release handle. A call made while the gate is held emits nothing.
    /// </summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="item">The item handed to the observer.</param>
    /// <param name="observer">The observer receiving the item and its release handle; ignored when <see langword="null"/>.</param>
    /// <returns>
    /// A <see cref="Task" /> that completes once the release handle is disposed, or a completed task when the gate was
    /// held.
    /// </returns>
    public Task Lock<T>(T item, IObserver<(T Value, IDisposable Sync)>? observer)
    {
        if (Interlocked.Exchange(ref _locked, 1) != 0)
        {
            return Task.CompletedTask;
        }

        observer?.OnNext((item, this));
        return ScheduleSignalPhase();
    }

    /// <summary><see cref="ValueTask"/>-returning counterpart to <see cref="Lock{T}"/>, for call sites that await the result exactly once.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="item">The item handed to the observer.</param>
    /// <param name="observer">The observer receiving the item and its release handle; ignored when <see langword="null"/>.</param>
    /// <returns>A <see cref="ValueTask"/> that completes once the release handle is disposed, or a completed task when the gate was held.</returns>
    public ValueTask LockValueTask<T>(T item, IObserver<(T Value, IDisposable Sync)>? observer)
    {
        if (Interlocked.Exchange(ref _locked, 1) != 0)
        {
            return default;
        }

        observer?.OnNext((item, this));
        return new(ScheduleSignalPhase());
    }

    /// <summary>Releases the gate and signals the barrier phase; a no-op when the gate is not held.</summary>
    /// <returns>A <see cref="Task"/> that completes once the barrier phase is signalled.</returns>
    internal Task UnLock() =>
        Interlocked.Exchange(ref _locked, 0) == 0 ? Task.CompletedTask : ScheduleSignalPhase();

    /// <summary>Releases unmanaged and - optionally - managed resources.</summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Concurrency",
        "SST1905:Do not use async void",
        Justification =
            "The disposal-pattern overload must return void, and no caller is positioned to observe the awaited barrier teardown.")]
    protected virtual async void Dispose(bool disposing)
    {
        if (Interlocked.Exchange(ref _disposedValue, 1) != 0 || !disposing)
        {
            return;
        }

        await UnLock().ConfigureAwait(false);
        _phaseSync.Dispose();
    }

    /// <summary>Signals the phase barrier and waits there for the paired participant.</summary>
    /// <param name="state">The owning <see cref="Continuation"/> instance.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void SignalPhaseSync(object? state) =>
        ((Continuation)state!)._phaseSync.SignalAndWait(CancellationToken.None);

    /// <summary>Runs the barrier signal on the default task scheduler.</summary>
    /// <returns>The task for the scheduled signal work.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private Task ScheduleSignalPhase() =>
        Task.Factory.StartNew(
            SignalPhaseSync,
            this,
            CancellationToken.None,
            TaskCreationOptions.DenyChildAttach,
            TaskScheduler.Default);
}
