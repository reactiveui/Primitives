// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions;

/// <summary>Pairs an emitted item with a release handle and completes the producer's task when the handle is disposed.</summary>
[System.Diagnostics.DebuggerDisplay("Continuation: Locked = {_locked}, CompletedPhases = {CompletedPhases}")]
public class Continuation : IDisposable
{
    /// <summary>Serializes changes to the current handoff.</summary>
    private readonly Lock _gate = new();

    /// <summary>The most recently acquired handoff.</summary>
    private Phase? _phase;

    /// <summary>The number of handoffs whose delivery and release have both completed.</summary>
    private long _completedPhases;

    /// <summary>One once this instance has been disposed; otherwise zero.</summary>
    private int _disposedValue;

    /// <summary>One while the continuation is locked; otherwise zero.</summary>
    private int _locked;

    /// <summary>Whether managed disposal has been requested.</summary>
    private bool _managedDisposed;

    /// <summary>Gets the number of handoffs whose delivery and release have both completed.</summary>
    public long CompletedPhases
    {
        get
        {
            lock (_gate)
            {
                return _completedPhases;
            }
        }
    }

    /// <summary>Releases the active handoff and disposes the continuation.</summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>Emits an item paired with this release handle, or drops the item while another handoff holds the gate.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="item">The item handed to the observer.</param>
    /// <param name="observer">The observer receiving the item and its release handle; ignored when <see langword="null"/>.</param>
    /// <returns>A task completed after delivery and release, or a completed task when the item was dropped.</returns>
    public Task Lock<T>(T item, IObserver<(T Value, IDisposable Sync)>? observer)
    {
        var phase = TryBeginPhase();
        if (phase is null)
        {
            return Task.CompletedTask;
        }

        observer?.OnNext((item, this));
        CompleteDelivery(phase);
        return phase.Completion.Task;
    }

    /// <summary>Emits an item with this release handle and returns a value task for its handoff.</summary>
    /// <typeparam name="T">The type of the elements in the source sequence.</typeparam>
    /// <param name="item">The item handed to the observer.</param>
    /// <param name="observer">The observer receiving the item and its release handle; ignored when <see langword="null"/>.</param>
    /// <returns>A value task completed after delivery and release, or a completed value task when the item was dropped.</returns>
    public ValueTask LockValueTask<T>(T item, IObserver<(T Value, IDisposable Sync)>? observer)
    {
        var handoff = Lock(item, observer);
        return ReferenceEquals(handoff, Task.CompletedTask) ? default : new(handoff);
    }

    /// <summary>Releases the current handoff; repeated releases have no effect.</summary>
    /// <returns>The handoff task, completed once its delivery has returned.</returns>
    internal Task UnLock()
    {
        var completion = Task.CompletedTask;
        lock (_gate)
        {
            if (_locked != 0)
            {
                _locked = 0;
                var phase = _phase!;
                phase.Released = true;
                TryCompletePhase(phase);
                completion = phase.Completion.Task;
            }
        }

        return completion;
    }

    /// <summary>Acquires a new handoff while the gate is free.</summary>
    /// <returns>The acquired handoff, or null when another handoff holds the gate.</returns>
    internal Phase? TryBeginPhase()
    {
        lock (_gate)
        {
            if (_locked != 0)
            {
                return null;
            }

            _locked = 1;
            _phase = new(_managedDisposed);
            return _phase;
        }
    }

    /// <summary>Records delivery completion and completes a handoff that has already been released.</summary>
    /// <param name="phase">The handoff whose observer callback returned.</param>
    internal void CompleteDelivery(Phase phase)
    {
        lock (_gate)
        {
            phase.Delivered = true;
            TryCompletePhase(phase);
        }
    }

    /// <summary>Releases the resources held by this continuation.</summary>
    /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
    protected virtual void Dispose(bool disposing)
    {
        lock (_gate)
        {
            if (_disposedValue != 0)
            {
                return;
            }

            _disposedValue = 1;
            if (!disposing)
            {
                return;
            }

            _managedDisposed = true;
        }

        _ = UnLock();
    }

    /// <summary>Completes a released delivery once, or faults a handoff acquired after disposal.</summary>
    /// <param name="phase">The handoff to complete.</param>
    private void TryCompletePhase(Phase phase)
    {
        if (phase.IsDisposed)
        {
            _ = phase.Completion.TrySetException(new ObjectDisposedException(nameof(Continuation)));
            return;
        }

        if (!phase.Delivered || !phase.Released || phase.Completion.Task.IsCompleted)
        {
            return;
        }

        _completedPhases++;
        phase.Completion.SetResult(true);
    }

    /// <summary>Tracks delivery and release for one acquired handoff.</summary>
    /// <param name="isDisposed">True when the handoff was acquired after managed disposal.</param>
    internal sealed class Phase(bool isDisposed)
    {
        /// <summary>Gets the completion shared by the delivery and release participants.</summary>
        internal TaskCompletionSource<bool> Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets a value indicating whether the handoff was acquired after managed disposal.</summary>
        internal bool IsDisposed { get; } = isDisposed;

        /// <summary>Gets or sets a value indicating whether the observer callback returned.</summary>
        internal bool Delivered { get; set; }

        /// <summary>Gets or sets a value indicating whether the handoff was released.</summary>
        internal bool Released { get; set; }
    }
}
