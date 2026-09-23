// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.OccasionallyConnected;

/// <summary>Coordinates bounded FIFO snapshot recovery permits under the engine's shared gate.</summary>
internal sealed class SnapshotRecoveryAdmissionQueue
{
    /// <summary>The same lock that guards engine recovery markers and parked upload heads.</summary>
    private readonly Lock _gate;

    /// <summary>The maximum simultaneously admitted recoveries.</summary>
    private readonly int _capacity;

    /// <summary>The FIFO waiters that do not yet own a permit.</summary>
    private readonly LinkedList<Admission> _waiters = [];

    /// <summary>The number of permits currently owned.</summary>
    private int _active;

    /// <summary>Initializes a new instance of the <see cref="SnapshotRecoveryAdmissionQueue"/> class.</summary>
    /// <param name="gate">The shared engine gate.</param>
    /// <param name="capacity">The positive concurrent recovery bound.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is not positive.</exception>
    internal SnapshotRecoveryAdmissionQueue(Lock gate, int capacity)
    {
        ArgumentOutOfRangeExceptionHelper.ThrowIfNegativeOrZero(capacity);

        _gate = gate;
        _capacity = capacity;
    }

    /// <summary>Creates an admission while the caller holds the shared engine gate.</summary>
    /// <param name="cancellationToken">The receive cancellation token.</param>
    /// <returns>The admission to publish after releasing the gate.</returns>
    internal Admission EnqueueLocked(CancellationToken cancellationToken)
    {
        var granted = _active < _capacity;
        var admission = new Admission(this, granted, cancellationToken);
        if (granted)
        {
            _active++;
            admission.OwnsPermit = true;
            admission.Completed = true;
        }
        else
        {
            _ = _waiters.AddLast(admission);
        }

        return admission;
    }

    /// <summary>Completes one recovery while the caller holds the shared engine gate.</summary>
    /// <param name="admission">The admission owned by the recovery.</param>
    /// <returns>The external task and registration transitions to publish after releasing the gate.</returns>
    internal CompletionActions CompleteLocked(Admission admission)
    {
        ArgumentExceptionHelper.ThrowIfNull(admission);
        if (admission.Finalized)
        {
            return default;
        }

        admission.Finalized = true;
        if (admission.OwnsPermit)
        {
            admission.OwnsPermit = false;
            return new(admission, false, ReleaseNextLocked());
        }

        if (!admission.Completed)
        {
            RemoveQueuedLocked(admission);
            return new(admission, CancelCurrent: true, Next: null);
        }

        return new(admission, CancelCurrent: false, Next: null);
    }

    /// <summary>Releases a permit to the next live FIFO waiter while the shared gate is held.</summary>
    /// <returns>The next granted waiter, if any.</returns>
    private Admission? ReleaseNextLocked()
    {
        _active--;
        if (_waiters.First is { } node)
        {
            var next = node.Value;
            _waiters.Remove(node);
            _active++;
            next.OwnsPermit = true;
            next.Completed = true;
            return next;
        }

        return null;
    }

    /// <summary>Removes an incomplete waiter with a bounded search over registered streams.</summary>
    /// <param name="admission">The incomplete admission.</param>
    private void RemoveQueuedLocked(Admission admission)
    {
        _ = _waiters.Remove(admission);
        admission.Completed = true;
    }

    /// <summary>Publishes one completed transition after releasing the shared gate.</summary>
    /// <param name="Current">The completed admission, if any.</param>
    /// <param name="CancelCurrent">Whether its pending task must be canceled.</param>
    /// <param name="Next">The next granted waiter, if any.</param>
    internal readonly record struct CompletionActions(Admission? Current, bool CancelCurrent, Admission? Next)
    {
        /// <summary>Completes tasks and disposes cancellation registration outside the engine gate.</summary>
        internal void Publish()
        {
            if (Current is not { } current)
            {
                return;
            }

            if (CancelCurrent)
            {
                current.CancelAdmission();
            }

            current.DisposeRegistration();
            Next?.ReleaseAdmission();
        }
    }

    /// <summary>Stores one bounded recovery admission and its cancellation ownership.</summary>
    internal sealed class Admission
    {
        /// <summary>The owning queue.</summary>
        private readonly SnapshotRecoveryAdmissionQueue _owner;

        /// <summary>The receive cancellation token.</summary>
        private readonly CancellationToken _cancellationToken;

        /// <summary>Whether the queue granted this admission at insertion.</summary>
        private readonly bool _initiallyGranted;

        /// <summary>The task completed when the admission is granted or canceled.</summary>
        private readonly TaskCompletionSource<bool> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The queued cancellation callback registration.</summary>
        private CancellationTokenRegistration _registration;

        /// <summary>Initializes a new instance of the <see cref="Admission"/> class.</summary>
        /// <param name="owner">The owning queue.</param>
        /// <param name="initiallyGranted">Whether a permit was available at insertion.</param>
        /// <param name="cancellationToken">The receive cancellation token.</param>
        internal Admission(SnapshotRecoveryAdmissionQueue owner, bool initiallyGranted, CancellationToken cancellationToken)
        {
            _owner = owner;
            _cancellationToken = cancellationToken;
            _initiallyGranted = initiallyGranted;
        }

        /// <summary>Gets or sets whether this admission owns one bounded permit.</summary>
        internal bool OwnsPermit { get; set; }

        /// <summary>Gets or sets whether grant or cancellation was decided under the shared gate.</summary>
        internal bool Completed { get; set; }

        /// <summary>Gets or sets whether recovery completion already accounted for this admission.</summary>
        internal bool Finalized { get; set; }

        /// <summary>Gets the admission task.</summary>
        internal Task Task => _completion.Task;

        /// <summary>Publishes the initial grant or registers cancellation outside the shared gate.</summary>
        internal void PublishInitial()
        {
            if (_initiallyGranted)
            {
                ReleaseAdmission();
            }
            else
            {
                RegisterCancellation();
            }
        }

        /// <summary>Cancels a queued waiter; this is also the registered token callback.</summary>
        internal void TryCancel()
        {
            lock (_owner._gate)
            {
                if (Completed)
                {
                    return;
                }

                _owner.RemoveQueuedLocked(this);
            }

            CancelAdmission();
        }

        /// <summary>Completes this waiter as canceled.</summary>
        internal void CancelAdmission() => _ = _completion.TrySetCanceled(_cancellationToken);

        /// <summary>Disposes the cancellation callback registration outside the shared gate.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void DisposeRegistration() => _registration.Dispose();

        /// <summary>Grants admission after disposing any queued callback registration.</summary>
        internal void ReleaseAdmission()
        {
            DisposeRegistration();
            _ = _completion.TrySetResult(true);
        }

        /// <summary>Registers receive cancellation after queue insertion.</summary>
        private void RegisterCancellation()
        {
            var registration = _cancellationToken.Register(TryCancel);
            lock (_owner._gate)
            {
                if (!Completed)
                {
                    _registration = registration;
                    return;
                }
            }

            registration.Dispose();
        }
    }
}
