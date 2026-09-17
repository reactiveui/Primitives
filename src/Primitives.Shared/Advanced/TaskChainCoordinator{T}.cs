// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Extensions;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Coordinates sequential task-source concatenation without a map adapter.</summary>
/// <typeparam name="T">The task result type.</typeparam>
/// <remarks>
/// The gate only guards the queue and flags. Deliveries are serialized by a <see cref="SerializedDelivery{T}"/> and the
/// next task signal is subscribed after the gate is released, so no lock is held while the observer runs, whichever
/// thread the outer source or a task completion arrives on. A terminal notification raised before <see cref="Dispose"/>
/// is still delivered; nothing raised after it is.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("TaskChainCoordinator: Queued = {_queue.Count}, Active = {_active}, Done = {_done}")]
public sealed class TaskChainCoordinator<T> : IDisposable
{
    /// <summary>Guards the queue and flags; never held while user code runs.</summary>
    private readonly Lock _gate = new();

    /// <summary>Queued task signals awaiting the active one to complete.</summary>
    private readonly Queue<IObservable<T>> _queue = new();

    /// <summary>Active subscriptions.</summary>
    private readonly MultipleDisposable _pocket = [];

    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>Serializes downstream deliveries.</summary>
    private SerializedDelivery<T> _delivery = new();

    /// <summary>A value indicating whether an inner task signal is active.</summary>
    private bool _active;

    /// <summary>A value indicating whether the outer task source completed.</summary>
    private bool _outerCompleted;

    /// <summary>Whether a terminal notification has been queued or the coordinator disposed; written under the gate.</summary>
    private bool _done;

    /// <summary>Initializes a new instance of the <see cref="TaskChainCoordinator{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public TaskChainCoordinator(IObserver<T> observer) => _observer = observer;

    /// <inheritdoc/>
    public void Dispose()
    {
        lock (_gate)
        {
            Volatile.Write(ref _done, true);
            _queue.Clear();
        }

        _pocket.Dispose();
    }

    /// <summary>Subscribes to the outer task source.</summary>
    /// <param name="sources">The outer task source.</param>
    /// <returns>The coordinator that owns the subscription cleanup.</returns>
    public TaskChainCoordinator<T> Run(IObservable<Task<T>> sources)
    {
        _pocket.Add(sources.Subscribe(OnTask, OnError, OnOuterCompleted));
        return this;
    }

    /// <summary>Subscribes the next queued task signal, or completes when the task chain is drained.</summary>
    internal void Drain()
    {
        IObservable<T>? next = null;
        lock (_gate)
        {
            if (_done)
            {
                _queue.Clear();
                return;
            }

            if (_active)
            {
                return;
            }

            if (_queue.Count > 0)
            {
                _active = true;
                next = _queue.Dequeue();
            }
            else if (_outerCompleted)
            {
                Volatile.Write(ref _done, true);
                _ = _delivery.PostCompleted();
            }
            else
            {
                return;
            }
        }

        if (next is null)
        {
            _delivery.Flush(new PendingDrain(this));
            return;
        }

        _pocket.Add(next.Subscribe(OnInnerNext, OnError, OnInnerCompleted));
    }

    /// <summary>Queues a task as a task-backed signal and pumps the drain.</summary>
    /// <param name="task">The task to observe in source order.</param>
    private void OnTask(Task<T> task)
    {
        IObservable<T> source;
        try
        {
            source = Signal.FromTask(task);
        }
        catch (Exception error) when (!FatalExceptionHelper.IsFatal(error))
        {
            OnError(error);
            return;
        }

        lock (_gate)
        {
            _queue.Enqueue(source);
        }

        Drain();
    }

    /// <summary>Marks the outer task source complete and pumps the drain.</summary>
    private void OnOuterCompleted()
    {
        lock (_gate)
        {
            _outerCompleted = true;
        }

        Drain();
    }

    /// <summary>Marks the active task signal complete and pumps the drain.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnInnerCompleted() =>
        TaskChainCoordinatorState.OnInnerCompleted(_gate, ref _done, ref _active, this);

    /// <summary>Forwards a task result, directly when nothing else is delivering.</summary>
    /// <param name="value">The task result.</param>
    /// <remarks>A result after the terminal is refused by the delivery, and a task signal stops once its subscription is disposed.</remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnInnerNext(T value) => _delivery.OnNext(_observer, value, new PendingDrain(this));

    /// <summary>Queues the first terminal error and terminates active subscriptions.</summary>
    /// <param name="error">The terminal error.</param>
    private void OnError(Exception error)
    {
        lock (_gate)
        {
            if (_done)
            {
                return;
            }

            Volatile.Write(ref _done, true);
            _queue.Clear();
            _ = _delivery.PostError(error);
        }

        _delivery.Flush(new PendingDrain(this));
        _pocket.Dispose();
    }

    /// <summary>Drains this coordinator's queued notifications for the delivery gate.</summary>
    /// <param name="Owner">The coordinator.</param>
    private readonly record struct PendingDrain(TaskChainCoordinator<T> Owner) : IDrainTarget
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Drain() => _ = Owner._delivery.DrainTo(Owner._observer);
    }
}
