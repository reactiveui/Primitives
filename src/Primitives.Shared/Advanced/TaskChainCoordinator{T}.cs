// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Reactive.Advanced;
#else
namespace ReactiveUI.Primitives.Advanced;
#endif

/// <summary>Coordinates sequential task-source concatenation without a map adapter.</summary>
/// <typeparam name="T">The task result type.</typeparam>
[System.Diagnostics.DebuggerDisplay("TaskChainCoordinator: Queued = {_queue.Count}, Active = {_active}, Done = {_done}")]
public sealed class TaskChainCoordinator<T> : IDisposable
{
    /// <summary>Guards the queue and active/completed flags.</summary>
    private readonly Lock _gate = new();

    /// <summary>Queued task signals awaiting the active one to complete.</summary>
    private readonly Queue<IObservable<T>> _queue = new();

    /// <summary>Active subscriptions.</summary>
    private readonly MultipleDisposable _pocket = [];

    /// <summary>The downstream observer.</summary>
    private readonly IObserver<T> _observer;

    /// <summary>A value indicating whether an inner task signal is active.</summary>
    private bool _active;

    /// <summary>A value indicating whether the outer task source completed.</summary>
    private bool _outerCompleted;

    /// <summary>A value indicating whether a terminal notification has been emitted.</summary>
    private bool _done;

    /// <summary>Initializes a new instance of the <see cref="TaskChainCoordinator{T}"/> class.</summary>
    /// <param name="observer">The downstream observer.</param>
    public TaskChainCoordinator(IObserver<T> observer) => _observer = observer;

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => _pocket.Dispose();

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
            if (_done || _active)
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
                _done = true;
                _observer.OnCompleted();
            }
        }

        if (next is null)
        {
            return;
        }

        _pocket.Add(next.Subscribe(_observer.OnNext, OnError, OnInnerCompleted));
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
            if (_done)
            {
                return;
            }

            _queue.Enqueue(source);
        }

        Drain();
    }

    /// <summary>Marks the outer task source complete and pumps the drain.</summary>
    private void OnOuterCompleted()
    {
        lock (_gate)
        {
            if (_done)
            {
                return;
            }

            _outerCompleted = true;
        }

        Drain();
    }

    /// <summary>Marks the active task signal complete and pumps the drain.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void OnInnerCompleted() =>
        TaskChainCoordinatorState.OnInnerCompleted(_gate, ref _done, ref _active, this);

    /// <summary>Forwards an error and terminates active subscriptions.</summary>
    /// <param name="error">The terminal error.</param>
    private void OnError(Exception error)
    {
        lock (_gate)
        {
            if (_done)
            {
                return;
            }

            _done = true;
            _observer.OnError(error);
        }

        Dispose();
    }
}
