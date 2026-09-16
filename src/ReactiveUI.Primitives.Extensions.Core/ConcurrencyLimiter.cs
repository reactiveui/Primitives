// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Extensions;

/// <summary>Runs task factories with bounded concurrency and emits results in completion order.</summary>
/// <typeparam name="T">The type of the task results.</typeparam>
/// <param name="taskFunctions">The task functions to drain.</param>
/// <param name="maxConcurrency">The maximum concurrency.</param>
/// <remarks>
/// Subscribers share progress; disposing any subscription stops every drain. A task fault or cancellation terminates the sequence with its
/// error. Results are queued under the gate and delivered after it is released, so no lock is held while an observer runs.
/// </remarks>
[System.Diagnostics.DebuggerDisplay("ConcurrencyLimiter: Outstanding = {_outstanding}, Disposed = {_disposed}")]
public sealed class ConcurrencyLimiter<T>(IEnumerable<Task<T>> taskFunctions, int maxConcurrency) : IObservable<T>
{
    /// <summary>The synchronization gate protecting task scheduling and completion state.</summary>
    private readonly Lock _gate = new();

    /// <summary>The number of tasks currently in flight that have not yet completed.</summary>
    private int _outstanding;

    /// <summary>Disposal latch set by any subscription; once set, no further tasks are pulled.</summary>
    private int _disposed;

    /// <summary>Lazy enumerator over the source task sequence; <see langword="null"/> when exhausted.</summary>
    private IEnumerator<Task<T>>? _rator;

    /// <summary>Gets this limiter as an observable sequence of task results.</summary>
    public IObservable<T> Observable => this;

    /// <summary>Gets or sets a value indicating whether any subscription has disposed the limiter.</summary>
    [SuppressMessage(
        "RoslynCommonAnalyzers",
        "SST2200:Replace this single-use backing field with the 'field' keyword",
        Justification =
            "Atomic Volatile.Read/Interlocked.Exchange need an 'int' backing field; the 'field' keyword would force 'bool'.")]
    internal bool Disposed
    {
        get => Volatile.Read(ref _disposed) != 0;
        set => Interlocked.Exchange(ref _disposed, value ? 1 : 0);
    }

    /// <inheritdoc/>
    public IDisposable Subscribe(IObserver<T> observer)
    {
        Subscription subscription = new(this, observer);
        lock (_gate)
        {
            _rator ??= taskFunctions.GetEnumerator();
        }

        for (var i = 0; i < maxConcurrency; i++)
        {
            PullNextTask(subscription);
        }

        return subscription;
    }

    /// <summary>Disposes and drops the task enumerator; callers must hold <see cref="_gate"/>.</summary>
    internal void ClearRator()
    {
        _rator?.Dispose();
        _rator = null;
    }

    /// <summary>Wraps the observer in a fresh subscription and pulls the next task for it.</summary>
    /// <param name="observer">The observer that will receive notifications.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void PullNextTask(IObserver<T> observer) =>
        PullNextTask(new Subscription(this, observer));

    /// <summary>Delivers a finished task's result and pulls the next one, or terminates the sequence on failure.</summary>
    /// <param name="subscription">The owning subscription.</param>
    /// <param name="completed">The completed task.</param>
    [SuppressMessage(
        "Concurrency",
        "PSH1315:A blocking wait on an awaitable that may not be done",
        Justification =
            "The task is complete at this call site, so reading Result does not block.")]
    internal void ProcessTaskCompletion(Subscription subscription, Task<T> completed)
    {
        var pullNext = false;
        lock (_gate)
        {
            if (subscription.Disposed || completed.IsFaulted || completed.IsCanceled)
            {
                ClearRator();
                if (!subscription.Disposed)
                {
                    var innerException = completed.Exception?.InnerExceptions is null
                        ? new OperationCanceledException()
                        : completed.Exception.InnerException!;
                    _ = subscription.PostError(innerException);
                }
            }
            else
            {
                _ = subscription.Post(completed.Result);
                _outstanding--;
                if (_outstanding == 0 && _rator is null)
                {
                    _ = subscription.PostCompleted();
                }
                else
                {
                    pullNext = true;
                }
            }
        }

        subscription.Flush();
        if (!pullNext)
        {
            return;
        }

        PullNextTask(subscription);
    }

    /// <summary>Registers result delivery for the pending task.</summary>
    /// <param name="task">The pending task.</param>
    /// <param name="subscription">The result recipient.</param>
    [ExcludeFromCodeCoverage]
    private static void RegisterCompletion(Task<T> task, Subscription subscription) =>
        _ = task.ContinueWith(
            static (completed, state) =>
            {
                var owner = (Subscription)state!;
                owner.Limiter.ProcessTaskCompletion(owner, completed);
            },
            subscription,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

    /// <summary>Pulls the next task under the gate, then registers its continuation after releasing it.</summary>
    /// <param name="subscription">The owning subscription.</param>
    private void PullNextTask(Subscription subscription)
    {
        Task<T>? task = null;
        lock (_gate)
        {
            if (subscription.Disposed)
            {
                ClearRator();
            }

            if (_rator is null)
            {
                return;
            }

            if (!_rator.MoveNext())
            {
                ClearRator();
                if (_outstanding == 0)
                {
                    _ = subscription.PostCompleted();
                }
            }
            else
            {
                _outstanding++;
                task = _rator.Current;
            }
        }

        if (task is null)
        {
            subscription.Flush();
            return;
        }

        RegisterCompletion(task, subscription);
    }

    /// <summary>Pairs an observer with its limiter and reports the limiter's disposal state to the drain loop.</summary>
    /// <param name="limiter">The owning limiter.</param>
    /// <param name="observer">The downstream observer.</param>
    internal sealed class Subscription(ConcurrencyLimiter<T> limiter, IObserver<T> observer) : IDisposable
    {
        /// <summary>Serializes deliveries to <see cref="Observer"/>.</summary>
        private SerializedDelivery<T> _delivery = new();

        /// <summary>Gets the owning limiter.</summary>
        internal ConcurrencyLimiter<T> Limiter { get; } = limiter;

        /// <summary>Gets the downstream observer.</summary>
        internal IObserver<T> Observer { get; } = observer;

        /// <summary>Gets a value indicating whether the subscription has been disposed.</summary>
        internal bool Disposed => Limiter.Disposed;

        /// <inheritdoc/>
        public void Dispose() => Limiter.Disposed = true;

        /// <summary>Queues a result for <see cref="Observer"/> while the limiter's gate is held.</summary>
        /// <param name="value">The result.</param>
        /// <returns><see langword="true"/> when the result was queued.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool Post(T value) => _delivery.Post(value);

        /// <summary>Queues an error as the terminal notification while the limiter's gate is held.</summary>
        /// <param name="error">The error.</param>
        /// <returns><see langword="true"/> when this is the first terminal notification.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool PostError(Exception error) => _delivery.PostError(error);

        /// <summary>Queues completion as the terminal notification while the limiter's gate is held.</summary>
        /// <returns><see langword="true"/> when this is the first terminal notification.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool PostCompleted() => _delivery.PostCompleted();

        /// <summary>Delivers the queued notifications; call it after releasing the limiter's gate.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Flush() => _delivery.Flush(new PendingDrain(this));

        /// <summary>Drains this subscription's queued notifications for the delivery gate.</summary>
        /// <param name="Owner">The subscription.</param>
        private readonly record struct PendingDrain(Subscription Owner) : IDrainTarget
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Drain() => _ = Owner._delivery.DrainTo(Owner.Observer);
        }
    }
}
