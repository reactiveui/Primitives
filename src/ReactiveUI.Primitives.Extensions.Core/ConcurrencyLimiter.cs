// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace ReactiveUI.Primitives.Extensions;

/// <summary>
/// Drains a task sequence with at most <paramref name="maxConcurrency"/> tasks in flight, emitting each result as it
/// completes and completing once the sequence is exhausted. A faulted or cancelled task terminates the sequence with
/// that task's exception. Subscribers share one enumerator over <paramref name="taskFunctions"/>, so a second
/// subscription continues draining where the first stopped, and disposing any subscription halts the drain for all.
/// </summary>
/// <typeparam name="T">The type of the task results.</typeparam>
/// <param name="taskFunctions">The task functions to drain.</param>
/// <param name="maxConcurrency">The maximum concurrency.</param>
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
                    subscription.Observer.OnError(innerException);
                }

                return;
            }

            subscription.Observer.OnNext(completed.Result);
            _outstanding--;
            if (_outstanding == 0 && _rator is null)
            {
                subscription.Observer.OnCompleted();
            }
            else
            {
                PullNextTask(subscription);
            }
        }
    }

    /// <summary>Pulls the next task and schedules its continuation against this limiter.</summary>
    /// <param name="subscription">The owning subscription.</param>
    private void PullNextTask(Subscription subscription)
    {
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
                    subscription.Observer.OnCompleted();
                }

                return;
            }

            _outstanding++;

            _rator.Current?.ContinueWith(
                static (ant, state) =>
                {
                    var sub = (Subscription)state!;
                    sub.Limiter.ProcessTaskCompletion(sub, ant);
                },
                subscription,
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }

    /// <summary>Pairs an observer with its limiter and reports the limiter's disposal state to the drain loop.</summary>
    /// <param name="limiter">The owning limiter.</param>
    /// <param name="observer">The downstream observer.</param>
    internal sealed class Subscription(ConcurrencyLimiter<T> limiter, IObserver<T> observer) : IDisposable
    {
        /// <summary>Gets the owning limiter.</summary>
        internal ConcurrencyLimiter<T> Limiter { get; } = limiter;

        /// <summary>Gets the downstream observer.</summary>
        internal IObserver<T> Observer { get; } = observer;

        /// <summary>Gets a value indicating whether the subscription has been disposed.</summary>
        internal bool Disposed => Limiter.Disposed;

        /// <inheritdoc/>
        public void Dispose() => Limiter.Disposed = true;
    }
}
