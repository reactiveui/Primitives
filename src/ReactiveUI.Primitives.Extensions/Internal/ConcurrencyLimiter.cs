// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics.CodeAnalysis;

namespace ReactiveUI.Primitives.Extensions.Internal;

/// <summary>
/// Limits the concurrency of task execution and emits results through an observable sequence.
/// Implements <see cref="IObservable{T}"/> directly so the surface needs no
/// <c>ActionDisposable</c> closure wrappers; the per-task continuation state is the
/// per-subscription <see cref="Subscription"/> instance, which is already a reference type
/// and therefore needs no boxing through <see cref="object"/>.
/// </summary>
/// <typeparam name="T">The type of the task results.</typeparam>
internal sealed class ConcurrencyLimiter<T> : IObservable<T>
{
    /// <summary>The synchronization gate protecting task scheduling and completion state.</summary>
    private readonly Lock _gate = new();

    /// <summary>Source enumerable; the enumerator is pulled lazily on first <see cref="Subscribe"/>.</summary>
    private readonly IEnumerable<Task<T>> _taskFunctions;

    /// <summary>Maximum concurrent task continuations.</summary>
    private readonly int _maxConcurrency;

    /// <summary>The number of tasks currently in flight that have not yet completed.</summary>
    private int _outstanding;

    /// <summary>Global disposal latch set by any <see cref="Subscription.Dispose"/>. Preserves the
    /// existing single-subscription-at-a-time semantics: once any consumer disposes, the limiter
    /// stops pulling further tasks.</summary>
    private int _disposed;

    /// <summary>Lazy enumerator over the source task sequence; <see langword="null"/> once exhausted.</summary>
    private IEnumerator<Task<T>>? _rator;

    /// <summary>Initializes a new instance of the <see cref="ConcurrencyLimiter{T}"/> class.</summary>
    /// <param name="taskFunctions">The task functions to drain.</param>
    /// <param name="maxConcurrency">The maximum concurrency.</param>
    public ConcurrencyLimiter(IEnumerable<Task<T>> taskFunctions, int maxConcurrency)
    {
        _taskFunctions = taskFunctions;
        _maxConcurrency = maxConcurrency;
    }

    /// <summary>Gets the observable sequence — the limiter is its own <see cref="IObservable{T}"/>.</summary>
    public IObservable<T> Observable => this;

    /// <summary>Gets or sets a value indicating whether the limiter has been disposed by any
    /// consumer. Exposed to internal tests; production paths set it via
    /// <see cref="Subscription.Dispose"/>.</summary>
    [SuppressMessage(
        "RoslynCommonAnalyzers",
        "SST2200:Replace this single-use backing field with the 'field' keyword",
        Justification = "Atomic Volatile.Read/Interlocked.Exchange need an 'int' backing field; the 'field' keyword would force 'bool'.")]
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
            _rator ??= _taskFunctions.GetEnumerator();
        }

        for (var i = 0; i < _maxConcurrency; i++)
        {
            PullNextTask(subscription);
        }

        return subscription;
    }

    /// <summary>Clears the lazy enumerator. Caller must hold <see cref="_gate"/> on the production
    /// paths; exposed to internal tests that exercise the idempotent-second-call branch.</summary>
    internal void ClearRator()
    {
        _rator?.Dispose();
        _rator = null;
    }

    /// <summary>Test entry point that adapts a raw <see cref="IObserver{T}"/> into a fresh
    /// <see cref="Subscription"/> and pulls the next task. Production paths go through
    /// <see cref="Subscribe"/> which creates the subscription once.</summary>
    /// <param name="observer">The observer that will receive notifications.</param>
    internal void PullNextTask(IObserver<T> observer) =>
        PullNextTask(new Subscription(this, observer));

    /// <summary>Processes the completion of a previously-scheduled task.</summary>
    /// <param name="subscription">The owning subscription.</param>
    /// <param name="completed">The completed task.</param>
    [SuppressMessage(
        "Major Bug",
        "S4462:Calls to async methods should not be blocking",
        Justification = "Task is guaranteed complete at this call site (IsFaulted/IsCanceled were both false above); reading .Result drives the synchronous IObserver<T> contract without blocking.")]
    private void ProcessTaskCompletion(Subscription subscription, Task<T> completed)
    {
        lock (_gate)
        {
            if (subscription.Disposed || completed.IsFaulted || completed.IsCanceled)
            {
                ClearRator();
                if (!subscription.Disposed)
                {
                    subscription.Observer.OnError((completed.Exception is null
                        ? new OperationCanceledException()
                        : completed.Exception.InnerException)!);
                }

                return;
            }

            subscription.Observer.OnNext(completed.Result);
            if (--_outstanding == 0 && _rator is null)
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

            // The continuation passes the Subscription as state — already a reference type, so
            // no per-task ValueTuple boxing is needed. The static lambda preserves zero closure
            // capture.
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

    /// <summary>Per-subscription handle: holds the observer reference and a disposal latch.
    /// Replaces the previous <c>ActionDisposable(() =&gt; Disposed = true)</c> pattern with a
    /// dedicated class — no closure object per subscribe.</summary>
    /// <param name="limiter">The owning limiter.</param>
    /// <param name="observer">The downstream observer.</param>
    internal sealed class Subscription(ConcurrencyLimiter<T> limiter, IObserver<T> observer) : IDisposable
    {
        /// <summary>Gets the owning limiter.</summary>
        public ConcurrencyLimiter<T> Limiter { get; } = limiter;

        /// <summary>Gets the downstream observer.</summary>
        public IObserver<T> Observer { get; } = observer;

        /// <summary>Gets a value indicating whether the subscription has been disposed.</summary>
        public bool Disposed => Limiter.Disposed;

        /// <inheritdoc/>
        public void Dispose() => Limiter.Disposed = true;
    }
}
