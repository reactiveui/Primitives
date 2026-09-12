// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

#if REACTIVE_SHIM
namespace ReactiveUI.Primitives.Async.Reactive;
#else
namespace ReactiveUI.Primitives.Async;
#endif

/// <summary>An execution context that pins continuations to a <see cref="SynchronizationContext"/>, a <see cref="TaskScheduler"/>, or an <see cref="ISequencer"/>.</summary>
[System.Diagnostics.DebuggerDisplay(
"AsyncContext: SynchronizationContext = {SynchronizationContext}, TaskScheduler = {TaskScheduler}, Sequencer = {Sequencer}")]
public sealed record AsyncContext
{
    /// <summary>Initializes a new instance of the <see cref="AsyncContext"/> class.</summary>
    private AsyncContext()
    {
    }

    /// <summary>Gets the context that schedules continuations on the default task scheduler.</summary>
    public static AsyncContext Default { get; } = new();

    /// <summary>Gets the synchronization context to use for marshaling callbacks and continuations.</summary>
    /// <remarks>A specified synchronization context receives posted continuations; otherwise the task scheduler determines execution.</remarks>
    public SynchronizationContext? SynchronizationContext { get; init; }

    /// <summary>Gets the task scheduler to use for scheduling tasks, or null to use the default scheduler.</summary>
    public TaskScheduler? TaskScheduler { get; init; }

    /// <summary>Gets the sequencer used to schedule continuations, or <see langword="null"/> when another context shape is used.</summary>
    public ISequencer? Sequencer { get; init; }

    /// <summary>Gets a value indicating whether the current context uses the default task scheduler and no synchronization context.</summary>
    internal bool UsesDefaultSequencer => SynchronizationContext is null
                                          && Sequencer is null
                                          && (TaskScheduler is null || TaskScheduler == TaskScheduler.Default);

    /// <summary>Creates a new AsyncContext that uses the specified SynchronizationContext for asynchronous operations.</summary>
    /// <param name="synchronizationContext">The SynchronizationContext to associate with the AsyncContext. Cannot be null.</param>
    /// <returns>An AsyncContext instance configured to use the provided SynchronizationContext.</returns>
    /// <exception cref="ArgumentNullException">Thrown if synchronizationContext is null.</exception>
    public static AsyncContext From(SynchronizationContext synchronizationContext)
    {
        ArgumentExceptionHelper.ThrowIfNull(synchronizationContext);

        return new() { SynchronizationContext = synchronizationContext, TaskScheduler = null, Sequencer = null };
    }

    /// <summary>Creates a new AsyncContext that uses the specified TaskScheduler for task execution.</summary>
    /// <param name="taskScheduler">The TaskScheduler to associate with the new AsyncContext. Cannot be null.</param>
    /// <returns>An AsyncContext instance configured to use the specified TaskScheduler. The SynchronizationContext property of
    /// the returned instance is set to null.</returns>
    /// <exception cref="ArgumentNullException">Thrown if taskScheduler is null.</exception>
    public static AsyncContext From(TaskScheduler taskScheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(taskScheduler);

        return new() { SynchronizationContext = null, TaskScheduler = taskScheduler, Sequencer = null };
    }

    /// <summary>Creates a new AsyncContext using the specified sequencer for continuation scheduling.</summary>
    /// <param name="scheduler">The sequencer to use for configuring the AsyncContext.</param>
    /// <returns>An AsyncContext instance configured with the provided scheduler.</returns>
    /// <exception cref="ArgumentNullException">Thrown if scheduler is null.</exception>
    /// <remarks>If the provided sequencer directly implements <see cref="SynchronizationContext"/>, that instance is used
    /// directly. Otherwise, continuations are scheduled as direct <see cref="IWorkItem"/> instances on the sequencer.</remarks>
    public static AsyncContext From(ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        return scheduler is SynchronizationContext sc
            ? From(sc)
            : new() { SynchronizationContext = null, TaskScheduler = null, Sequencer = scheduler };
    }

    /// <summary>Gets the current asynchronous context associated with the calling thread.</summary>
    /// <returns>An <see cref="AsyncContext"/> representing the current asynchronous context. If a <see
    /// cref="SynchronizationContext"/> is present, it is used; otherwise, the current <see cref="TaskScheduler"/> is
    /// used.</returns>
    public static AsyncContext GetCurrent()
    {
        var currentSc = SynchronizationContext.Current;
        return currentSc is not null ? From(currentSc) : From(TaskScheduler.Current);
    }

    /// <summary>Creates an awaitable that switches execution to the associated asynchronous context.</summary>
    /// <param name="forceYielding">true to always yield execution to the context, even when the calling thread is on it;
    /// otherwise, false to continue inline when the context matches.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the context switch operation.</param>
    /// <returns>An awaitable that completes when execution has switched to the asynchronous context.</returns>
    public AsyncContextSwitcherAwaitable SwitchContextAsync(bool forceYielding, CancellationToken cancellationToken) =>
        new(this, forceYielding, cancellationToken);

    /// <summary>Provides an awaitable that switches execution to a specified asynchronous context, optionally forcing a yield and supporting cancellation.</summary>
    /// <param name="AsyncContext">The asynchronous context to which execution should be switched when awaited.</param>
    /// <param name="ForceYielding">true to always yield execution even when the calling thread is on the target context;
    /// otherwise, false to continue inline when the context matches.</param>
    /// <param name="CancellationToken">A cancellation token that can be used to cancel the await operation before the continuation is scheduled.</param>
    /// <remarks>Cancellation invokes the continuation immediately; GetResult then throws OperationCanceledException.</remarks>
    [System.Diagnostics.DebuggerDisplay("AsyncContextSwitcherAwaitable: IsCompleted = {IsCompleted}, ForceYielding = {ForceYielding}")]
    public readonly record struct AsyncContextSwitcherAwaitable(
        AsyncContext AsyncContext,
        bool ForceYielding,
        CancellationToken CancellationToken) : INotifyCompletion
    {
        /// <summary>Gets a value indicating whether the asynchronous operation has completed in the current context.</summary>
        public bool IsCompleted => !ForceYielding && AsyncContext.IsSameAsCurrentAsyncContext();

        /// <summary>Throws if cancellation was requested.</summary>
        /// <exception cref="OperationCanceledException">Cancellation was requested.</exception>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void GetResult() => CancellationToken.ThrowIfCancellationRequested();

        /// <summary>Returns this instance, which acts as its own awaiter.</summary>
        /// <returns>An awaiter that can be used to await this instance and perform an asynchronous context switch.</returns>
        public AsyncContextSwitcherAwaitable GetAwaiter() => this;

        /// <summary>Schedules the specified continuation action to be invoked when the operation has completed.</summary>
        /// <param name="continuation">The action to execute when the operation is complete. Cannot be null.</param>
        /// <remarks>Continuations use the synchronization context or task scheduler; cancellation invokes them immediately on the current thread.</remarks>
        public void OnCompleted(Action continuation)
        {
            ArgumentExceptionHelper.ThrowIfNull(continuation);

            if (CancellationToken.IsCancellationRequested)
            {
                continuation();
                return;
            }

            var sc = AsyncContext.SynchronizationContext;
            if (sc is not null)
            {
                sc.Post(static c => ((Action)c!).Invoke(), continuation);
                return;
            }

            var ts = AsyncContext.TaskScheduler;
            if (ts is not null && ts != TaskScheduler.Default)
            {
                _ = Task.Factory.StartNew(
                    continuation,
                    CancellationToken.None,
                    TaskCreationOptions.DenyChildAttach,
                    ts);
                return;
            }

            var sequencer = AsyncContext.Sequencer;
            if (sequencer is not null)
            {
                sequencer.Schedule(new ContinuationWorkItem(continuation));
                return;
            }

            // Queue directly to avoid allocating a Task for each continuation.
            if (ts is null || ts == TaskScheduler.Default)
            {
                _ = ThreadPool.UnsafeQueueUserWorkItem(static c => ((Action)c!).Invoke(), continuation);
            }
        }

        /// <summary>Work item used to schedule context-switch continuations directly on an <see cref="ISequencer"/>.</summary>
        /// <param name="continuation">The continuation to invoke.</param>
        private sealed class ContinuationWorkItem(Action continuation) : IWorkItem
        {
            /// <inheritdoc/>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Execute() => continuation();
        }
    }

    /// <summary>Routes task execution through the supplied sequencer.</summary>
    /// <param name="scheduler">The ISequencer used to schedule and execute tasks. Cannot be null.</param>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Performance",
        "CA1812:Avoid uninstantiated internal classes",
        Justification = "The adapter is constructed outside this assembly, not by the library itself.")]
    internal sealed class SequencerTaskScheduler(ISequencer scheduler) : TaskScheduler
    {
        /// <summary>Gets the sequencer used by this task-scheduler adapter.</summary>
        internal ISequencer Sequencer => scheduler;

        /// <summary>Returns the adapter's scheduled-task enumeration.</summary>
        /// <returns>The result of the protected <see cref="GetScheduledTasks"/> implementation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal IEnumerable<Task>? GetScheduledTasksForTesting() => GetScheduledTasks();

        /// <summary>Attempts inline execution through the adapter.</summary>
        /// <param name="task">The task to attempt to execute inline.</param>
        /// <param name="taskWasPreviouslyQueued">Whether the task has been queued to this scheduler before the call.</param>
        /// <returns>The result of the protected <see cref="TryExecuteTaskInline"/> implementation.</returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool TryExecuteTaskInlineForTesting(Task task, bool taskWasPreviouslyQueued) =>
            TryExecuteTaskInline(task, taskWasPreviouslyQueued);

        /// <inheritdoc/>
        protected override IEnumerable<Task>? GetScheduledTasks() => null;

        /// <inheritdoc/>
        protected override void QueueTask(Task task) =>
            scheduler.Schedule(
                (Self: this, task),
                static (sequencer, s) =>
                {
                    _ = sequencer;
                    _ = s.Self.TryExecuteTask(s.task);
                    return EmptyDisposable.Instance;
                });

        /// <inheritdoc/>
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;
    }
}
