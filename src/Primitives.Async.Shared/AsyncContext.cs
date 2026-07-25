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

/// <summary>
/// Represents an asynchronous execution context that encapsulates a specific SynchronizationContext or TaskScheduler
/// for controlling the scheduling of asynchronous operations.
/// </summary>
/// <remarks>Use AsyncContext to capture and restore a particular synchronization or task scheduling environment
/// when running asynchronous code. This is useful for ensuring that continuations or asynchronous callbacks execute on
/// a desired context, such as a UI thread or a custom scheduler. An AsyncContext can be created from a
/// SynchronizationContext, TaskScheduler, or ISequencer. The Default context represents the absence of a specific
/// synchronization or scheduling context, and typically corresponds to the default task scheduler.</remarks>
[System.Diagnostics.DebuggerDisplay(
    "SynchronizationContext = {SynchronizationContext}, TaskScheduler = {TaskScheduler}, Sequencer = {Sequencer}")]
public sealed record AsyncContext
{
    /// <summary>Initializes a new instance of the <see cref="AsyncContext"/> class.</summary>
    private AsyncContext()
    {
    }

    /// <summary>Gets the default instance of the AsyncContext class.</summary>
    /// <remarks>Use this property to access a shared, default AsyncContext instance when a custom context is
    /// not required.</remarks>
    public static AsyncContext Default { get; } = new();

    /// <summary>Gets the synchronization context to use for marshaling callbacks and continuations.</summary>
    /// <remarks>If this property is set, callbacks and continuations will be posted to the specified
    /// synchronization context. If null, the default context is used, which may result in execution on a thread pool
    /// thread. This property is typically used to ensure that asynchronous operations resume on a specific thread or
    /// context, such as a UI thread.</remarks>
    public SynchronizationContext? SynchronizationContext { get; init; }

    /// <summary>Gets the task scheduler to use for scheduling tasks, or null to use the default scheduler.</summary>
    public TaskScheduler? TaskScheduler { get; init; }

    /// <summary>Gets the sequencer used to schedule continuations, or <see langword="null"/> when another context shape is used.</summary>
    public ISequencer? Sequencer { get; init; }

    /// <summary>Gets a value indicating whether the current context uses the default task scheduler and no synchronization context.</summary>
    internal bool UsesDefaultSequencer => SynchronizationContext is null &&
                                          Sequencer is null &&
                                          (TaskScheduler is null || TaskScheduler == TaskScheduler.Default);

    /// <summary>Creates a new AsyncContext that uses the specified SynchronizationContext for asynchronous operations.</summary>
    /// <remarks>The returned AsyncContext will have its TaskScheduler property set to null. Use this method
    /// when you want to control asynchronous execution using a specific SynchronizationContext, such as for UI thread
    /// synchronization.</remarks>
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
    /// <remarks>If the provided sequencer directly implements <see cref="SynchronizationContext"/>, that instance is used
    /// directly. Otherwise, continuations are scheduled as direct <see cref="IWorkItem"/> instances on the sequencer.</remarks>
    /// <param name="scheduler">The sequencer to use for configuring the AsyncContext.</param>
    /// <returns>An AsyncContext instance configured with the provided scheduler.</returns>
    /// <exception cref="ArgumentNullException">Thrown if scheduler is null.</exception>
    public static AsyncContext From(ISequencer scheduler)
    {
        ArgumentExceptionHelper.ThrowIfNull(scheduler);

        return scheduler is SynchronizationContext sc
            ? From(sc)
            : new() { SynchronizationContext = null, TaskScheduler = null, Sequencer = scheduler };
    }

    /// <summary>Gets the current asynchronous context associated with the calling thread.</summary>
    /// <remarks>Use this method to capture the context for scheduling asynchronous operations that should
    /// continue on the same logical thread or synchronization context. This is commonly used to ensure code executes on
    /// the appropriate context, such as a UI thread in desktop applications.</remarks>
    /// <returns>An <see cref="AsyncContext"/> representing the current asynchronous context. If a <see
    /// cref="SynchronizationContext"/> is present, it is used; otherwise, the current <see cref="TaskScheduler"/> is
    /// used.</returns>
    public static AsyncContext GetCurrent()
    {
        var currentSc = SynchronizationContext.Current;
        return currentSc is not null ? From(currentSc) : From(TaskScheduler.Current);
    }

    /// <summary>Creates an awaitable that switches execution to the associated asynchronous context.</summary>
    /// <param name="forceYielding">true to always yield execution to the context, even if already in the correct context; otherwise, false to avoid
    /// yielding if already in the context.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the context switch operation.</param>
    /// <returns>An awaitable that completes when execution has switched to the asynchronous context.</returns>
    public AsyncContextSwitcherAwaitable SwitchContextAsync(bool forceYielding, CancellationToken cancellationToken) =>
        new(this, forceYielding, cancellationToken);

    /// <summary>
    /// Provides an awaitable that switches execution to a specified asynchronous context, optionally forcing a yield
    /// and supporting cancellation.
    /// </summary>
    /// <remarks>Use this struct to ensure that code after an await resumes on a specific asynchronous
    /// context, such as a particular SynchronizationContext or TaskScheduler. If cancellation is requested before the
    /// continuation is scheduled, the continuation is invoked immediately and an OperationCanceledException will be
    /// thrown when GetResult is called. This type is intended for advanced scenarios where precise control over
    /// asynchronous context switching is required.</remarks>
    /// <param name="AsyncContext">The asynchronous context to which execution should be switched when awaited.</param>
    /// <param name="ForceYielding">true to always yield execution even if already in the target context; otherwise, false to avoid yielding if
    /// already in the specified context.</param>
    /// <param name="CancellationToken">A cancellation token that can be used to cancel the await operation before the continuation is scheduled.</param>
    [System.Diagnostics.DebuggerDisplay("IsCompleted = {IsCompleted}, ForceYielding = {ForceYielding}")]
    public readonly record struct AsyncContextSwitcherAwaitable(
        AsyncContext AsyncContext,
        bool ForceYielding,
        CancellationToken CancellationToken) : INotifyCompletion
    {
        /// <summary>Gets a value indicating whether the asynchronous operation has completed in the current context.</summary>
        public bool IsCompleted => !ForceYielding && AsyncContext.IsSameAsCurrentAsyncContext();

        /// <summary>Checks whether the associated cancellation token has had cancellation requested and throws an exception if so.</summary>
        /// <remarks>This method is typically used to observe cancellation requests and respond by
        /// throwing an OperationCanceledException if cancellation has been signaled. If cancellation has not been
        /// requested, the method returns normally.</remarks>
        public void GetResult() => CancellationToken.ThrowIfCancellationRequested();

        /// <summary>
        /// Returns an awaiter for this AsyncContextSwitcherAwaitable instance, enabling use of the await keyword to
        /// asynchronously switch execution context.
        /// </summary>
        /// <returns>An awaiter that can be used to await this instance and perform an asynchronous context switch.</returns>
        public AsyncContextSwitcherAwaitable GetAwaiter() => this;

        /// <summary>Schedules the specified continuation action to be invoked when the operation has completed.</summary>
        /// <remarks>If a synchronization context is available, the continuation is posted to it;
        /// otherwise, the continuation is scheduled on the associated task scheduler or the default task scheduler. If
        /// the operation has already been canceled, the continuation is invoked immediately on the current
        /// thread.</remarks>
        /// <param name="continuation">The action to execute when the operation is complete. Cannot be null.</param>
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
                sc.Post(
                    static state => ((Action)(
                        state ?? throw new InvalidOperationException("The continuation is missing."))).Invoke(),
                    continuation);
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

            // Fast path for the default scheduler: bypass Task.Factory.StartNew (which allocates a
            // Task per call) and queue the continuation directly to the threadpool. This is the
            // path Yield takes by default, so the saving lands on the operator's hot path.
            if (ts is null || ts == TaskScheduler.Default)
            {
                _ = ThreadPool.UnsafeQueueUserWorkItem(
                    static state => ((Action)(
                        state ?? throw new InvalidOperationException("The continuation is missing."))).Invoke(),
                    continuation);
            }
        }

        /// <summary>Work item used to schedule context-switch continuations directly on an <see cref="ISequencer"/>.</summary>
        /// <param name="continuation">The continuation to invoke.</param>
        private sealed class ContinuationWorkItem(Action continuation) : IWorkItem
        {
            /// <inheritdoc/>
            public void Execute() => continuation();
        }
    }

    /// <summary>Provides a custom TaskScheduler that schedules tasks using the specified IScheduler.</summary>
    /// <remarks>This TaskScheduler enables integration of Task-based asynchronous code with reactive or
    /// custom scheduling strategies by delegating task execution to the provided ISequencer. Tasks scheduled through
    /// this TaskScheduler will be executed according to the policies of the specified ISequencer. This class is
    /// intended for advanced scenarios where control over task scheduling is required.</remarks>
    /// <param name="scheduler">The ISequencer used to schedule and execute tasks. Cannot be null.</param>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Performance",
        "CA1812:Avoid uninstantiated internal classes",
        Justification =
            "Kept as an internal adapter for generator and test smoke scenarios that need TaskScheduler-shaped sequencer execution.")]
    internal sealed class SequencerTaskScheduler(ISequencer scheduler) : TaskScheduler
    {
        /// <summary>Gets the sequencer used by this task-scheduler adapter.</summary>
        internal ISequencer Sequencer => scheduler;

        /// <summary>Internal accessor for the protected <see cref="GetScheduledTasks"/> override; used only by the test assembly.</summary>
        /// <returns>The result of the protected <see cref="GetScheduledTasks"/> implementation.</returns>
        internal IEnumerable<Task>? GetScheduledTasksForTesting() => GetScheduledTasks();

        /// <summary>Internal accessor for the protected <see cref="TryExecuteTaskInline"/> override; used only by the test assembly.</summary>
        /// <param name="task">The task to attempt to execute inline.</param>
        /// <param name="taskWasPreviouslyQueued">Whether the task was previously queued.</param>
        /// <returns>The result of the protected <see cref="TryExecuteTaskInline"/> implementation.</returns>
        internal bool TryExecuteTaskInlineForTesting(Task task, bool taskWasPreviouslyQueued) =>
            TryExecuteTaskInline(task, taskWasPreviouslyQueued);

        /// <inheritdoc/>
        protected override IEnumerable<Task>? GetScheduledTasks() => null;

        /// <inheritdoc/>
        protected override void QueueTask(Task task) =>
            scheduler.Schedule(
                (self: this, task),
                static (sequencer, s) =>
                {
                    _ = sequencer;
                    _ = s.self.TryExecuteTask(s.task);
                    return EmptyDisposable.Instance;
                });

        /// <inheritdoc/>
        protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued) => false;
    }
}
