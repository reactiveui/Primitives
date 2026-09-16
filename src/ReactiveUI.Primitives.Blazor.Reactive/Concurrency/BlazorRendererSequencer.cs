// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Concurrency;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using Microsoft.AspNetCore.Components;
using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Blazor.Reactive.Concurrency;

/// <summary>Scheduler that coalesces scheduled work through a Blazor renderer dispatcher delegate.</summary>
/// <remarks>Immediate and delayed callbacks run on the renderer dispatcher; renderer task failures reach UnhandledExceptionHandler.</remarks>
/// <seealso cref="System.Reactive.Concurrency.IScheduler" />
[System.Diagnostics.DebuggerDisplay("BlazorRendererSequencer: InvokeAsync = {_invokeAsync}, UnhandledExceptionHandler = {UnhandledExceptionHandler}")]
public sealed class BlazorRendererSequencer : LocalScheduler
{
    /// <summary>Delegate used to marshal work through Blazor's renderer.</summary>
    private readonly Func<Action, Task> _invokeAsync;

    /// <summary>Queues work and coalesces renderer drains.</summary>
    private CoalescingDispatchState _dispatch;

    /// <summary>Initializes a new instance of the <see cref="BlazorRendererSequencer"/> class.</summary>
    /// <param name="invokeAsync">A delegate such as <c>ComponentBase.InvokeAsync</c> that runs work through the renderer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="invokeAsync"/> is <see langword="null"/>.</exception>
    public BlazorRendererSequencer(Func<Action, Task> invokeAsync)
    {
        _invokeAsync = invokeAsync ?? throw new ArgumentNullException(nameof(invokeAsync));
        _dispatch = new(RunDrain, DefaultScheduler.Instance);
    }

    /// <summary>Initializes a new instance of the <see cref="BlazorRendererSequencer"/> class.</summary>
    /// <param name="dispatcher">The renderer dispatcher, for hosts that hold one (e.g. a <c>Renderer</c> or <c>HtmlRenderer</c>).</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcher"/> is <see langword="null"/>.</exception>
    public BlazorRendererSequencer(Dispatcher dispatcher)
        : this(dispatcher is null ? throw new ArgumentNullException(nameof(dispatcher)) : dispatcher.InvokeAsync)
    {
    }

    /// <summary>Gets or sets the renderer fault handler, rethrowing faults on the thread pool when null.</summary>
    public Action<Exception>? UnhandledExceptionHandler { get; set; }

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override IDisposable Schedule<TState>(TState state, Func<IScheduler, TState, IDisposable> action) =>
        _dispatch.Schedule(new DispatchHost(this), this, state, action);

    /// <inheritdoc/>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<IScheduler, TState, IDisposable> action) =>
        _dispatch.Schedule(new DispatchHost(this), this, state, dueTime, action);

    /// <summary>Registers fault observation unless the renderer task has already succeeded.</summary>
    /// <param name="task">The renderer task to observe.</param>
    /// <param name="register">Registers the task and its owning sequencer for fault notification.</param>
    internal void ObserveFaults(Task task, Action<Task, BlazorRendererSequencer> register)
    {
        if (task.IsCompletedSuccessfully)
        {
            return;
        }

        register(task, this);
    }

    /// <summary>Forwards a renderer task's base exception when the task faulted.</summary>
    /// <param name="task">The renderer task whose state changed.</param>
    internal void CompleteRendererTask(Task task)
    {
        if (!task.IsFaulted)
        {
            return;
        }

        HandleFault(task.Exception!.GetBaseException(), Rethrow);
    }

    /// <summary>Hands a fault to the handler, or rethrows it on the thread pool.</summary>
    /// <param name="exception">The observed fault.</param>
    /// <param name="rethrow">The fallback invoked when no fault handler is configured.</param>
    internal void HandleFault(Exception exception, Action<Exception> rethrow)
    {
        var handler = UnhandledExceptionHandler;
        if (handler is not null)
        {
            handler(exception);
            return;
        }

        rethrow(exception);
    }

    /// <summary>Registers notification for a renderer task that faults.</summary>
    /// <param name="task">The renderer task to observe.</param>
    /// <param name="sequencer">The owner receiving the fault notification.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private static void RegisterFaultContinuation(Task task, BlazorRendererSequencer sequencer) =>
        _ = task.ContinueWith(
            static (completed, state) => ((BlazorRendererSequencer)state!).CompleteRendererTask(completed),
            sequencer,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

    /// <summary>Rethrows a fault on the thread pool with its captured stack.</summary>
    /// <param name="exception">The fault to rethrow.</param>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private static void Rethrow(Exception exception) =>
        _ = ThreadPool.UnsafeQueueUserWorkItem(
            static state => ((ExceptionDispatchInfo)state!).Throw(),
            ExceptionDispatchInfo.Capture(exception));

    /// <summary>Posts the drain callback through the renderer and observes its faults.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns>Always <see langword="true"/>.</returns>
    private bool Post(Action drain)
    {
        ObserveFaults(_invokeAsync(drain), RegisterFaultContinuation);
        return true;
    }

    /// <summary>Runs one renderer batch.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RunDrain() => _dispatch.RunDrain(new DispatchHost(this));

    /// <summary>Reaches this scheduler's renderer for its dispatch state.</summary>
    /// <param name="Owner">The scheduler.</param>
    private readonly record struct DispatchHost(BlazorRendererSequencer Owner) : IDispatchHost
    {
        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool Post(Action drain) => Owner.Post(drain);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IDisposable ScheduleOnDispatcher(Action work, TimeSpan dueTime) =>
            Owner._dispatch.ScheduleThroughDelayScheduler(Owner, work, dueTime);
    }
}
