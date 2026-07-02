// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.ExceptionServices;
using Microsoft.AspNetCore.Components;
using ReactiveUI.Primitives.Reactive.Concurrency;

namespace ReactiveUI.Primitives.Blazor.Reactive.Concurrency;

/// <summary>Scheduler that coalesces scheduled work through a Blazor renderer dispatcher delegate.</summary>
/// <seealso cref="System.Reactive.Concurrency.IScheduler" />
public sealed class BlazorRendererSequencer : CoalescingDispatchScheduler
{
    /// <summary>Delegate used to marshal work through Blazor's renderer.</summary>
    private readonly Func<Action, Task> _invokeAsync;

    /// <summary>Initializes a new instance of the <see cref="BlazorRendererSequencer"/> class.</summary>
    /// <param name="invokeAsync">A delegate such as <c>ComponentBase.InvokeAsync</c> that runs work through the renderer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="invokeAsync"/> is <see langword="null"/>.</exception>
    public BlazorRendererSequencer(Func<Action, Task> invokeAsync) =>
        _invokeAsync = invokeAsync ?? throw new ArgumentNullException(nameof(invokeAsync));

    /// <summary>Initializes a new instance of the <see cref="BlazorRendererSequencer"/> class.</summary>
    /// <param name="dispatcher">The renderer dispatcher, for hosts that hold one (e.g. a <c>Renderer</c> or <c>HtmlRenderer</c>).</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcher"/> is <see langword="null"/>.</exception>
    public BlazorRendererSequencer(Dispatcher dispatcher)
        : this(dispatcher is null ? throw new ArgumentNullException(nameof(dispatcher)) : dispatcher.InvokeAsync)
    {
    }

    /// <summary>
    /// Gets or sets the handler for exceptions the renderer task surfaces after the drain is posted.
    /// When <see langword="null"/>, faults are rethrown on the thread pool instead of being lost as
    /// unobserved task exceptions.
    /// </summary>
    public Action<Exception>? UnhandledExceptionHandler { get; set; }

    /// <inheritdoc/>
    protected override bool Post(Action drain)
    {
        ObserveFaults(_invokeAsync(drain));
        return true;
    }

    /// <summary>Routes renderer-task faults to the handler instead of leaving them unobserved.</summary>
    /// <param name="task">The renderer task to observe.</param>
    private void ObserveFaults(Task task)
    {
        if (task.IsCompletedSuccessfully)
        {
            return;
        }

        _ = task.ContinueWith(
            static (t, state) => ((BlazorRendererSequencer)state!).HandleFault(t.Exception!.GetBaseException()),
            this,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);
    }

    /// <summary>Hands a fault to the handler, or rethrows it on the thread pool.</summary>
    /// <param name="exception">The observed fault.</param>
    private void HandleFault(Exception exception)
    {
        var handler = UnhandledExceptionHandler;
        if (handler is not null)
        {
            handler(exception);
            return;
        }

        _ = ThreadPool.UnsafeQueueUserWorkItem(
            static state => ((ExceptionDispatchInfo)state!).Throw(),
            ExceptionDispatchInfo.Capture(exception));
    }
}
