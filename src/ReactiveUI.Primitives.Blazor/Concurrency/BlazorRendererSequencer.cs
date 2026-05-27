// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Blazor.Concurrency;

/// <summary>
/// Sequencer that schedules work through a Blazor renderer dispatcher delegate.
/// </summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class BlazorRendererSequencer : ISequencer
{
    /// <summary>
    /// Delegate used to marshal work through Blazor's renderer.
    /// </summary>
    private readonly Func<Action, Task> _invokeAsync;

    /// <summary>
    /// Initializes a new instance of the <see cref="BlazorRendererSequencer"/> class.
    /// </summary>
    /// <param name="invokeAsync">A delegate such as <c>ComponentBase.InvokeAsync</c> that runs work through the renderer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="invokeAsync"/> is <see langword="null"/>.</exception>
    public BlazorRendererSequencer(Func<Action, Task> invokeAsync) =>
        _invokeAsync = invokeAsync ?? throw new ArgumentNullException(nameof(invokeAsync));

    /// <summary>
    /// Gets the scheduler's notion of current time.
    /// </summary>
    public DateTimeOffset Now => TimeProvider.System.GetUtcNow();

    /// <summary>
    /// Gets the debugger display text.
    /// </summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <summary>
    /// Schedules an action to be executed.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action on a best-effort basis.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    public IDisposable Schedule<TState>(TState state, Func<ISequencer, TState, IDisposable> action)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        var cancelable = new BooleanDisposable();
        _ = _invokeAsync(() =>
        {
            if (cancelable.IsDisposed)
            {
                return;
            }

            action(this, state);
        });
        return cancelable;
    }

    /// <summary>
    /// Schedules an action to be executed after dueTime.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Relative time after which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action on a best-effort basis.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="action"/> is <see langword="null"/>.</exception>
    public IDisposable Schedule<TState>(TState state, TimeSpan dueTime, Func<ISequencer, TState, IDisposable> action)
    {
        if (action == null)
        {
            throw new ArgumentNullException(nameof(action));
        }

        var cancellation = new CancellationDisposable();
        _ = DelayThenDispatchAsync(state, Sequencer.Normalize(dueTime), action, cancellation.Token);
        return cancellation;
    }

    /// <summary>
    /// Schedules an action to be executed at dueTime.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">Absolute time at which to execute the action.</param>
    /// <param name="action">Action to be executed.</param>
    /// <returns>The disposable object used to cancel the scheduled action on a best-effort basis.</returns>
    public IDisposable Schedule<TState>(TState state, DateTimeOffset dueTime, Func<ISequencer, TState, IDisposable> action) =>
        Schedule(state, Sequencer.Normalize(dueTime - Now), action);

    /// <summary>
    /// Delays work and then dispatches it through the renderer.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    /// <param name="state">State passed to the action to be executed.</param>
    /// <param name="dueTime">The normalized due time.</param>
    /// <param name="action">Action to be executed.</param>
    /// <param name="cancellationToken">Token used to cancel delayed work.</param>
    /// <returns>A task representing the asynchronous delay and dispatch.</returns>
    private async Task DelayThenDispatchAsync<TState>(
        TState state,
        TimeSpan dueTime,
        Func<ISequencer, TState, IDisposable> action,
        CancellationToken cancellationToken)
    {
        try
        {
            await Task.Delay(dueTime, cancellationToken).ConfigureAwait(false);
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            await _invokeAsync(() =>
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }

                action(this, state);
            }).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Cancellation is the expected disposal path for delayed renderer work.
        }
    }
}
