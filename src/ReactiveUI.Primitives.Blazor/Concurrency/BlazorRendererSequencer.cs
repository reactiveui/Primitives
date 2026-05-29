// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Threading;
using ReactiveUI.Primitives.Concurrency;

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

        var workItem = new SequencerWorkItem<BlazorRendererSequencer, TState>(this, state, action);
        _ = _invokeAsync(workItem.Invoke);
        return workItem;
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

        var normalized = Sequencer.Normalize(dueTime);
        if (normalized == TimeSpan.Zero)
        {
            return Schedule(state, action);
        }

        var workItem = new DelayedRendererWorkItem<TState>(this, state, action, normalized);
        _ = workItem.DelayThenDispatchAsync();
        return workItem;
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
    /// Disposable delayed renderer work item.
    /// </summary>
    /// <typeparam name="TState">The type of the state passed to the scheduled action.</typeparam>
    private sealed class DelayedRendererWorkItem<TState> : IDisposable
    {
        /// <summary>
        /// Sequencer passed to the scheduled action.
        /// </summary>
        private readonly BlazorRendererSequencer _sequencer;

        /// <summary>
        /// State passed to the scheduled action.
        /// </summary>
        private readonly TState _state;

        /// <summary>
        /// Action invoked when the scheduled item runs.
        /// </summary>
        private readonly Func<ISequencer, TState, IDisposable> _action;

        /// <summary>
        /// Relative time after which to dispatch the action.
        /// </summary>
        private readonly TimeSpan _dueTime;

        /// <summary>
        /// Cancellation source for the delayed work.
        /// </summary>
        private readonly CancellationTokenSource _cancellation = new();

        /// <summary>
        /// Cancellation token for the delayed work.
        /// </summary>
        private readonly CancellationToken _token;

        /// <summary>
        /// Tracks cancellation.
        /// </summary>
        private int _isDisposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="DelayedRendererWorkItem{TState}"/> class.
        /// </summary>
        /// <param name="sequencer">Sequencer passed to the action.</param>
        /// <param name="state">State passed to the action.</param>
        /// <param name="action">Action to invoke.</param>
        /// <param name="dueTime">Relative time after which to execute the action.</param>
        public DelayedRendererWorkItem(BlazorRendererSequencer sequencer, TState state, Func<ISequencer, TState, IDisposable> action, TimeSpan dueTime)
        {
            _sequencer = sequencer;
            _state = state;
            _action = action;
            _dueTime = dueTime;
            _token = _cancellation.Token;
        }

        /// <summary>
        /// Cancels the delayed work item.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _isDisposed, 1) != 0)
            {
                return;
            }

            _cancellation.Cancel();
            _cancellation.Dispose();
        }

        /// <summary>
        /// Delays work and then dispatches it through the renderer.
        /// </summary>
        /// <returns>A task representing the asynchronous delay and dispatch.</returns>
        public async Task DelayThenDispatchAsync()
        {
            try
            {
                await Task.Delay(_dueTime, _token).ConfigureAwait(false);
                if (Volatile.Read(ref _isDisposed) != 0)
                {
                    return;
                }

                await _sequencer._invokeAsync(Invoke).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_token.IsCancellationRequested)
            {
                // Cancellation is the expected disposal path for delayed renderer work.
            }
        }

        /// <summary>
        /// Invokes the scheduled action if it has not been cancelled.
        /// </summary>
        private void Invoke()
        {
            if (Volatile.Read(ref _isDisposed) != 0)
            {
                return;
            }

            _action(_sequencer, _state);
        }
    }
}
