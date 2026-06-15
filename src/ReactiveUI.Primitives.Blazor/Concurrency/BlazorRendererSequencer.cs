// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Blazor.Concurrency;

/// <summary>Sequencer that coalesces scheduled work through a Blazor renderer dispatcher delegate.</summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class BlazorRendererSequencer : ISequencer
{
    /// <summary>Delegate used to marshal work through Blazor's renderer.</summary>
    private readonly Func<Action, Task> _invokeAsync;

    /// <summary>Coalescing dispatch engine.</summary>
    private DispatchSequencerState _state;

    /// <summary>Initializes a new instance of the <see cref="BlazorRendererSequencer"/> class.</summary>
    /// <param name="invokeAsync">A delegate such as <c>ComponentBase.InvokeAsync</c> that runs work through the renderer.</param>
    /// <exception cref="ArgumentNullException"><paramref name="invokeAsync"/> is <see langword="null"/>.</exception>
    public BlazorRendererSequencer(Func<Action, Task> invokeAsync)
    {
        _invokeAsync = invokeAsync ?? throw new ArgumentNullException(nameof(invokeAsync));
        _state = new(this, Post, RunDrain);
    }

    /// <inheritdoc/>
    public DateTimeOffset Now => DispatchSequencerState.Now;

    /// <inheritdoc/>
    public long Timestamp => DispatchSequencerState.Timestamp;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    public void Schedule(IWorkItem item) => _state.Schedule(item);

    /// <inheritdoc/>
    public void Schedule(IWorkItem item, long dueTimestamp) => _state.Schedule(item, dueTimestamp);

    /// <summary>Marshals the cached drain callback through the Blazor renderer.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns><see langword="true"/>, since the renderer always accepts the work.</returns>
    private bool Post(Action drain)
    {
        _ = _invokeAsync(drain);
        return true;
    }

    /// <summary>Forwards the cached drain callback to the engine.</summary>
    private void RunDrain() => _state.RunDrain();
}
