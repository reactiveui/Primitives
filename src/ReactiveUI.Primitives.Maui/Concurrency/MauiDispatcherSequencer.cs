// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Maui.Dispatching;
using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>MAUI dispatcher sequencer that coalesces scheduled work through an <see cref="IDispatcher"/>.</summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class MauiDispatcherSequencer : ISequencer
{
    /// <summary>Coalescing dispatch engine.</summary>
    private DispatchSequencerState _state;

    /// <summary>Initializes a new instance of the <see cref="MauiDispatcherSequencer"/> class.</summary>
    /// <param name="dispatcher">The dispatcher used to marshal work to the UI thread.</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcher"/> is <see langword="null"/>.</exception>
    public MauiDispatcherSequencer(IDispatcher dispatcher)
    {
        Dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        _state = new(this, Post, RunDrain, ScheduleDelayed);
    }

    /// <summary>Gets the dispatcher used to marshal work to the UI thread.</summary>
    public IDispatcher Dispatcher { get; }

    /// <inheritdoc/>
    public DateTimeOffset Now => DispatchSequencerState.Now;

    /// <inheritdoc/>
    public long Timestamp => DispatchSequencerState.Timestamp;

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    public void Schedule(IWorkItem item) => _state.Schedule(item);

    /// <inheritdoc/>
    public void Schedule(IWorkItem item, long dueTimestamp) => _state.Schedule(item, dueTimestamp);

    /// <summary>Marshals the cached drain callback through the dispatcher.</summary>
    /// <param name="drain">The drain callback.</param>
    /// <returns><see langword="true"/> when the dispatcher accepted the work.</returns>
    private bool Post(Action drain) => Dispatcher.Dispatch(drain);

    /// <summary>Runs delayed work through the dispatcher's native delayed dispatch.</summary>
    /// <param name="item">Work item to execute at the due time.</param>
    /// <param name="dueTimestamp">Absolute monotonic timestamp at which to execute the item.</param>
    private void ScheduleDelayed(IWorkItem item, long dueTimestamp) =>
        _ = Dispatcher.DispatchDelayed(
            DispatchSequencerState.DelayUntil(dueTimestamp),
            () => DispatchSequencerState.RunIfActive(item));

    /// <summary>Forwards the cached drain callback to the engine.</summary>
    private void RunDrain() => _state.RunDrain();
}
