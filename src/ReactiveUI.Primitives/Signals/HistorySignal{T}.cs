// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Signals;

/// <summary>Replay-capable signal that stores a bounded or time-windowed history for later subscribers.</summary>
/// <typeparam name="T">The value type.</typeparam>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public class HistorySignal<T> : ReplaySignal<T>
{
    /// <summary>Initializes a new instance of the <see cref="HistorySignal{T}"/> class.</summary>
    public HistorySignal()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="HistorySignal{T}"/> class.</summary>
    /// <param name="scheduler">The sequencer used for time-window trimming.</param>
    public HistorySignal(ISequencer scheduler)
        : base(scheduler)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="HistorySignal{T}"/> class.</summary>
    /// <param name="bufferSize">Maximum number of values to replay.</param>
    public HistorySignal(int bufferSize)
        : base(bufferSize)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="HistorySignal{T}"/> class.</summary>
    /// <param name="bufferSize">Maximum number of values to replay.</param>
    /// <param name="scheduler">The sequencer used for time-window trimming.</param>
    public HistorySignal(int bufferSize, ISequencer scheduler)
        : base(bufferSize, scheduler)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="HistorySignal{T}"/> class.</summary>
    /// <param name="window">Maximum replay window.</param>
    public HistorySignal(TimeSpan window)
        : base(window)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="HistorySignal{T}"/> class.</summary>
    /// <param name="window">Maximum replay window.</param>
    /// <param name="scheduler">The sequencer used for time-window trimming.</param>
    public HistorySignal(TimeSpan window, ISequencer scheduler)
        : base(window, scheduler)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="HistorySignal{T}"/> class.</summary>
    /// <param name="bufferSize">Maximum number of values to replay.</param>
    /// <param name="window">Maximum replay window.</param>
    public HistorySignal(int bufferSize, TimeSpan window)
        : base(bufferSize, window)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="HistorySignal{T}"/> class.</summary>
    /// <param name="bufferSize">Maximum number of values to replay.</param>
    /// <param name="window">Maximum replay window.</param>
    /// <param name="scheduler">The sequencer used for time-window trimming.</param>
    public HistorySignal(int bufferSize, TimeSpan window, ISequencer scheduler)
        : base(bufferSize, window, scheduler)
    {
    }

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;
}
