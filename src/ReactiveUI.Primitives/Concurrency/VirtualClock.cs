// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>Deterministic virtual scheduler backed by <see cref="DateTimeOffset"/> and <see cref="TimeSpan"/>.</summary>
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public class VirtualClock : VirtualTimeSequencer<DateTimeOffset, TimeSpan>
{
    /// <summary>Initializes a new instance of the <see cref="VirtualClock"/> class at the default clock value.</summary>
    public VirtualClock()
        : this(default)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="VirtualClock"/> class.</summary>
    /// <param name="initialClock">Initial virtual time.</param>
    public VirtualClock(DateTimeOffset initialClock)
        : base(initialClock, Comparer<DateTimeOffset>.Default)
    {
    }

    /// <summary>Gets the debugger display text.</summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    protected override DateTimeOffset Add(DateTimeOffset absolute, TimeSpan relative) =>
        absolute + Sequencer.Normalize(relative);

    /// <inheritdoc/>
    protected override DateTimeOffset ToDateTimeOffset(DateTimeOffset absolute) => absolute;

    /// <inheritdoc/>
    protected override TimeSpan ToRelative(TimeSpan timeSpan) => Sequencer.Normalize(timeSpan);
}
