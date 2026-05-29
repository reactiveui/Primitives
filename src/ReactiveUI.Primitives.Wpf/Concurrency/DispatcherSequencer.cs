// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System;
using System.Windows.Threading;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// WPF dispatcher sequencer that coalesces scheduled work onto a dispatcher drain.
/// </summary>
/// <seealso cref="ReactiveUI.Primitives.Concurrency.DispatchSequencerBase" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public class DispatcherSequencer : DispatchSequencerBase
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DispatcherSequencer"/> class.
    /// </summary>
    /// <param name="dispatcher">The dispatcher.</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcher"/> is <see langword="null"/>.</exception>
    public DispatcherSequencer(Dispatcher dispatcher)
        : this(dispatcher, DispatcherPriority.Normal)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DispatcherSequencer"/> class.
    /// </summary>
    /// <param name="dispatcher">The dispatcher.</param>
    /// <param name="priority">Dispatcher priority used for posted drains.</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcher"/> is <see langword="null"/>.</exception>
    public DispatcherSequencer(Dispatcher dispatcher, DispatcherPriority priority)
    {
        Dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));
        Priority = priority;
    }

    /// <summary>
    /// Gets the dispatcher.
    /// </summary>
    public Dispatcher Dispatcher { get; }

    /// <summary>
    /// Gets the dispatcher priority used for posted drains.
    /// </summary>
    public DispatcherPriority Priority { get; }

    /// <summary>
    /// Gets the debugger display text.
    /// </summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    protected override bool Post(Action drain)
    {
        Dispatcher.BeginInvoke(drain, Priority);
        return true;
    }
}
