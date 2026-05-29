// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Maui.Dispatching;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// MAUI dispatcher sequencer that coalesces scheduled work through an <see cref="IDispatcher"/>.
/// </summary>
/// <seealso cref="DispatchSequencerBase" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class MauiDispatcherSequencer : DispatchSequencerBase
{
    /// <summary>
    /// Initializes a new instance of the <see cref="MauiDispatcherSequencer"/> class.
    /// </summary>
    /// <param name="dispatcher">The dispatcher used to marshal work to the UI thread.</param>
    /// <exception cref="ArgumentNullException"><paramref name="dispatcher"/> is <see langword="null"/>.</exception>
    public MauiDispatcherSequencer(IDispatcher dispatcher) =>
        Dispatcher = dispatcher ?? throw new ArgumentNullException(nameof(dispatcher));

    /// <summary>
    /// Gets the dispatcher used to marshal work to the UI thread.
    /// </summary>
    public IDispatcher Dispatcher { get; }

    /// <summary>
    /// Gets the debugger display text.
    /// </summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    protected override bool Post(Action drain) => Dispatcher.Dispatch(drain);
}
