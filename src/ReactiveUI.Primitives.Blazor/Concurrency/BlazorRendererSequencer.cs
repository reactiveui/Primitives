// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Blazor.Concurrency;

/// <summary>
/// Sequencer that coalesces scheduled work through a Blazor renderer dispatcher delegate.
/// </summary>
/// <seealso cref="DispatchSequencerBase" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class BlazorRendererSequencer : DispatchSequencerBase
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
    /// Gets the debugger display text.
    /// </summary>
    [System.Diagnostics.DebuggerBrowsable(System.Diagnostics.DebuggerBrowsableState.Never)]
    private string DebuggerDisplay => ToString() ?? string.Empty;

    /// <inheritdoc/>
    protected override bool Post(Action drain)
    {
        _ = _invokeAsync(drain);
        return true;
    }
}
