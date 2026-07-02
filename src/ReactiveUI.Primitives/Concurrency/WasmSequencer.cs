// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using Timer = System.Threading.Timer;

namespace ReactiveUI.Primitives.Concurrency;

/// <summary>
/// Task-pool replacement for single-threaded event-loop runtimes such as browser WebAssembly: it never starts
/// threads and never blocks. Immediate work is batched one drain per event-loop turn through a zero-due timer
/// (a <c>setTimeout(0)</c> macrotask on WebAssembly, so the browser can render between batches); delayed work
/// uses the shared timer, which the WebAssembly runtime backs with the JS event loop.
/// </summary>
/// <seealso cref="ISequencer" />
[System.Diagnostics.DebuggerDisplay("{DebuggerDisplay,nq}")]
public sealed class WasmSequencer : ISequencer, IDisposable
{
    /// <summary>One-shot timer used to yield a drain to the event loop.</summary>
    private readonly Timer _timer;

    /// <summary>Coalescing dispatch engine.</summary>
    private DispatchSequencerState _state;

    /// <summary>Initializes a new instance of the <see cref="WasmSequencer"/> class.</summary>
    private WasmSequencer()
    {
        _timer = new(static state => ((WasmSequencer)state!).RunDrain(), this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        _state = new(this, Post, RunDrain);
    }

    /// <summary>Gets the shared WebAssembly sequencer.</summary>
    public static WasmSequencer Default { get; } = new();

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

    /// <summary>Disposes the drain timer owned by this sequencer.</summary>
    public void Dispose() => _timer.Dispose();

    /// <summary>Arms the drain timer to fire on the next event-loop turn.</summary>
    /// <param name="drain">The drain callback (unused: the timer carries the cached drain).</param>
    /// <returns><see langword="true"/> when the timer accepted the change.</returns>
    private bool Post(Action drain) => _timer.Change(TimeSpan.Zero, Timeout.InfiniteTimeSpan);

    /// <summary>Forwards the cached drain callback to the engine.</summary>
    private void RunDrain() => _state.RunDrain();
}
