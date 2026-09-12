// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Drives a thread-pool sequencer through explicit queue and clock transitions.</summary>
internal sealed class ManualThreadPool : IDisposable
{
    /// <summary>Pending immediate callbacks.</summary>
    private readonly Queue<Action> _ready = new();

    /// <summary>Initializes a new instance of the <see cref="ManualThreadPool"/> class.</summary>
    internal ManualThreadPool() =>
        Sequencer = new(() => Timestamp, (callback, state) => _ready.Enqueue(() => callback(state)), Delays.Add);

    /// <summary>Gets the sequencer under test.</summary>
    internal ThreadPoolSequencer Sequencer { get; }

    /// <summary>Gets or sets the current monotonic clock reading.</summary>
    internal long Timestamp { get; set; }

    /// <summary>Gets the timer changes requested by the sequencer.</summary>
    internal List<TimeSpan> Delays { get; } = [];

    /// <inheritdoc/>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Dispose() => Sequencer.Dispose();

    /// <summary>Executes all immediate callbacks.</summary>
    internal void RunReady()
    {
        while (_ready.TryDequeue(out var callback))
        {
            callback();
        }
    }

    /// <summary>Runs the timer callback at an explicit clock reading.</summary>
    /// <param name="timestamp">The clock reading for the callback.</param>
    internal void RunDue(long timestamp)
    {
        Timestamp = timestamp;
        Sequencer.RunDue();
    }
}
