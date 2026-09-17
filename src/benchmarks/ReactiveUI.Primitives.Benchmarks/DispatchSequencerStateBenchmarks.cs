// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Benchmarks;

/// <summary>Measures a dispatcher sequencer built on the coalescing dispatch state, pumped by a manual message loop.</summary>
[MemoryDiagnoser]
public class DispatchSequencerStateBenchmarks
{
    /// <summary>The number of work items scheduled per case.</summary>
    private const int Count = 1000;

    /// <summary>Benchmarks coalescing a burst of ready work into a single posted drain.</summary>
    /// <returns>The number of executed items plus posted drains.</returns>
    [Benchmark(Baseline = true)]
    public int PrimitivesDispatchBurstDrain()
    {
        StrongBox<int> executed = new();
        var sequencer = ManualDispatchSequencer.Create();
        for (var i = 0; i < Count; i++)
        {
            _ = sequencer.Schedule(executed, static box => box.Value++);
        }

        return executed.Value + sequencer.Pump();
    }

    /// <summary>Benchmarks ready work where half is cancelled before the drain and past-due work joins the batch.</summary>
    /// <returns>The number of executed items plus posted drains.</returns>
    [Benchmark]
    public int PrimitivesDispatchCancelledAndDueDrain()
    {
        StrongBox<int> executed = new();
        var sequencer = ManualDispatchSequencer.Create();
        for (var i = 0; i < Count; i++)
        {
            var handle = sequencer.Schedule(executed, sequencer.Timestamp, static box => box.Value++);
            if ((i & 1) == 0)
            {
                handle.Dispose();
            }
        }

        return executed.Value + sequencer.Pump() + (DispatchSequencerState.DelayUntil(sequencer.Timestamp) <= TimeSpan.Zero ? 1 : 0);
    }

    /// <summary>Benchmarks running work items directly on the dispatcher thread, skipping cancelled ones.</summary>
    /// <returns>The number of executed items.</returns>
    [Benchmark]
    public int PrimitivesDispatchRunIfActive()
    {
        StrongBox<int> executed = new();
        var sequencer = ManualDispatchSequencer.Create();
        for (var i = 0; i < Count; i++)
        {
            RunOnceWorkItem item = new(executed, (i & 1) == 0);
            DispatchSequencerState.RunIfActive(item);
        }

        return executed.Value + sequencer.Pump();
    }

    /// <summary>A dispatcher sequencer whose drain requests are pumped by the caller.</summary>
    private sealed class ManualDispatchSequencer : ISequencer
    {
        /// <summary>The coalescing dispatch state.</summary>
        private DispatchSequencerState _state;

        /// <summary>The drain posted to the message loop and not yet run.</summary>
        private Action? _pendingDrain;

        /// <inheritdoc/>
        public DateTimeOffset Now => DispatchSequencerState.Now;

        /// <inheritdoc/>
        public long Timestamp => DispatchSequencerState.Timestamp;

        /// <summary>Creates a sequencer wired to its dispatch state.</summary>
        /// <returns>The sequencer.</returns>
        public static ManualDispatchSequencer Create()
        {
            ManualDispatchSequencer sequencer = new();
            sequencer._state = new(sequencer, sequencer.Post, sequencer.RunDrain);
            return sequencer;
        }

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Schedule(IWorkItem item) => _state.Schedule(item);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Schedule(IWorkItem item, long dueTimestamp) => _state.Schedule(item, dueTimestamp);

        /// <summary>Runs posted drains until none remain.</summary>
        /// <returns>The number of drains run.</returns>
        public int Pump()
        {
            var drains = 0;
            while (_pendingDrain is { } drain)
            {
                _pendingDrain = null;
                drain();
                drains++;
            }

            _state.PostDrain();
            return drains;
        }

        /// <summary>Posts a drain to the message loop.</summary>
        /// <param name="drain">The drain callback.</param>
        /// <returns>Always <see langword="true"/>.</returns>
        private bool Post(Action drain)
        {
            _pendingDrain = drain;
            return true;
        }

        /// <summary>Runs one dispatcher batch.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void RunDrain() => _state.RunDrain();
    }

    /// <summary>A work item that increments a counter and may report itself cancelled.</summary>
    /// <param name="counter">The counter to increment.</param>
    /// <param name="cancelled">Whether the item reports itself cancelled.</param>
    private sealed class RunOnceWorkItem(StrongBox<int> counter, bool cancelled) : IWorkItem, Disposables.IsDisposed
    {
        /// <inheritdoc/>
        public bool IsDisposed => cancelled;

        /// <inheritdoc/>
        public void Execute() => counter.Value++;

        /// <inheritdoc/>
        public void Dispose()
        {
        }
    }
}
