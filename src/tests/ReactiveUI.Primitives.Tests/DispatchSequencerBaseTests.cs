// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for the shared UI dispatch sequencer base.</summary>
public sealed class DispatchSequencerBaseTests
{
    /// <summary>Expected post count after reentrant scheduling.</summary>
    private const int ExpectedReentrantPostCount = 2;

    /// <summary>Stateful schedule test value.</summary>
    private const int StatefulScheduleValue = 7;

    /// <summary>Expected burst values.</summary>
    private static readonly int[] ExpectedBurstValues = [1, 2, 3];

    /// <summary>Expected reentrant values.</summary>
    private static readonly int[] ExpectedReentrantValues = [1, 2];

    /// <summary>Verifies a burst posts one dispatcher drain and preserves FIFO order.</summary>
    [Test]
    public void DispatchSequencerBaseCoalescesBurstIntoOneDrain()
    {
        var sequencer = new TestDispatchSequencer();
        var values = new List<int>();

        sequencer.Schedule(new RecordingWorkItem(values, 1));
        sequencer.Schedule(new RecordingWorkItem(values, 2));
        sequencer.Schedule(new RecordingWorkItem(values, 3));

        Assert.Equal(1, sequencer.PostCount);
        sequencer.RunNextDrain();

        Assert.Equal(ExpectedBurstValues.AsEnumerable(), values);
        Assert.Equal(1, sequencer.PostCount);
    }

    /// <summary>Verifies cancelled queued work is skipped when the drain runs.</summary>
    [Test]
    public void DispatchSequencerBaseSkipsCancelledQueuedWork()
    {
        var sequencer = new TestDispatchSequencer();
        var values = new List<int>();
        var item = new RecordingWorkItem(values, 1);

        sequencer.Schedule(item);
        item.Dispose();
        sequencer.RunNextDrain();

        Assert.Equal(0, values.Count);
    }

    /// <summary>Verifies work scheduled from inside a drain runs in the next drain.</summary>
    [Test]
    public void DispatchSequencerBaseDefersReentrantWorkToNextDrain()
    {
        var sequencer = new TestDispatchSequencer();
        var values = new List<int>();

        sequencer.Schedule(new ReentrantWorkItem(sequencer, values));
        sequencer.RunNextDrain();

        Assert.Equal(ExpectedReentrantValues[..1].AsEnumerable(), values);
        Assert.Equal(ExpectedReentrantPostCount, sequencer.PostCount);

        sequencer.RunNextDrain();

        Assert.Equal(ExpectedReentrantValues.AsEnumerable(), values);
    }

    /// <summary>Verifies stateful schedule overloads pass state without requiring a captured closure.</summary>
    [Test]
    public void StatefulScheduleOverloadPassesState()
    {
        var values = new List<int>();

        Sequencer.Immediate.Schedule((values, value: StatefulScheduleValue), static state => state.values.Add(state.value));

        Assert.Equal(StatefulScheduleValue, values[0]);
    }

    /// <summary>Test dispatch sequencer that records posted drains.</summary>
    private sealed class TestDispatchSequencer : DispatchSequencerBase
    {
        /// <summary>Posted drains.</summary>
        private readonly Queue<Action> _drains = new();

        /// <summary>Gets the number of posted drains.</summary>
        public int PostCount { get; private set; }

        /// <summary>Runs the next posted drain.</summary>
        public void RunNextDrain() => _drains.Dequeue()();

        /// <inheritdoc/>
        protected override bool Post(Action drain)
        {
            PostCount++;
            _drains.Enqueue(drain);
            return true;
        }
    }

    /// <summary>Work item that records one value.</summary>
    private sealed class RecordingWorkItem : IWorkItem, IsDisposed
    {
        /// <summary>Recorded values.</summary>
        private readonly List<int> _values;

        /// <summary>Value to record.</summary>
        private readonly int _value;

        /// <summary>Initializes a new instance of the <see cref="RecordingWorkItem"/> class.</summary>
        /// <param name="values">Recorded values.</param>
        /// <param name="value">Value to record.</param>
        public RecordingWorkItem(List<int> values, int value)
        {
            _values = values;
            _value = value;
        }

        /// <inheritdoc/>
        public bool IsDisposed { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => IsDisposed = true;

        /// <inheritdoc/>
        public void Execute() => _values.Add(_value);
    }

    /// <summary>Work item that schedules more work from inside a drain.</summary>
    private sealed class ReentrantWorkItem : IWorkItem
    {
        /// <summary>Sequencer under test.</summary>
        private readonly ISequencer _sequencer;

        /// <summary>Recorded values.</summary>
        private readonly List<int> _values;

        /// <summary>Initializes a new instance of the <see cref="ReentrantWorkItem"/> class.</summary>
        /// <param name="sequencer">Sequencer under test.</param>
        /// <param name="values">Recorded values.</param>
        public ReentrantWorkItem(ISequencer sequencer, List<int> values)
        {
            _sequencer = sequencer;
            _values = values;
        }

        /// <inheritdoc/>
        public void Execute()
        {
            _values.Add(1);
            _sequencer.Schedule(new RecordingWorkItem(_values, 2));
        }
    }
}
