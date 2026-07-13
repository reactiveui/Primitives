// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
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

    /// <summary>The value carried by the second work item of a scheduled burst.</summary>
    private const int SecondBurstValue = 2;

    /// <summary>The value carried by the third work item of a scheduled burst.</summary>
    private const int ThirdBurstValue = 3;

    /// <summary>Expected burst values.</summary>
    private static readonly int[] ExpectedBurstValues = [1, 2, 3];

    /// <summary>Expected reentrant values.</summary>
    private static readonly int[] ExpectedReentrantValues = [1, 2];

    /// <summary>Verifies a burst posts one dispatcher drain and preserves FIFO order.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DispatchSequencerBaseCoalescesBurstIntoOneDrain()
    {
        var sequencer = TestDispatchSequencer.Create();
        List<int> values = [];
        sequencer.Schedule(new RecordingWorkItem(values, 1));
        sequencer.Schedule(new RecordingWorkItem(values, SecondBurstValue));
        sequencer.Schedule(new RecordingWorkItem(values, ThirdBurstValue));
        await Assert.That(sequencer.PostCount).IsEqualTo(1);
        sequencer.RunNextDrain();
        await Assert.That(values.SequenceEqual(ExpectedBurstValues)).IsTrue();
        await Assert.That(sequencer.PostCount).IsEqualTo(1);
    }

    /// <summary>Verifies cancelled queued work is skipped when the drain runs.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DispatchSequencerBaseSkipsCancelledQueuedWork()
    {
        var sequencer = TestDispatchSequencer.Create();
        List<int> values = [];
        RecordingWorkItem item = new(values, 1);
        sequencer.Schedule(item);
        item.Dispose();
        sequencer.RunNextDrain();
        await Assert.That(values.Count).IsEqualTo(0);
    }

    /// <summary>Verifies work scheduled from inside a drain runs in the next drain.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DispatchSequencerBaseDefersReentrantWorkToNextDrain()
    {
        var sequencer = TestDispatchSequencer.Create();
        List<int> values = [];
        sequencer.Schedule(new ReentrantWorkItem(sequencer, values));
        sequencer.RunNextDrain();
        await Assert.That(values.SequenceEqual(ExpectedReentrantValues[..1])).IsTrue();
        await Assert.That(sequencer.PostCount).IsEqualTo(ExpectedReentrantPostCount);
        sequencer.RunNextDrain();
        await Assert.That(values.SequenceEqual(ExpectedReentrantValues)).IsTrue();
    }

    /// <summary>Verifies stateful schedule overloads pass state without requiring a captured closure.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task StatefulScheduleOverloadPassesState()
    {
        List<int> values = [];
        _ = Sequencer.Immediate.Schedule(
            (values, value: StatefulScheduleValue),
            static state => state.values.Add(state.value));
        await Assert.That(values[0]).IsEqualTo(StatefulScheduleValue);
    }

    /// <summary>Test dispatch sequencer that records posted drains.</summary>
    private sealed class TestDispatchSequencer : ISequencer
    {
        /// <summary>Posted drains.</summary>
        private readonly Queue<Action> _drains = new();

        /// <summary>Coalescing dispatch engine.</summary>
        private DispatchSequencerState _state;

        /// <summary>Initializes a new instance of the <see cref="TestDispatchSequencer"/> class.</summary>
        private TestDispatchSequencer()
        {
        }

        /// <summary>Gets the number of posted drains.</summary>
        public int PostCount { get; private set; }

        /// <inheritdoc/>
        public DateTimeOffset Now => DispatchSequencerState.Now;

        /// <inheritdoc/>
        public long Timestamp => DispatchSequencerState.Timestamp;

        /// <summary>Creates a sequencer whose dispatch state is wired only after construction has finished,
        /// so the engine never sees a half-built owner.</summary>
        /// <returns>The wired sequencer.</returns>
        public static TestDispatchSequencer Create()
        {
            TestDispatchSequencer sequencer = new();
            sequencer._state = new(sequencer, sequencer.Post, sequencer.RunDrain);
            return sequencer;
        }

        /// <summary>Runs the next posted drain.</summary>
        public void RunNextDrain() => _drains.Dequeue()();

        /// <inheritdoc/>
        public void Schedule(IWorkItem item) => _state.Schedule(item);

        /// <inheritdoc/>
        public void Schedule(IWorkItem item, long dueTimestamp) => _state.Schedule(item, dueTimestamp);

        /// <summary>Records and stores a posted drain.</summary>
        /// <param name="drain">The drain callback.</param>
        /// <returns><see langword="true"/> always, since the drain is recorded.</returns>
        private bool Post(Action drain)
        {
            PostCount++;
            _drains.Enqueue(drain);
            return true;
        }

        /// <summary>Forwards the cached drain callback to the engine.</summary>
        private void RunDrain() => _state.RunDrain();
    }

    /// <summary>Work item that records one value.</summary>
    /// <param name = "values">Recorded values.</param>
    /// <param name = "value">Value to record.</param>
    private sealed class RecordingWorkItem(List<int> values, int value) : IWorkItem, IsDisposed
    {
        /// <summary>Recorded values.</summary>
        private readonly List<int> _values = values;

        /// <summary>Value to record.</summary>
        private readonly int _value = value;

        /// <inheritdoc/>
        public bool IsDisposed { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => IsDisposed = true;

        /// <inheritdoc/>
        public void Execute() => _values.Add(_value);
    }

    /// <summary>Work item that schedules more work from inside a drain.</summary>
    /// <param name = "sequencer">Sequencer under test.</param>
    /// <param name = "values">Recorded values.</param>
    private sealed class ReentrantWorkItem(ISequencer sequencer, List<int> values) : IWorkItem
    {
        /// <summary>The value recorded by the work item this one schedules from inside the drain.</summary>
        private const int DeferredValue = 2;

        /// <summary>Sequencer under test.</summary>
        private readonly ISequencer _sequencer = sequencer;

        /// <summary>Recorded values.</summary>
        private readonly List<int> _values = values;

        /// <inheritdoc/>
        public void Execute()
        {
            _values.Add(1);
            _sequencer.Schedule(new RecordingWorkItem(_values, DeferredValue));
        }
    }
}
