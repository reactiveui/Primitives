// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Runtime.CompilerServices;
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

    /// <summary>Expected post count once a rejected or failed post has been retried.</summary>
    private const int ExpectedRetriedPostCount = 2;

    /// <summary>Message carried by a dispatcher post that fails on purpose.</summary>
    private const string PostFailureMessage = "the dispatcher rejected the post";

    /// <summary>How far in the past a timestamp must be for the shared delay helper to report no delay at all.</summary>
    private const long ElapsedTimestampOffset = 1000;

    /// <summary>Value recorded by the work item that owns the outer drain.</summary>
    private const int OuterDrainValue = 1;

    /// <summary>Value recorded by the work item a nested drain swallows.</summary>
    private const int NestedDrainValue = 2;

    /// <summary>Expected burst values.</summary>
    private static readonly int[] ExpectedBurstValues = [1, 2, 3];

    /// <summary>Expected reentrant values.</summary>
    private static readonly int[] ExpectedReentrantValues = [1, 2];

    /// <summary>The two values a drain must record, in queue order.</summary>
    private static readonly int[] ExpectedDrainPair = [OuterDrainValue, NestedDrainValue];

    /// <summary>The single value recorded when only the live half of a pair of work items runs.</summary>
    private static readonly int[] ExpectedLiveOnly = [OuterDrainValue];

    /// <summary>How far ahead delayed work is scheduled.</summary>
    private static readonly TimeSpan DelayedDueTime = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// A due time far enough out that no scheduling pause between capturing the timestamp and reading the
    /// remaining delay can elapse it. Asserting a still-pending delay against a short due time races the wall
    /// clock: a loaded runner can spend longer than the due time inside the preceding assertion, leaving nothing
    /// to wait for and clamping the result to zero.
    /// </summary>
    private static readonly TimeSpan UnreachableDueTime = TimeSpan.FromHours(1);

    /// <summary>How long a test watches for delayed work that must never reach the dispatcher.</summary>
    private static readonly TimeSpan CancelObservationWindow = TimeSpan.FromMilliseconds(400);

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
            (values, Value: StatefulScheduleValue),
            static state => state.values.Add(state.Value));
        await Assert.That(values[0]).IsEqualTo(StatefulScheduleValue);
    }

    /// <summary>Verifies the dispatcher entry point runs live work and skips work cancelled before the drain reached it.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RunIfActiveRunsLiveWorkAndSkipsCancelledWork()
    {
        List<int> values = [];
        RecordingWorkItem live = new(values, OuterDrainValue);
        RecordingWorkItem cancelled = new(values, NestedDrainValue);
        cancelled.Dispose();

        DispatchSequencerState.RunIfActive(live);
        DispatchSequencerState.RunIfActive(cancelled);

        await Assert.That(values.SequenceEqual(ExpectedLiveOnly)).IsTrue();
    }

    /// <summary>Verifies the shared delay helper clamps an elapsed due timestamp to no delay at all.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayUntilClampsElapsedDueTimestampsToZero()
    {
        var now = DispatchSequencerState.Timestamp;

        await Assert.That(DispatchSequencerState.DelayUntil(now - ElapsedTimestampOffset)).IsEqualTo(TimeSpan.Zero);
        await Assert.That(DispatchSequencerState.DelayUntil(Sequencer.AddTimestamp(now, UnreachableDueTime)) > TimeSpan.Zero)
            .IsTrue();
    }

    /// <summary>Verifies a platform delayed-scheduling override receives delayed work instead of the shared timer.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedWorkUsesThePlatformOverrideWhenOneIsSupplied()
    {
        var sequencer = ConfigurableDispatchSequencer.CreateWithDelayedOverride();
        List<int> values = [];
        RecordingWorkItem item = new(values, OuterDrainValue);
        var dueTimestamp = Sequencer.AddTimestamp(sequencer.Timestamp, DelayedDueTime);

        sequencer.Schedule(item, dueTimestamp);

        await Assert.That(sequencer.DelayedWork.Count).IsEqualTo(1);
        await Assert.That(sequencer.DelayedWork[0].Item).IsSameReferenceAs(item);
        await Assert.That(sequencer.DelayedWork[0].DueTimestamp).IsEqualTo(dueTimestamp);
        await Assert.That(sequencer.PostCount).IsEqualTo(0);
        await Assert.That(values.Count).IsEqualTo(0);
    }

    /// <summary>Verifies delayed work cancelled before it is due never reaches the dispatcher.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DelayedWorkCancelledBeforeItIsDueNeverReachesTheDispatcher()
    {
        var sequencer = ConfigurableDispatchSequencer.Create();
        List<int> values = [];
        RecordingWorkItem item = new(values, OuterDrainValue);

        sequencer.Schedule(item, Sequencer.AddTimestamp(sequencer.Timestamp, DelayedDueTime));
        item.Dispose();

        await Task.Delay(CancelObservationWindow);

        await Assert.That(sequencer.PostCount).IsEqualTo(0);
        await Assert.That(values.Count).IsEqualTo(0);
    }

    /// <summary>Verifies no drain is posted while nothing is queued.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task PostDrainDoesNothingWhenNoWorkIsQueued()
    {
        var sequencer = ConfigurableDispatchSequencer.Create();

        sequencer.PostDrain();

        await Assert.That(sequencer.PostCount).IsEqualTo(0);
    }

    /// <summary>Verifies a dispatcher that rejects a post releases the drain latch, so the next schedule posts again.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RejectedPostReleasesTheDrainLatch()
    {
        var sequencer = ConfigurableDispatchSequencer.Create();
        sequencer.PostSucceeds = false;
        List<int> values = [];

        sequencer.Schedule(new RecordingWorkItem(values, OuterDrainValue));
        sequencer.PostSucceeds = true;
        sequencer.Schedule(new RecordingWorkItem(values, NestedDrainValue));

        // A latch left set by the rejected post would swallow this second post, stranding both work items.
        await Assert.That(sequencer.PostCount).IsEqualTo(ExpectedRetriedPostCount);

        sequencer.RunNextDrain();
        await Assert.That(values.SequenceEqual(ExpectedDrainPair)).IsTrue();
    }

    /// <summary>Verifies a dispatcher post that throws surfaces the failure and still releases the drain latch.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FailedPostReleasesTheDrainLatchAndRethrows()
    {
        var sequencer = ConfigurableDispatchSequencer.Create();
        sequencer.PostFailure = new InvalidOperationException(PostFailureMessage);
        List<int> values = [];

        await Assert.That(() => sequencer.Schedule(new RecordingWorkItem(values, OuterDrainValue)))
            .ThrowsExactly<InvalidOperationException>();

        sequencer.PostFailure = null;
        sequencer.Schedule(new RecordingWorkItem(values, NestedDrainValue));
        sequencer.RunNextDrain();

        await Assert.That(sequencer.PostCount).IsEqualTo(ExpectedRetriedPostCount);
        await Assert.That(values.SequenceEqual(ExpectedDrainPair)).IsTrue();
    }

    /// <summary>Verifies a drain re-entered by one of its own work items still runs every queued item exactly once.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ReentrantDrainRunsEachQueuedItemExactlyOnce()
    {
        var sequencer = ConfigurableDispatchSequencer.Create();
        List<int> values = [];

        sequencer.Schedule(new DrainingWorkItem(sequencer, values, OuterDrainValue));
        sequencer.Schedule(new RecordingWorkItem(values, NestedDrainValue));

        sequencer.RunNextDrain();

        await Assert.That(values.SequenceEqual(ExpectedDrainPair)).IsTrue();
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RunNextDrain() => _drains.Dequeue()();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Schedule(IWorkItem item) => _state.Schedule(item);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
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
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Execute() => _values.Add(_value);
    }

    /// <summary>
    /// Dispatch sequencer whose post can be made to reject a drain or throw, and which can capture delayed work
    /// through a platform override instead of falling back to the shared thread-pool timer.
    /// </summary>
    private sealed class ConfigurableDispatchSequencer : ISequencer
    {
        /// <summary>Posted drains awaiting a run.</summary>
        private readonly Queue<Action> _drains = new();

        /// <summary>Delayed work captured by the platform override.</summary>
        private readonly List<DelayedWorkItem> _delayedWork = [];

        /// <summary>Coalescing dispatch engine.</summary>
        private DispatchSequencerState _state;

        /// <summary>Initializes a new instance of the <see cref="ConfigurableDispatchSequencer"/> class.</summary>
        private ConfigurableDispatchSequencer()
        {
        }

        /// <summary>Gets the number of posts the engine attempted.</summary>
        public int PostCount { get; private set; }

        /// <summary>Gets the delayed work the platform override captured.</summary>
        public List<DelayedWorkItem> DelayedWork => _delayedWork;

        /// <summary>Gets or sets a value indicating whether the dispatcher accepts a posted drain.</summary>
        public bool PostSucceeds { get; set; } = true;

        /// <summary>Gets or sets the failure a post throws, or <see langword="null"/> when the post must not throw.</summary>
        public Exception? PostFailure { get; set; }

        /// <inheritdoc/>
        public DateTimeOffset Now => DispatchSequencerState.Now;

        /// <inheritdoc/>
        public long Timestamp => DispatchSequencerState.Timestamp;

        /// <summary>Creates a sequencer that falls back to the shared timer for delayed work.</summary>
        /// <returns>The wired sequencer.</returns>
        public static ConfigurableDispatchSequencer Create()
        {
            ConfigurableDispatchSequencer sequencer = new();
            sequencer._state = new(sequencer, sequencer.Post, sequencer.RunDrain);
            return sequencer;
        }

        /// <summary>Creates a sequencer whose delayed work is captured by a platform override.</summary>
        /// <returns>The wired sequencer.</returns>
        public static ConfigurableDispatchSequencer CreateWithDelayedOverride()
        {
            ConfigurableDispatchSequencer sequencer = new();
            sequencer._state = new(sequencer, sequencer.Post, sequencer.RunDrain, sequencer.CaptureDelayed);
            return sequencer;
        }

        /// <summary>Runs the next posted drain.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RunNextDrain() => _drains.Dequeue()();

        /// <summary>Runs a drain batch on the calling thread.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void RunDrain() => _state.RunDrain();

        /// <summary>Asks the engine to post a drain.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void PostDrain() => _state.PostDrain();

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Schedule(IWorkItem item) => _state.Schedule(item);

        /// <inheritdoc/>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Schedule(IWorkItem item, long dueTimestamp) => _state.Schedule(item, dueTimestamp);

        /// <summary>Records the posted drain, or rejects it, according to the configured behaviour.</summary>
        /// <param name="drain">The drain callback.</param>
        /// <returns><see langword="true"/> when the drain was accepted.</returns>
        private bool Post(Action drain)
        {
            PostCount++;
            if (PostFailure is not null)
            {
                throw PostFailure;
            }

            if (!PostSucceeds)
            {
                return false;
            }

            _drains.Enqueue(drain);
            return true;
        }

        /// <summary>Captures delayed work instead of handing it to the shared timer.</summary>
        /// <param name="item">The scheduled item.</param>
        /// <param name="dueTimestamp">The absolute monotonic due timestamp.</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void CaptureDelayed(IWorkItem item, long dueTimestamp) => _delayedWork.Add(new(item, dueTimestamp));
    }

    /// <summary>Work item that records a value and then re-enters the sequencer's drain from inside it.</summary>
    /// <param name = "sequencer">Sequencer under test.</param>
    /// <param name = "values">Recorded values.</param>
    /// <param name = "value">Value to record.</param>
    private sealed class DrainingWorkItem(ConfigurableDispatchSequencer sequencer, List<int> values, int value) : IWorkItem
    {
        /// <summary>Sequencer under test.</summary>
        private readonly ConfigurableDispatchSequencer _sequencer = sequencer;

        /// <summary>Recorded values.</summary>
        private readonly List<int> _values = values;

        /// <summary>Value to record.</summary>
        private readonly int _value = value;

        /// <inheritdoc/>
        public void Execute()
        {
            _values.Add(_value);
            _sequencer.RunDrain();
        }
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

    /// <summary>Delayed work handed to a platform delayed-scheduling override.</summary>
    /// <param name="Item">The work item that was scheduled.</param>
    /// <param name="DueTimestamp">The absolute monotonic timestamp the item is due at.</param>
    private sealed record DelayedWorkItem(IWorkItem Item, long DueTimestamp);
}
