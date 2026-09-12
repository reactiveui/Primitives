// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests iteration, scheduling, predicate and action failures, and disposal.</summary>
public class WhileObservableTests
{
    /// <summary>Synthetic error message attached to predicate failures.</summary>
    private const string PredicateFailedMessage = "predicate failed";

    /// <summary>Synthetic error message attached to action failures.</summary>
    private const string ActionFailedMessage = "action failed";

    /// <summary>Number of inline iterations to run.</summary>
    private const int IterationCount = 3;

    /// <summary>Verifies that the inline form runs until the predicate returns <c>false</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhileInline_ThenRunsUntilPredicateFalseAndCompletes()
    {
        var remaining = IterationCount;
        var emitted = 0;
        var completed = false;

        using var sub = ReactiveExtensions.While(() => remaining > 0, () => remaining--)
            .Subscribe(_ => emitted++, () => completed = true);

        await Assert.That(emitted).IsEqualTo(IterationCount);
        await Assert.That(completed).IsTrue();
        await Assert.That(remaining).IsEqualTo(0);
    }

    /// <summary>Verifies that the scheduler form dispatches every iteration via the scheduler.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhileWithScheduler_ThenRunsUntilPredicateFalse()
    {
        var remaining = IterationCount;
        var emitted = 0;
        var completed = false;
        ManualSequencer sequencer = new();

        using var sub = ReactiveExtensions.While(() => remaining > 0, () => remaining--, sequencer)
            .Subscribe(_ => emitted++, () => completed = true);

        // Nothing runs until the queued work is drained, proving every iteration went through the scheduler.
        await Assert.That(emitted).IsEqualTo(0);
        sequencer.RunAll();
        await Assert.That(emitted).IsEqualTo(IterationCount);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that an exception thrown by the predicate is forwarded.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhilePredicateThrows_ThenForwardsError()
    {
        Exception? caught = null;
        InvalidOperationException expected = new(PredicateFailedMessage);

        using var sub = ReactiveExtensions.While(() => throw expected, static () => { })
            .Subscribe(static _ => { }, ex => caught = ex);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that an exception thrown by the action is forwarded.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhileActionThrows_ThenForwardsError()
    {
        Exception? caught = null;
        InvalidOperationException expected = new(ActionFailedMessage);

        using var sub = ReactiveExtensions.While(static () => true, () => throw expected)
            .Subscribe(static _ => { }, ex => caught = ex);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that disposing the scheduled loop stops further iterations.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhileScheduledThenDisposed_ThenIterationStops()
    {
        var ran = 0;
        ManualSequencer sequencer = new();

        var sub = ReactiveExtensions.While(static () => true, () => ran++, sequencer)
            .Subscribe(static _ => { });

        // One iteration, which arms the next one.
        sequencer.RunNext();
        await Assert.That(ran).IsEqualTo(1);

        sub.Dispose();
        sequencer.RunAll();
        await Assert.That(ran).IsEqualTo(1);
    }

    /// <summary>A sequencer that queues every work item so the test decides when each iteration runs.</summary>
    private sealed class ManualSequencer : ISequencer
    {
        /// <summary>Work items scheduled and not yet run.</summary>
        private readonly Queue<IWorkItem> _pending = new();

        /// <summary>Gets the sequencer's notion of current time, which never moves.</summary>
        public DateTimeOffset Now => DateTimeOffset.UnixEpoch;

        /// <summary>Gets the sequencer's monotonic timestamp, which never moves.</summary>
        public long Timestamp => 0;

        /// <inheritdoc/>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public void Schedule(IWorkItem item) => _pending.Enqueue(item);

        /// <inheritdoc/>
        [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
        public void Schedule(IWorkItem item, long dueTimestamp) => Schedule(item);

        /// <summary>Runs the oldest queued work item, if any.</summary>
        internal void RunNext()
        {
            if (_pending.Count == 0)
            {
                return;
            }

            _pending.Dequeue().Execute();
        }

        /// <summary>Drains the queue, including work items queued by the items it runs.</summary>
        internal void RunAll()
        {
            while (_pending.Count > 0)
            {
                _pending.Dequeue().Execute();
            }
        }
    }
}
