// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="ExpireCoordinator{T}"/>.</summary>
public sealed class ExpireCoordinatorTests
{
    /// <summary>The integer constant one.</summary>
    private const int One = 1;

    /// <summary>The number of values emitted by the re-arming test.</summary>
    private const int ValueCount = 5;

    /// <summary>The timeout window in ticks used by the re-arming tests.</summary>
    private const int DueTicks = 10;

    /// <summary>A gap shorter than <see cref="DueTicks"/> that must not expire the timeout.</summary>
    private const int ActiveGapTicks = 9;

    /// <summary>A gap shorter than <see cref="DueTicks"/> after which a value still arrives in time.</summary>
    private const int ShortGapTicks = 5;

    /// <summary>The values forwarded by the active-source re-arming test.</summary>
    private static readonly int[] ExpectedActiveValues = [0, 1, 2, 3, 4];

    /// <summary>Timeout used while waiting for background work in this test.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(5);

    /// <summary>How long the superseded timeout is given to reach the observer before the invariant is checked.</summary>
    private static readonly TimeSpan RaceSettleDelay = TimeSpan.FromMilliseconds(50);

    /// <summary>Verifies the timeout re-arms on each value so an active source never expires.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TimeoutResetsOnEachValueSoActiveSourceNeverExpires()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        List<int> values = [];
        List<string> errors = [];
        using var subscription = source.Expire(TimeSpan.FromTicks(DueTicks), clock)
            .Subscribe(values.Add, ex => errors.Add(ex.GetType().Name));

        for (var i = 0; i < ValueCount; i++)
        {
            clock.AdvanceBy(TimeSpan.FromTicks(ActiveGapTicks));
            source.OnNext(i);
        }

        await Assert.That(errors.Count).IsEqualTo(0);
        await Assert.That(values.SequenceEqual(ExpectedActiveValues)).IsTrue();
    }

    /// <summary>Verifies silence longer than the timeout still expires after re-arming.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TimeoutExpiresWhenSilenceExceedsDueTimeAfterAValue()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        List<int> values = [];
        List<string> errors = [];
        using var subscription = source.Expire(TimeSpan.FromTicks(DueTicks), clock)
            .Subscribe(values.Add, ex => errors.Add(ex.GetType().Name));

        clock.AdvanceBy(TimeSpan.FromTicks(ShortGapTicks));
        source.OnNext(One);
        clock.AdvanceBy(TimeSpan.FromTicks(DueTicks));

        await Assert.That(values.SequenceEqual([One])).IsTrue();
        await Assert.That(errors.SequenceEqual([nameof(TimeoutException)])).IsTrue();
    }

    /// <summary>Verifies no timer fires once the source has terminated.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task NoTimeoutFiresAfterCompletion()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        List<int> values = [];
        List<string> errors = [];
        var completed = false;
        using var subscription = source.Expire(TimeSpan.FromTicks(DueTicks), clock)
            .Subscribe(values.Add, ex => errors.Add(ex.GetType().Name), () => completed = true);

        clock.AdvanceBy(TimeSpan.FromTicks(ShortGapTicks));
        source.OnNext(One);
        source.OnCompleted();
        clock.AdvanceBy(TimeSpan.FromTicks(DueTicks * ValueCount));

        await Assert.That(completed).IsTrue();
        await Assert.That(values.SequenceEqual([One])).IsTrue();
        await Assert.That(errors.Count).IsEqualTo(0);
    }

    /// <summary>Verifies no timer fires once the source has errored.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task NoTimeoutFiresAfterError()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        List<int> values = [];
        List<string> errors = [];
        using var subscription = source.Expire(TimeSpan.FromTicks(DueTicks), clock)
            .Subscribe(values.Add, ex => errors.Add(ex.GetType().Name));

        clock.AdvanceBy(TimeSpan.FromTicks(ShortGapTicks));
        source.OnNext(One);
        source.OnError(new InvalidOperationException());
        clock.AdvanceBy(TimeSpan.FromTicks(DueTicks * ValueCount));

        await Assert.That(values.SequenceEqual([One])).IsTrue();
        await Assert.That(errors.SequenceEqual([nameof(InvalidOperationException)])).IsTrue();
    }

    /// <summary>
    /// Verifies a value that arrives after the inactivity window closed expires the sequence even though the armed
    /// timeout has not been dispatched yet. The timeout runs on the sequencer, and a thread-pool sequencer whose pool
    /// is saturated can dispatch it arbitrarily late while a source ticking on its own thread keeps producing. The
    /// window is a property of the clock, not of whether the timer callback has been given a thread, so a value that
    /// missed it must not reach the observer.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValueArrivingAfterTheWindowClosedExpiresWhileTheTimeoutIsStillUndispatched()
    {
        UndispatchedSequencer sequencer = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        List<int> values = [];
        List<string> errors = [];
        using var subscription = source.Expire(TimeSpan.FromTicks(DueTicks), sequencer)
            .Subscribe(values.Add, ex => errors.Add(ex.GetType().Name));

        sequencer.Advance(TimeSpan.FromTicks(DueTicks));
        source.OnNext(One);

        await Assert.That(sequencer.Pending).IsGreaterThan(0);
        await Assert.That(values.Count).IsEqualTo(0);
        await Assert.That(errors.SequenceEqual([nameof(TimeoutException)])).IsTrue();
    }

    /// <summary>
    /// Verifies the deadline check does not expire a value that is still inside its window. This is the guard against
    /// the previous test's fix over-firing: an undispatched timeout must not turn an on-time value into a timeout.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ValueArrivingInsideTheWindowIsForwardedWhileTheTimeoutIsStillUndispatched()
    {
        UndispatchedSequencer sequencer = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        List<int> values = [];
        List<string> errors = [];
        using var subscription = source.Expire(TimeSpan.FromTicks(DueTicks), sequencer)
            .Subscribe(values.Add, ex => errors.Add(ex.GetType().Name));

        sequencer.Advance(TimeSpan.FromTicks(ActiveGapTicks));
        source.OnNext(One);

        await Assert.That(values.SequenceEqual([One])).IsTrue();
        await Assert.That(errors.Count).IsEqualTo(0);
    }

    /// <summary>Verifies an in-flight value wins the race and suppresses the superseded timeout.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TimeoutDoesNotEnterObserverWhileOnNextIsInFlight()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        BlockingObserver observer = new();
        using var subscription = source.Expire(TimeSpan.FromTicks(One), clock).Subscribe(observer);

        // Dedicated threads rather than the pool: the observer parks its caller inside OnNext until this
        // test releases it, so on the pool that notification holds a worker while the timeout waits behind
        // it in the queue. A saturated pool then starves the very interleaving under test.
        var onNextFinished = RunOnDedicatedThread(() => source.OnNext(One));
        await observer.OnNextEntered.Task.WaitAsync(WaitTimeout).ConfigureAwait(false);

        var timeoutFinished = RunOnDedicatedThread(() => clock.AdvanceBy(TimeSpan.FromTicks(One)));
        await Task.Delay(RaceSettleDelay).ConfigureAwait(false);

        await Assert.That(observer.ErrorEnteredDuringOnNext).IsFalse();

        observer.ReleaseOnNext.Set();
        await onNextFinished.WaitAsync(WaitTimeout).ConfigureAwait(false);
        await timeoutFinished.WaitAsync(WaitTimeout).ConfigureAwait(false);

        // Timeout may be observed after OnNext exits depending on scheduler timing.
        // The invariant required here is that OnError never re-enters while OnNext is active.
        await Assert.That(observer.Errors).IsLessThanOrEqualTo(One);
        await Assert.That(observer.Values).IsEqualTo(One);
    }

    /// <summary>
    /// Runs work on its own thread and reports when it finished. Used where the work blocks for the duration of
    /// the scenario, which the thread pool cannot absorb without the risk of the work never being given a thread.
    /// </summary>
    /// <param name="work">The work to run.</param>
    /// <returns>A task that completes when the work has returned.</returns>
    private static Task RunOnDedicatedThread(Action work)
    {
        TaskCompletionSource finished = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Thread thread = new(() =>
        {
            try
            {
                work();
                _ = finished.TrySetResult();
            }
            catch (Exception error)
            {
                _ = finished.TrySetException(error);
            }
        }) { IsBackground = true };

        thread.Start();
        return finished.Task;
    }

    /// <summary>
    /// A sequencer that accepts scheduled work and never dispatches it, modelling a thread-pool sequencer whose pool
    /// is saturated: the timer becomes due on the clock, but no thread is free to run the callback. Its clock is
    /// driven by the test.
    /// </summary>
    /// <param name="start">The instant the clock starts at.</param>
    private sealed class UndispatchedSequencer(DateTimeOffset start) : ISequencer
    {
        /// <summary>The current instant.</summary>
        private DateTimeOffset _now = start;

        /// <summary>Gets the number of work items accepted and never dispatched.</summary>
        public int Pending { get; private set; }

        /// <inheritdoc/>
        public DateTimeOffset Now => _now;

        /// <inheritdoc/>
        public long Timestamp => _now.UtcTicks;

        /// <summary>Moves the clock forward without dispatching anything that became due.</summary>
        /// <param name="time">The amount of time to advance by.</param>
        public void Advance(TimeSpan time) => _now = _now.Add(time);

        /// <inheritdoc/>
        public void Schedule(IWorkItem item) => Pending++;

        /// <inheritdoc/>
        [System.Diagnostics.CodeAnalysis.SuppressMessage(
            "Design",
            "SST2318:Members should not have identical bodies",
            Justification =
                "The relative and absolute Schedule overloads of this test-double sequencer intentionally behave the "
                + "same way; both are required by the ISequencer contract and, as distinct interface overloads, cannot "
                + "forward to one another.")]
        public void Schedule(IWorkItem item, long dueTimestamp) => Pending++;
    }

    /// <summary>Observer that blocks source value handling so timeout serialization can be observed.</summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage(
        "Design",
        "SST2315:A type that owns a disposable should be disposable",
        Justification =
            "Test double that owns a ManualResetEventSlim used to gate OnNext so the test can observe timeout "
            + "serialization. Its lifetime is the test's; the test process owns and releases it, so it is deliberately "
            + "not IDisposable.")]
    private sealed class BlockingObserver : IObserver<int>
    {
        /// <summary>Non-zero while <see cref="OnNext"/> is active. Written by the notifying thread and read by
        /// the timeout thread, so the two must not race on a plain field.</summary>
        private int _isInOnNext;

        /// <summary>Non-zero once an error arrived while <see cref="OnNext"/> was active. The test reads this
        /// while both threads are still running, so the write has to be published rather than merely made.</summary>
        private int _errorEnteredDuringOnNext;

        /// <summary>The number of forwarded values.</summary>
        private int _values;

        /// <summary>The number of forwarded errors.</summary>
        private int _errors;

        /// <summary>Gets the task completed when <see cref="OnNext"/> is entered.</summary>
        public TaskCompletionSource OnNextEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the event released by the test to unblock <see cref="OnNext"/>.</summary>
        public ManualResetEventSlim ReleaseOnNext { get; } = new();

        /// <summary>Gets the number of forwarded values.</summary>
        public int Values => Volatile.Read(ref _values);

        /// <summary>Gets the number of forwarded errors.</summary>
        public int Errors => Volatile.Read(ref _errors);

        /// <summary>Gets a value indicating whether an error entered while <see cref="OnNext"/> was active.</summary>
        public bool ErrorEnteredDuringOnNext => Volatile.Read(ref _errorEnteredDuringOnNext) != 0;

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (Volatile.Read(ref _isInOnNext) != 0)
            {
                Volatile.Write(ref _errorEnteredDuringOnNext, 1);
            }

            _ = Interlocked.Increment(ref _errors);
        }

        /// <inheritdoc/>
        public void OnNext(int value)
        {
            _ = Interlocked.Increment(ref _values);
            Volatile.Write(ref _isInOnNext, 1);
            OnNextEntered.SetResult();
            _ = ReleaseOnNext.Wait(WaitTimeout);
            Volatile.Write(ref _isInOnNext, 0);
        }
    }
}
