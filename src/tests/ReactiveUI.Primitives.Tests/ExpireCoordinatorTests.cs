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

    /// <summary>A value after the inactivity deadline expires the sequence even when the timeout callback remains queued.</summary>
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

    /// <summary>A value inside the inactivity window is forwarded while its timeout callback remains queued.</summary>
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

    /// <summary>Verifies a timeout that becomes due while a value is in flight is suppressed by that value.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TimeoutDoesNotEnterObserverWhileOnNextIsInFlight()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        ReentrantTimeoutObserver observer = new(clock, TimeSpan.FromTicks(One));
        using var subscription = source.Expire(TimeSpan.FromTicks(One), clock).Subscribe(observer);

        source.OnNext(One);

        await Assert.That(observer.ErrorEnteredDuringOnNext).IsFalse();
        await Assert.That(observer.Errors).IsEqualTo(0);
        await Assert.That(observer.Values).IsEqualTo(One);
    }

    /// <summary>Tracks virtual time and accepts work without dispatching it.</summary>
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

    /// <summary>Observer that makes the armed timeout due from inside <see cref="OnNext"/>, so the timeout is
    /// dispatched while the value is still in flight.</summary>
    /// <param name="clock">The clock that dispatches due work inline.</param>
    /// <param name="dueTime">The amount to advance the clock by so the armed timeout becomes due.</param>
    private sealed class ReentrantTimeoutObserver(VirtualClock clock, TimeSpan dueTime) : IObserver<int>
    {
        /// <summary>Gets the number of forwarded values.</summary>
        public int Values { get; private set; }

        /// <summary>Gets the number of forwarded errors.</summary>
        public int Errors { get; private set; }

        /// <summary>Gets a value indicating whether an error arrived while <see cref="OnNext"/> was active.</summary>
        public bool ErrorEnteredDuringOnNext { get; private set; }

        /// <summary>Gets or sets a value indicating whether <see cref="OnNext"/> is active.</summary>
        private bool IsInOnNext { get; set; }

        /// <inheritdoc/>
        public void OnCompleted()
        {
        }

        /// <inheritdoc/>
        public void OnError(Exception error)
        {
            if (IsInOnNext)
            {
                ErrorEnteredDuringOnNext = true;
            }

            Errors++;
        }

        /// <inheritdoc/>
        public void OnNext(int value)
        {
            Values++;
            IsInOnNext = true;
            clock.AdvanceBy(dueTime);
            IsInOnNext = false;
        }
    }
}
