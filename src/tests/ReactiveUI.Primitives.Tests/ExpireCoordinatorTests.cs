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

    /// <summary>Verifies an in-flight value wins the race and suppresses the superseded timeout.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task TimeoutDoesNotEnterObserverWhileOnNextIsInFlight()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        Signal<int> source = new();
        BlockingObserver observer = new();
        using var subscription = source.Expire(TimeSpan.FromTicks(One), clock).Subscribe(observer);

        var onNextTask = Task.Run(() => source.OnNext(One));
        await observer.OnNextEntered.Task.WaitAsync(WaitTimeout).ConfigureAwait(false);

        var timeoutTask = Task.Run(() => clock.AdvanceBy(TimeSpan.FromTicks(One)));
        await Task.Delay(TimeSpan.FromMilliseconds(50)).ConfigureAwait(false);

        await Assert.That(observer.ErrorEnteredDuringOnNext).IsFalse();

        observer.ReleaseOnNext.Set();
        await onNextTask.WaitAsync(WaitTimeout).ConfigureAwait(false);
        await timeoutTask.WaitAsync(WaitTimeout).ConfigureAwait(false);

        await Assert.That(observer.Errors).IsEqualTo(0);
        await Assert.That(observer.Values).IsEqualTo(One);
    }

    /// <summary>Observer that blocks source value handling so timeout serialization can be observed.</summary>
    private sealed class BlockingObserver : IObserver<int>
    {
        /// <summary>Gets the task completed when <see cref="OnNext"/> is entered.</summary>
        public TaskCompletionSource OnNextEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Gets the event released by the test to unblock <see cref="OnNext"/>.</summary>
        public ManualResetEventSlim ReleaseOnNext { get; } = new();

        /// <summary>Gets the number of forwarded values.</summary>
        public int Values { get; private set; }

        /// <summary>Gets the number of forwarded errors.</summary>
        public int Errors { get; private set; }

        /// <summary>Gets a value indicating whether an error entered while <see cref="OnNext"/> was active.</summary>
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
            OnNextEntered.SetResult();
            _ = ReleaseOnNext.Wait(WaitTimeout);
            IsInOnNext = false;
        }
    }
}
