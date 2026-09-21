// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests <see cref="VirtualClock"/> as a <see cref="TimeProvider"/>.</summary>
public class VirtualClockTests
{
    /// <summary>The delay of the first firing in the timer tests.</summary>
    private static readonly TimeSpan OneSecond = TimeSpan.FromSeconds(1);

    /// <summary>The interval between periodic firings in the timer tests.</summary>
    private static readonly TimeSpan TwoSeconds = TimeSpan.FromSeconds(2);

    /// <summary>A delay later than every firing the timer tests expect.</summary>
    private static readonly TimeSpan TenSeconds = TimeSpan.FromSeconds(10);

    /// <summary>The virtual time starts at the unix epoch.</summary>
    private static readonly DateTimeOffset Epoch = DateTimeOffset.UnixEpoch;

    /// <summary>The times a timer that first fires after one second and then every two seconds reaches within ten seconds.</summary>
    private static readonly TimeSpan[] OddSeconds =
    [
        TimeSpan.FromSeconds(1),
        TimeSpan.FromSeconds(3),
        TimeSpan.FromSeconds(5),
        TimeSpan.FromSeconds(7),
        TimeSpan.FromSeconds(9),
    ];

    /// <summary>The clock is usable wherever a time provider is expected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ClockIsATimeProvider()
    {
        TimeProvider provider = new VirtualClock();

        await Assert.That(provider).IsTypeOf<VirtualClock>();
    }

    /// <summary>The UTC time follows the virtual clock as it advances.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GetUtcNowFollowsTheVirtualClock()
    {
        VirtualClock clock = new(Epoch);
        TimeProvider provider = clock;

        await Assert.That(provider.GetUtcNow()).IsEqualTo(Epoch);

        clock.AdvanceBy(TenSeconds);

        await Assert.That(provider.GetUtcNow()).IsEqualTo(Epoch + TenSeconds);
        await Assert.That(provider.GetUtcNow()).IsEqualTo(clock.Now);
    }

    /// <summary>Timestamps count ticks of the virtual clock at <see cref="TimeSpan.TicksPerSecond"/>.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GetTimestampCountsTicksOfTheVirtualClock()
    {
        VirtualClock clock = new(Epoch);
        TimeProvider provider = clock;

        await Assert.That(provider.TimestampFrequency).IsEqualTo(TimeSpan.TicksPerSecond);
        await Assert.That(provider.GetTimestamp()).IsEqualTo(Epoch.UtcTicks);

        clock.AdvanceBy(OneSecond);

        await Assert.That(provider.GetTimestamp()).IsEqualTo((Epoch + OneSecond).UtcTicks);
    }

    /// <summary>Elapsed time between two timestamps equals the virtual time that passed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GetElapsedTimeMeasuresVirtualTime()
    {
        VirtualClock clock = new(Epoch);
        TimeProvider provider = clock;
        var start = provider.GetTimestamp();

        clock.AdvanceBy(TenSeconds);

        await Assert.That(provider.GetElapsedTime(start)).IsEqualTo(TenSeconds);
    }

    /// <summary>A one-shot timer fires exactly once, when the clock reaches its due time.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task OneShotTimerFiresOnceWhenTheClockReachesItsDueTime()
    {
        VirtualClock clock = new(Epoch);
        List<DateTimeOffset> fired = [];
        await using var timer =clock.CreateTimer(_ => fired.Add(clock.Now), null, OneSecond, Timeout.InfiniteTimeSpan);

        clock.AdvanceBy(OneSecond - TimeSpan.FromTicks(1));

        await Assert.That(fired).IsEmpty();

        clock.AdvanceBy(TenSeconds);

        await Assert.That(fired.SequenceEqual([Epoch + OneSecond])).IsTrue();
    }

    /// <summary>A timer hands its state to the callback.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TimerPassesItsStateToTheCallback()
    {
        VirtualClock clock = new(Epoch);
        object state = new();
        object? received = null;
        await using var timer =clock.CreateTimer(s => received = s, state, OneSecond, Timeout.InfiniteTimeSpan);

        clock.AdvanceBy(OneSecond);

        await Assert.That(received).IsSameReferenceAs(state);
    }

    /// <summary>A periodic timer fires after its due time and then once per period.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task PeriodicTimerFiresOnceEveryPeriod()
    {
        VirtualClock clock = new(Epoch);
        List<TimeSpan> fired = [];
        await using var timer =clock.CreateTimer(_ => fired.Add(clock.Now - Epoch), null, OneSecond, TwoSeconds);

        clock.AdvanceBy(TenSeconds - TimeSpan.FromTicks(1));

        await Assert.That(fired.SequenceEqual(OddSeconds)).IsTrue();
    }

    /// <summary>A timer with a zero period fires once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TimerWithAZeroPeriodFiresOnce()
    {
        VirtualClock clock = new(Epoch);
        var fired = 0;
        await using var timer =clock.CreateTimer(_ => fired++, null, OneSecond, TimeSpan.Zero);

        clock.AdvanceBy(TenSeconds);

        await Assert.That(fired).IsEqualTo(1);
    }

    /// <summary>A timer with a zero due time fires when the clock next runs work.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TimerWithAZeroDueTimeFiresWhenTheClockStarts()
    {
        VirtualClock clock = new(Epoch);
        var fired = 0;
        await using var timer =clock.CreateTimer(_ => fired++, null, TimeSpan.Zero, Timeout.InfiniteTimeSpan);

        await Assert.That(fired).IsEqualTo(0);

        clock.Start();

        await Assert.That(fired).IsEqualTo(1);
    }

    /// <summary>A timer created with an infinite due time stays stopped until it is changed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TimerWithAnInfiniteDueTimeStaysStoppedUntilChanged()
    {
        VirtualClock clock = new(Epoch);
        var fired = 0;
        await using var timer =clock.CreateTimer(_ => fired++, null, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

        clock.AdvanceBy(TenSeconds);

        await Assert.That(fired).IsEqualTo(0);

        await Assert.That(timer.Change(OneSecond, Timeout.InfiniteTimeSpan)).IsTrue();
        clock.AdvanceBy(TenSeconds);

        await Assert.That(fired).IsEqualTo(1);
    }

    /// <summary>Changing a timer replaces its pending firing.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ChangeReplacesThePendingFiring()
    {
        VirtualClock clock = new(Epoch);
        List<TimeSpan> fired = [];
        await using var timer =clock.CreateTimer(_ => fired.Add(clock.Now - Epoch), null, TenSeconds, Timeout.InfiniteTimeSpan);

        await Assert.That(timer.Change(TwoSeconds, Timeout.InfiniteTimeSpan)).IsTrue();
        clock.AdvanceBy(TenSeconds + TenSeconds);

        await Assert.That(fired.SequenceEqual([TwoSeconds])).IsTrue();
    }

    /// <summary>Changing a timer to an infinite due time cancels its pending firing.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ChangeToAnInfiniteDueTimeCancelsThePendingFiring()
    {
        VirtualClock clock = new(Epoch);
        var fired = 0;
        await using var timer =clock.CreateTimer(_ => fired++, null, OneSecond, OneSecond);

        _ = timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        clock.AdvanceBy(TenSeconds);

        await Assert.That(fired).IsEqualTo(0);
    }

    /// <summary>A timer that changes its own period from inside the callback keeps firing at the new period.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ChangeInsideTheCallbackSetsTheNextFiring()
    {
        VirtualClock clock = new(Epoch);
        List<TimeSpan> fired = [];
        ITimer? timer = null;
        timer = clock.CreateTimer(
            _ =>
            {
                fired.Add(clock.Now - Epoch);
                _ = timer!.Change(TwoSeconds, Timeout.InfiniteTimeSpan);
            },
            null,
            OneSecond,
            Timeout.InfiniteTimeSpan);

        clock.AdvanceBy(TenSeconds);
        await timer.DisposeAsync();

        await Assert.That(fired.SequenceEqual(OddSeconds)).IsTrue();
    }

    /// <summary>Disposing a timer cancels its pending firing and makes later changes report failure.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposeCancelsThePendingFiringAndRejectsChanges()
    {
        VirtualClock clock = new(Epoch);
        var fired = 0;
        var timer = clock.CreateTimer(_ => fired++, null, OneSecond, OneSecond);

        timer.Dispose();
        clock.AdvanceBy(TenSeconds);

        await Assert.That(fired).IsEqualTo(0);
        await Assert.That(timer.Change(OneSecond, OneSecond)).IsFalse();
    }

    /// <summary>Disposing a periodic timer from its callback stops later firings.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposeInsideTheCallbackStopsAPeriodicTimer()
    {
        VirtualClock clock = new(Epoch);
        var fired = 0;
        ITimer? timer = null;
        timer = clock.CreateTimer(
            _ =>
            {
                fired++;
                timer!.Dispose();
            },
            null,
            OneSecond,
            OneSecond);

        clock.AdvanceBy(TenSeconds);

        await Assert.That(fired).IsEqualTo(1);
    }

    /// <summary>A firing that was already dequeued when the timer is disposed does not invoke the callback or queue another firing.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task FiringAfterDisposeDoesNothing()
    {
        VirtualClock clock = new(Epoch);
        var fired = 0;
        var timer = (VirtualClockTimer)clock.CreateTimer(_ => fired++, null, OneSecond, OneSecond);

        await timer.DisposeAsync();
        _ = timer.Fire();
        clock.AdvanceBy(TenSeconds);

        await Assert.That(fired).IsEqualTo(0);
    }

    /// <summary>Asynchronous disposal cancels the timer.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposeAsyncCancelsThePendingFiring()
    {
        VirtualClock clock = new(Epoch);
        var fired = 0;
        var timer = clock.CreateTimer(_ => fired++, null, OneSecond, Timeout.InfiniteTimeSpan);

        await timer.DisposeAsync();
        clock.AdvanceBy(TenSeconds);

        await Assert.That(fired).IsEqualTo(0);
    }

    /// <summary>Timers reject a missing callback and negative times other than the infinite time span.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TimersRejectInvalidArguments()
    {
        VirtualClock clock = new(Epoch);
        TimeProvider provider = clock;
        await using var timer =provider.CreateTimer(static _ => { }, null, OneSecond, Timeout.InfiniteTimeSpan);

        await Assert.That(() => provider.CreateTimer(null!, null, OneSecond, Timeout.InfiniteTimeSpan))
            .ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => provider.CreateTimer(static _ => { }, null, -TwoSeconds, Timeout.InfiniteTimeSpan))
            .ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => provider.CreateTimer(static _ => { }, null, OneSecond, -TwoSeconds))
            .ThrowsExactly<ArgumentOutOfRangeException>();
        await Assert.That(() => timer.Change(-TwoSeconds, Timeout.InfiniteTimeSpan))
            .ThrowsExactly<ArgumentOutOfRangeException>();
    }
}
