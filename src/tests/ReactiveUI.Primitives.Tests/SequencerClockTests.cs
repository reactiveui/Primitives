// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Diagnostics;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests <see cref="SequencerClock"/> timestamp scaling, waiting and timer creation.</summary>
public class SequencerClockTests
{
    /// <summary>Timestamp ticks per second of a provider that differs from <see cref="Stopwatch.Frequency"/> and divides evenly into it.</summary>
    private const long MillisecondFrequency = 1000;

    /// <summary>Timestamp ticks per second of a provider whose scale factor is not a whole number.</summary>
    private const long ThirdsFrequency = 3;

    /// <summary>The provider timestamp at which a rescaling clock is created.</summary>
    private const long OriginTimestamp = 5_000_000;

    /// <summary>Elapsed provider ticks used by the rescaling tests.</summary>
    private const long ElapsedMilliseconds = 1500;

    /// <summary>The delay used by the waiting and timer tests.</summary>
    private static readonly TimeSpan OneSecond = TimeSpan.FromSeconds(1);

    /// <summary>A clock rejects a missing provider.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ConstructorRejectsANullProvider() =>
        await Assert.That(static () => new SequencerClock(null!)).ThrowsExactly<ArgumentNullException>();

    /// <summary>A provider counting at <see cref="Stopwatch.Frequency"/> passes its timestamps through unchanged.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ProviderAtStopwatchFrequencyPassesTimestampsThrough()
    {
        SettableTimeProvider provider = new(Stopwatch.Frequency) { Timestamp = OriginTimestamp };
        SequencerClock clock = new(provider);

        await Assert.That(clock.GetTimestamp()).IsEqualTo(OriginTimestamp);

        provider.Timestamp += Stopwatch.Frequency;

        await Assert.That(clock.GetTimestamp()).IsEqualTo(OriginTimestamp + Stopwatch.Frequency);
    }

    /// <summary>A provider with another frequency reports timestamps in stopwatch units counted from the clock's creation.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ProviderWithAnotherFrequencyIsRescaledFromItsCreationTimestamp()
    {
        SettableTimeProvider provider = new(MillisecondFrequency) { Timestamp = OriginTimestamp };
        SequencerClock clock = new(provider);

        await Assert.That(clock.GetTimestamp()).IsEqualTo(0);

        provider.Timestamp += ElapsedMilliseconds;

        await Assert.That(clock.GetTimestamp()).IsEqualTo(Stopwatch.Frequency * ElapsedMilliseconds / MillisecondFrequency);
    }

    /// <summary>A scale factor that is not a whole number truncates the fractional stopwatch unit.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task RescalingTruncatesAFractionalStopwatchUnit()
    {
        SettableTimeProvider provider = new(ThirdsFrequency);
        SequencerClock clock = new(provider);

        provider.Timestamp = 1;

        await Assert.That(clock.GetTimestamp()).IsEqualTo(Stopwatch.Frequency / ThirdsFrequency);
    }

    /// <summary>The remaining time is measured on the provider's clock and never goes negative.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task TimeUntilMeasuresTheRemainingTimeOnTheProvidersClock()
    {
        SettableTimeProvider provider = new(MillisecondFrequency) { Timestamp = OriginTimestamp };
        SequencerClock clock = new(provider);
        var due = clock.GetTimestamp() + Sequencer.ToTimestampDelta(OneSecond);

        await Assert.That(clock.TimeUntil(due)).IsEqualTo(OneSecond);

        provider.Timestamp += MillisecondFrequency;

        await Assert.That(clock.TimeUntil(due)).IsEqualTo(TimeSpan.Zero);
    }

    /// <summary>The current time comes from the provider.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task GetUtcNowReadsTheProvider()
    {
        VirtualClock virtualClock = new(DateTimeOffset.UnixEpoch);
        SequencerClock clock = new(virtualClock);

        virtualClock.AdvanceBy(OneSecond);

        await Assert.That(clock.GetUtcNow()).IsEqualTo(DateTimeOffset.UnixEpoch + OneSecond);
    }

    /// <summary>Timers are created through the provider and fire when the provider's time reaches them.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task CreateTimerUsesTheProvider()
    {
        VirtualClock virtualClock = new();
        SequencerClock clock = new(virtualClock);
        var fired = 0;

        await using var timer = clock.CreateTimer(_ => fired++, null, OneSecond, Timeout.InfiniteTimeSpan);
        virtualClock.AdvanceBy(OneSecond - TimeSpan.FromTicks(1));

        await Assert.That(fired).IsEqualTo(0);

        virtualClock.AdvanceBy(TimeSpan.FromTicks(1));

        await Assert.That(fired).IsEqualTo(1);
    }

    /// <summary>The system clock waits on the calling thread and returns immediately for an empty delay.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task SystemClockWaitReturnsForAnEmptyDelay()
    {
        SequencerClock.Default.Wait(TimeSpan.Zero);

        await Assert.That(SequencerClock.Default.GetTimestamp()).IsGreaterThan(0);
    }

    /// <summary>A wait on another provider blocks until that provider's timer fires.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task WaitBlocksUntilTheProvidersTimerFires()
    {
        VirtualClock virtualClock = new();
        ScaledTimeProvider provider = new(virtualClock, TimeSpan.TicksPerSecond);
        SequencerClock clock = new(provider);

        var waiter = Task.Run(() => clock.Wait(OneSecond));
        await provider.TimerCreated;

        await Assert.That(waiter.IsCompleted).IsFalse();
        await Assert.That(provider.TimerDueTimes.SequenceEqual([OneSecond])).IsTrue();

        virtualClock.AdvanceBy(OneSecond);
        await waiter;

        await Assert.That(waiter.IsCompletedSuccessfully).IsTrue();
    }
}
