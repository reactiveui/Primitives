// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Verifies the virtual-time sequencers: the stateful scheduling overloads, the guards that stop the clock being
/// re-entered while it is running, and a sequencer built on a clock type of the caller's choosing.
/// </summary>
public partial class SequencerTests
{
    /// <summary>Clock value a virtual sequencer starts at.</summary>
    private const long InitialVirtualClock = 0;

    /// <summary>Relative virtual time used when advancing a tick-based clock.</summary>
    private const long VirtualDelay = 2;

    /// <summary>Absolute virtual clock value a scheduled item is due at.</summary>
    private const long VirtualDueClock = 5;

    /// <summary>Name of the item scheduled at the clock's current value.</summary>
    private const string AtClockName = "at-clock";

    /// <summary>Name of the item scheduled after a relative due time.</summary>
    private const string RelativeName = "relative";

    /// <summary>Name of the item scheduled at an absolute due time.</summary>
    private const string AbsoluteName = "absolute";

    /// <summary>The order the virtual clock's stateful overloads must run in.</summary>
    private static readonly string[] VirtualScheduleOrder = [AtClockName, RelativeName, AbsoluteName];

    /// <summary>The single name recorded by a sequencer that only has absolute work queued.</summary>
    private static readonly string[] AbsoluteOnly = [AbsoluteName];

    /// <summary>Verifies the virtual clock's stateful overloads queue work at the current, relative, and absolute due times.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task VirtualClockStatefulOverloadsQueueWorkInDueOrder()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        List<string> ran = [];
        await Assert.That(clock.IsEnabled).IsFalse();

        using var atClock = clock.Schedule(AtClockName, (_, state) => Record(ran, state));
        using var relative = clock.Schedule(
            RelativeName,
            TimeSpan.FromTicks(VirtualDelay),
            (_, state) => Record(ran, state));
        using var absolute = clock.Schedule(
            AbsoluteName,
            DateTimeOffset.UnixEpoch.AddTicks(VirtualDueClock),
            (_, state) => Record(ran, state));

        clock.Start();

        await Assert.That(ran.SequenceEqual(VirtualScheduleOrder)).IsTrue();
        await Assert.That(clock.IsEnabled).IsFalse();
    }

    /// <summary>Verifies starting a virtual clock from inside running work is ignored instead of re-entering the queue.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task VirtualClockStartIsIgnoredWhileWorkIsAlreadyRunning()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        var ran = 0;
        var enabledDuringRun = false;

        using var item = clock.Schedule(One, (_, _) =>
        {
            ran++;
            enabledDuringRun = clock.IsEnabled;
            clock.Start();
            return EmptyDisposable.Instance;
        });

        clock.Start();

        await Assert.That(enabledDuringRun).IsTrue();
        await Assert.That(ran).IsEqualTo(1);
    }

    /// <summary>Verifies a caller-supplied clock type reports its own time and refuses to be moved backwards.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task VirtualTimeSequencerReportsItsClockAndRejectsNegativeSleep()
    {
        var sequencer = CreateTickSequencer();

        await Assert.That(sequencer.Clock).IsEqualTo(InitialVirtualClock);
        await Assert.That(sequencer.IsEnabled).IsFalse();
        await Assert.That(sequencer.Now).IsEqualTo(DateTimeOffset.UnixEpoch);
        await Assert.That(sequencer.Timestamp).IsEqualTo(DateTimeOffset.UnixEpoch.UtcTicks);

        _ = Assert.Throws<ArgumentOutOfRangeException>(() => sequencer.Sleep(NegativeOne));

        sequencer.Sleep(VirtualDelay);

        await Assert.That(sequencer.Clock).IsEqualTo(VirtualDelay);
    }

    /// <summary>Verifies a caller-supplied clock type runs both queued work items and stateful work when started.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task VirtualTimeSequencerRunsWorkItemsAndStatefulWorkOnStart()
    {
        var sequencer = CreateTickSequencer();
        CountingWorkItem work = new();
        List<string> ran = [];

        sequencer.Schedule(work);
        sequencer.Schedule(work, sequencer.Timestamp);
        using var absolute = sequencer.ScheduleAbsolute(
            AbsoluteName,
            VirtualDueClock,
            (_, state) => Record(ran, state));

        sequencer.Start();

        await Assert.That(work.ExecuteCount).IsEqualTo(Two);
        await Assert.That(ran.SequenceEqual(AbsoluteOnly)).IsTrue();
        await Assert.That(sequencer.Clock).IsEqualTo(VirtualDueClock);

        sequencer.Stop();
        await Assert.That(sequencer.IsEnabled).IsFalse();
    }

    /// <summary>Verifies a caller-supplied clock type exposes itself as the stopwatch provider and ticks its stopwatch.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task VirtualTimeSequencerExposesItsStopwatchProvider()
    {
        var sequencer = CreateTickSequencer();
        var provider = (IServiceProvider)sequencer;

        await Assert.That(provider.GetService(typeof(IStopwatchProvider))!).IsSameReferenceAs(sequencer);
        await Assert.That(provider.GetService(typeof(string))).IsNull();

        var stopwatch = sequencer.StartStopwatch();
        sequencer.Sleep(VirtualDelay);

        await Assert.That(stopwatch.Elapsed).IsEqualTo(TimeSpan.FromTicks(VirtualDelay));
    }

    /// <summary>
    /// Creates a virtual-time sequencer whose clock is a raw tick count. Unlike <see cref="VirtualClock"/> it does not
    /// normalize a negative relative time away, so the sequencer's own arithmetic delegates and guards are exercised.
    /// </summary>
    /// <returns>The sequencer.</returns>
    private static VirtualTimeSequencer<long, long> CreateTickSequencer() =>
        new(
            InitialVirtualClock,
            Comparer<long>.Default,
            static (absolute, relative) => absolute + relative,
            DateTimeOffset.UnixEpoch.AddTicks,
            static relative => relative.Ticks);

    /// <summary>Records the name of a scheduled item and returns the empty disposable the sequencer expects.</summary>
    /// <param name="ran">The names recorded so far, in run order.</param>
    /// <param name="name">The name of the item that just ran.</param>
    /// <returns>The empty disposable.</returns>
    private static EmptyDisposable Record(List<string> ran, string name)
    {
        ran.Add(name);
        return EmptyDisposable.Instance;
    }
}
