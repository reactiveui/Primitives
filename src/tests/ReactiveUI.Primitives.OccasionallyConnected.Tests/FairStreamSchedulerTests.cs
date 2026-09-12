// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using Microsoft.Extensions.Time.Testing;
using ReactiveUI.Primitives.OccasionallyConnected;

namespace ReactiveUI.Primitives.OccasionallyConnected.Tests;

/// <summary>Tests for <see cref="FairStreamScheduler"/>.</summary>
public sealed partial class FairStreamSchedulerTests
{
    /// <summary>Defines a common stream identity.</summary>
    private const string StreamName = "stream";

    /// <summary>Defines the first stream identity used by tie-break tests.</summary>
    private const string FirstStreamName = "first";

    /// <summary>Defines the second stream identity used by tie-break tests.</summary>
    private const string SecondStreamName = "second";

    /// <summary>Defines the light stream scheduling weight.</summary>
    private const int LightWeight = 1;

    /// <summary>Defines an invalid stream scheduling weight.</summary>
    private const int InvalidWeight = 0;

    /// <summary>Defines the heavy stream scheduling weight.</summary>
    private const int HeavyWeight = 3;

    /// <summary>Defines the hot stream scheduling weight.</summary>
    private const int HotWeight = 20;

    /// <summary>Defines the maximum configured scheduling weight.</summary>
    private const int MaximumWeight = 100;

    /// <summary>Defines the higher peer scheduling weight.</summary>
    private const int HigherWeight = 10;

    /// <summary>Defines the compensated weight used to isolate priority tie-breaks.</summary>
    private const int PriorityTieWeight = 2;

    /// <summary>Defines the lowest priority used by priority-aware tests.</summary>
    private const int LowPriority = -10;

    /// <summary>Defines the normal priority used by most tests.</summary>
    private const int NormalPriority = 0;

    /// <summary>Defines the highest priority used by priority-aware tests.</summary>
    private const int HighPriority = 10;

    /// <summary>Defines a small priority offset used to create equal fair credit.</summary>
    private const int PriorityTieOffset = 1;

    /// <summary>Defines an invalid high priority.</summary>
    private const int InvalidHighPriority = 11;

    /// <summary>Defines an invalid minimum priority for option validation.</summary>
    private const int InvalidPriorityMinimum = 1;

    /// <summary>Defines an invalid maximum priority for option validation.</summary>
    private const int InvalidPriorityMaximum = 0;

    /// <summary>Defines the expected number of weighted scheduling rounds.</summary>
    private const int WeightedSelectionCount = 80;

    /// <summary>Defines the expected heavy-stream selection count.</summary>
    private const int ExpectedHeavySelections = 60;

    /// <summary>Defines the expected light-stream selection count.</summary>
    private const int ExpectedLightSelections = 20;

    /// <summary>Defines the aged-stream wait seconds.</summary>
    private const int AgedWaitSeconds = 30;

    /// <summary>Defines the updated-stream wait seconds.</summary>
    private const int UpdatedWaitSeconds = 40;

    /// <summary>Defines a wait long enough to saturate aging contribution.</summary>
    private const int SaturatedAgingSeconds = 2_000_000;

    /// <summary>Defines a tiny encoded descriptor byte limit.</summary>
    private const int TinyDescriptorLimit = 16;

    /// <summary>Defines enough encoded capacity for a single registered stream with one head.</summary>
    private const int OneStreamCapacityLimit = 80;

    /// <summary>Defines capacity that admits a second registration only after the first head completes.</summary>
    private const int TwoStreamCapacityLimit = 90;

    /// <summary>Defines a single stream.</summary>
    private const int SingleStream = 1;

    /// <summary>Defines two streams.</summary>
    private const int TwoStreams = 2;

    /// <summary>Defines the stream capacity used by concurrent acquisition tests.</summary>
    private const int ConcurrentMaximumStreams = 64;

    /// <summary>Defines the default test stream capacity.</summary>
    private const int DefaultMaximumStreams = 8;

    /// <summary>Defines the default encoded descriptor byte limit.</summary>
    private const int DefaultDescriptorLimit = 4096;

    /// <summary>Defines the number of concurrent test streams.</summary>
    private const int ConcurrentStreamCount = 32;

    /// <summary>Defines a short aging interval used by deterministic tests.</summary>
    private static readonly TimeSpan AgingInterval = TimeSpan.FromSeconds(1);

    /// <summary>Defines the guard timeout for concurrent tests.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies weighted fair scheduling distributes continuously ready stream heads by stream weight.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task TryAcquireDistributesEligibleHeadsByWeight()
    {
        var clock = new FakeTimeProvider();
        var scheduler = CreateScheduler(clock);
        var light = new StreamId("light");
        var heavy = new StreamId("heavy");

        scheduler.Register(new(light, Weight: LightWeight));
        scheduler.Register(new(heavy, Weight: HeavyWeight));
        scheduler.Ready(light, NormalPriority, clock.GetUtcNow());
        scheduler.Ready(heavy, NormalPriority, clock.GetUtcNow());

        var selected = new List<StreamId>();
        for (var index = 0; index < WeightedSelectionCount; index++)
        {
            var acquisition = await AcquireAsync(scheduler);
            selected.Add(acquisition.StreamId);
            scheduler.Complete(acquisition);
            scheduler.Ready(acquisition.StreamId, NormalPriority, clock.GetUtcNow());
        }

        await Assert.That(selected.Count(stream => stream == heavy)).IsEqualTo(ExpectedHeavySelections);
        await Assert.That(selected.Count(stream => stream == light)).IsEqualTo(ExpectedLightSelections);
    }

    /// <summary>Verifies the next head may carry a different priority while stream credit is preserved.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task SuccessiveHeadsUseHeadPriorityWithoutResettingFairCredit()
    {
        var clock = new FakeTimeProvider();
        var scheduler = CreateScheduler(clock, minimumPriority: LowPriority);
        var first = new StreamId(FirstStreamName);
        var second = new StreamId(SecondStreamName);

        scheduler.Register(new(first, Weight: LightWeight));
        scheduler.Register(new(second, Weight: LightWeight));
        scheduler.Ready(first, HighPriority, clock.GetUtcNow());
        scheduler.Ready(second, NormalPriority, clock.GetUtcNow());

        var firstAcquisition = await AcquireAsync(scheduler);
        await Assert.That(firstAcquisition.StreamId).IsEqualTo(first);
        scheduler.Complete(firstAcquisition);
        scheduler.Ready(first, LowPriority, clock.GetUtcNow());

        var secondAcquisition = await AcquireAsync(scheduler);
        await Assert.That(secondAcquisition.StreamId).IsEqualTo(second);
        scheduler.Complete(secondAcquisition);

        var thirdAcquisition = await AcquireAsync(scheduler);
        await Assert.That(thirdAcquisition.StreamId).IsEqualTo(first);
    }

    /// <summary>Verifies an aged head can overtake a fresh higher-weight stream.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task TryAcquireAgesWaitingHeadsAgainstHotStreams()
    {
        var clock = new FakeTimeProvider();
        var scheduler = CreateScheduler(clock);
        var hot = new StreamId("hot");
        var aged = new StreamId("aged");

        scheduler.Register(new(hot, Weight: HotWeight));
        scheduler.Register(new(aged, Weight: LightWeight));
        scheduler.Ready(aged, NormalPriority, clock.GetUtcNow());
        clock.Advance(TimeSpan.FromSeconds(AgedWaitSeconds));
        scheduler.Ready(hot, NormalPriority, clock.GetUtcNow());

        var acquisition = await AcquireAsync(scheduler);
        await Assert.That(acquisition.StreamId).IsEqualTo(aged);
    }

    /// <summary>Verifies updating an old low-priority head preserves its waiting age against a fresh peer.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UpdatePreservesWaitingAgeForChangedPriorityHeads()
    {
        var clock = new FakeTimeProvider();
        var scheduler = CreateScheduler(clock, minimumPriority: LowPriority);
        var low = new StreamId("low");
        var high = new StreamId("high");

        scheduler.Register(new(low, Weight: LightWeight));
        scheduler.Register(new(high, Weight: HigherWeight));
        scheduler.Ready(low, LowPriority, clock.GetUtcNow());
        clock.Advance(TimeSpan.FromSeconds(UpdatedWaitSeconds));
        scheduler.Update(low, NormalPriority, clock.GetUtcNow());
        scheduler.Ready(high, HighPriority, clock.GetUtcNow());

        var acquisition = await AcquireAsync(scheduler);
        await Assert.That(acquisition.StreamId).IsEqualTo(low);
    }

    /// <summary>Verifies pending heads validate priority bounds during ready and update calls.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ReadyAndUpdateValidateHeadPriorityBounds()
    {
        var clock = new FakeTimeProvider();
        var scheduler = CreateScheduler(clock);
        var stream = new StreamId(StreamName);

        scheduler.Register(new(stream, Weight: LightWeight));

        await Assert.That(() => scheduler.Ready(stream, InvalidHighPriority, clock.GetUtcNow()))
            .ThrowsExactly<InvalidOperationException>();
        scheduler.Ready(stream, NormalPriority, clock.GetUtcNow());
        await Assert.That(() => scheduler.Update(stream, InvalidHighPriority, clock.GetUtcNow()))
            .ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies pending head updates are rejected after acquisition until the head completes.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UpdateRejectsInflightHeads()
    {
        var clock = new FakeTimeProvider();
        var scheduler = CreateScheduler(clock);
        var stream = new StreamId(StreamName);

        scheduler.Register(new(stream, Weight: LightWeight));
        scheduler.Ready(stream, NormalPriority, clock.GetUtcNow());
        var acquisition = await AcquireAsync(scheduler);

        await Assert.That(() => scheduler.Update(stream, HighPriority, clock.GetUtcNow())).ThrowsExactly<InvalidOperationException>();
        scheduler.Complete(acquisition);
    }

    /// <summary>Verifies update rejects registered streams without pending heads.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UpdateRejectsRegisteredStreamWithoutHead()
    {
        var clock = new FakeTimeProvider();
        var scheduler = CreateScheduler(clock);
        var stream = new StreamId(StreamName);

        scheduler.Register(new(stream, Weight: LightWeight));

        await Assert.That(() => scheduler.Update(stream, NormalPriority, clock.GetUtcNow())).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a due-blocked stream head cannot be bypassed within its stream.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ReadyRejectsSecondHeadWhileEarlierHeadIsBlockedByDueTime()
    {
        var clock = new FakeTimeProvider();
        var scheduler = CreateScheduler(clock);
        var stream = new StreamId(StreamName);

        scheduler.Register(new(stream, Weight: LightWeight));
        scheduler.Ready(stream, NormalPriority, clock.GetUtcNow().Add(AgingInterval));

        await Assert.That(scheduler.TryAcquire(out _)).IsFalse();
        await Assert.That(() => scheduler.Ready(stream, HighPriority, clock.GetUtcNow())).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies inflight heads are exclusive until completed.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task TryAcquireSkipsInflightHeadsUntilCompletion()
    {
        var clock = new FakeTimeProvider();
        var scheduler = CreateScheduler(clock);
        var stream = new StreamId(StreamName);

        scheduler.Register(new(stream, Weight: LightWeight));
        scheduler.Ready(stream, NormalPriority, clock.GetUtcNow());

        var acquisition = await AcquireAsync(scheduler);
        await Assert.That(acquisition).IsNotNull();
        await Assert.That(scheduler.TryAcquire(out var missing)).IsFalse();
        await Assert.That(missing).IsNull();
        scheduler.Complete(acquisition);

        await Assert.That(scheduler.TryAcquire(out _)).IsFalse();
    }

    /// <summary>Verifies stale acquisition objects cannot complete a re-registered stream head.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task StaleAcquisitionCannotCompleteHeadAfterRemoveAndRegister()
    {
        var clock = new FakeTimeProvider();
        var scheduler = CreateScheduler(clock);
        var stream = new StreamId(StreamName);

        scheduler.Register(new(stream, Weight: LightWeight));
        scheduler.Ready(stream, NormalPriority, clock.GetUtcNow());
        var staleAcquisition = await AcquireAsync(scheduler);
        await Assert.That(scheduler.Remove(stream)).IsTrue();
        scheduler.Register(new(stream, Weight: LightWeight));
        scheduler.Ready(stream, NormalPriority, clock.GetUtcNow());
        var currentAcquisition = await AcquireAsync(scheduler);

        await Assert.That(() => scheduler.Complete(staleAcquisition)).ThrowsExactly<InvalidOperationException>();
        scheduler.Complete(currentAcquisition);

        await Assert.That(scheduler.TryAcquire(out _)).IsFalse();
    }

    /// <summary>Verifies pending head updates replace due time and priority while preserving retained bytes.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task UpdateReplacesPendingHeadMetadata()
    {
        var clock = new FakeTimeProvider();
        var scheduler = CreateScheduler(clock, minimumPriority: LowPriority);
        var stream = new StreamId(StreamName);
        var peer = new StreamId("peer");

        scheduler.Register(new(stream, Weight: LightWeight));
        scheduler.Register(new(peer, Weight: LightWeight));
        var registrationBytes = scheduler.EncodedDescriptorBytes;
        scheduler.Ready(stream, LowPriority, clock.GetUtcNow().Add(AgingInterval));
        var readyBytes = scheduler.EncodedDescriptorBytes;
        scheduler.Update(stream, HighPriority, clock.GetUtcNow());
        await Assert.That(scheduler.EncodedDescriptorBytes).IsEqualTo(readyBytes);
        await Assert.That(readyBytes).IsGreaterThan(registrationBytes);
        scheduler.Ready(peer, NormalPriority, clock.GetUtcNow());

        var acquisition = await AcquireAsync(scheduler);
        await Assert.That(acquisition.StreamId).IsEqualTo(stream);
    }

    /// <summary>Verifies completing and removing streams reclaim retained logical capacity.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task CompleteAndRemoveReclaimDescriptorCapacity()
    {
        var clock = new FakeTimeProvider();
        var scheduler = new FairStreamScheduler(
            new(MaximumStreams: TwoStreams, MaximumEncodedDescriptorBytes: TwoStreamCapacityLimit, AgingInterval: AgingInterval),
            clock);
        var first = new StreamId("first");
        var second = new StreamId("second");

        scheduler.Register(new(first, Weight: LightWeight));
        var firstRegistrationBytes = scheduler.EncodedDescriptorBytes;
        scheduler.Ready(first, NormalPriority, clock.GetUtcNow());
        await Assert.That(() => scheduler.Register(new(second, Weight: LightWeight))).ThrowsExactly<InvalidOperationException>();

        var acquisition = await AcquireAsync(scheduler);
        scheduler.Complete(acquisition);
        scheduler.Register(new(second, Weight: LightWeight));
        var secondRegistrationBytes = scheduler.EncodedDescriptorBytes - firstRegistrationBytes;

        await Assert.That(scheduler.Remove(first)).IsTrue();
        await Assert.That(scheduler.EncodedDescriptorBytes).IsEqualTo(secondRegistrationBytes);
        await Assert.That(scheduler.RegisteredStreamCount).IsEqualTo(1);
    }

    /// <summary>Verifies concurrent acquisitions do not lease the same head more than once.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ConcurrentAcquireLeasesEachHeadOnce()
    {
        var clock = new FakeTimeProvider();
        var scheduler = CreateScheduler(clock, maximumStreams: ConcurrentMaximumStreams);

        for (var index = 0; index < ConcurrentStreamCount; index++)
        {
            var stream = new StreamId($"stream-{index}");
            scheduler.Register(new(stream, Weight: LightWeight));
            scheduler.Ready(stream, NormalPriority, clock.GetUtcNow());
        }

        var start = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var workers = Enumerable.Range(0, ConcurrentStreamCount)
            .Select(_ => Task.Run(async () =>
            {
                await start.Task;
                return scheduler.TryAcquire(out var acquisition) && acquisition is not null ? acquisition.StreamId.Value : string.Empty;
            }))
            .ToArray();

        start.SetResult(true);
        var acquired = await Task.WhenAll(workers).WaitAsync(GuardTimeout);

        await Assert.That(acquired.Where(static value => value.Length > 0).Distinct()).Count().IsEqualTo(ConcurrentStreamCount);
    }

    /// <summary>Verifies invalid options and stream metadata are rejected.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ConstructorAndRegisterValidateConfiguredBounds()
    {
        var clock = new FakeTimeProvider();
        await Assert.That(() => new FairStreamScheduler(new(0, 1, AgingInterval: AgingInterval), clock))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => new FairStreamScheduler(new(1, 0, AgingInterval: AgingInterval), clock))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => new FairStreamScheduler(
                new(
                    SingleStream,
                    SingleStream,
                    MinimumPriority: InvalidPriorityMinimum,
                    MaximumPriority: InvalidPriorityMaximum,
                    AgingInterval: AgingInterval),
                clock))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => new FairStreamScheduler(new(1, 1, AgingInterval: Timeout.InfiniteTimeSpan), clock))
            .ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => new FairStreamScheduler(new(1, 1, MaximumWeight: InvalidWeight, AgingInterval: AgingInterval), clock))
            .ThrowsExactly<InvalidOperationException>();

        var defaultAgingScheduler = new FairStreamScheduler(new(DefaultMaximumStreams, DefaultDescriptorLimit), clock);
        await Assert.That(defaultAgingScheduler.RegisteredStreamCount).IsEqualTo(0);

        var scheduler = CreateScheduler(clock);
        var stream = new StreamId(StreamName);
        var valid = new StreamId("valid");

        await Assert.That(() => scheduler.Register(new(stream, Weight: InvalidWeight))).ThrowsExactly<InvalidOperationException>();
        scheduler.Register(new(valid, Weight: LightWeight));
        await Assert.That(() => scheduler.Register(new(valid, Weight: LightWeight))).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies registration rejects default identities and stream-count overflow.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task RegisterRejectsDefaultStreamIdAndStreamCountOverflow()
    {
        var clock = new FakeTimeProvider();
        var scheduler = new FairStreamScheduler(
            new(MaximumStreams: SingleStream, MaximumEncodedDescriptorBytes: DefaultDescriptorLimit, AgingInterval: AgingInterval),
            clock);
        StreamId missing = default;

        await Assert.That(() => scheduler.Register(new(missing, Weight: LightWeight))).ThrowsExactly<InvalidOperationException>();

        scheduler.Register(new(new(FirstStreamName), Weight: LightWeight));
        await Assert.That(() => scheduler.Register(new(new(SecondStreamName), Weight: LightWeight))).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies operations against unknown streams fail without mutating scheduler state.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task MissingStreamOperationsFailWithoutMutatingState()
    {
        var clock = new FakeTimeProvider();
        var scheduler = CreateScheduler(clock);
        var missing = new StreamId("missing");

        await Assert.That(scheduler.Remove(missing)).IsFalse();
        await Assert.That(() => scheduler.Ready(missing, NormalPriority, clock.GetUtcNow())).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => scheduler.Update(missing, NormalPriority, clock.GetUtcNow())).ThrowsExactly<InvalidOperationException>();
        await Assert.That(() => scheduler.Complete(new(missing))).ThrowsExactly<InvalidOperationException>();
        await Assert.That(scheduler.RegisteredStreamCount).IsEqualTo(0);
        await Assert.That(scheduler.EncodedDescriptorBytes).IsEqualTo(0);
    }

    /// <summary>Verifies stream identifiers are charged by their actual encoded size.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task RegisterChargesActualStreamIdentifierBytes()
    {
        var clock = new FakeTimeProvider();
        var stream = new StreamId("long-stream-identifier");
        var scheduler = new FairStreamScheduler(new(MaximumStreams: SingleStream, MaximumEncodedDescriptorBytes: TinyDescriptorLimit, AgingInterval: AgingInterval), clock);

        await Assert.That(() => scheduler.Register(new(stream, Weight: LightWeight))).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies a head consumes deterministic fixed capacity and releases it accurately.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ReadyUsesFixedHeadCapacityAndCompleteReclaimsIt()
    {
        var clock = new FakeTimeProvider();
        var stream = new StreamId("cafe\u0301");
        var scheduler = new FairStreamScheduler(
            new(MaximumStreams: SingleStream, MaximumEncodedDescriptorBytes: OneStreamCapacityLimit, AgingInterval: AgingInterval),
            clock);

        scheduler.Register(new(stream, Weight: LightWeight));
        var registrationBytes = scheduler.EncodedDescriptorBytes;
        scheduler.Ready(stream, NormalPriority, clock.GetUtcNow());

        await Assert.That(scheduler.EncodedDescriptorBytes).IsGreaterThan(registrationBytes);
        await Assert.That(() => scheduler.Ready(stream, NormalPriority, clock.GetUtcNow())).ThrowsExactly<InvalidOperationException>();
        var acquisition = await AcquireAsync(scheduler);
        scheduler.Complete(acquisition);
        await Assert.That(scheduler.EncodedDescriptorBytes).IsEqualTo(registrationBytes);
    }

    /// <summary>Verifies equal credit uses current head priority for ties.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task EqualCreditTieUsesHeadPriority()
    {
        var clock = new FakeTimeProvider();
        var scheduler = CreateScheduler(clock);
        var first = new StreamId(FirstStreamName);
        var second = new StreamId(SecondStreamName);

        scheduler.Register(new(first, Weight: PriorityTieWeight));
        scheduler.Register(new(second, Weight: LightWeight));
        scheduler.Ready(first, NormalPriority, clock.GetUtcNow());
        scheduler.Ready(second, PriorityTieOffset, clock.GetUtcNow());

        var priorityAcquisition = await AcquireAsync(scheduler);
        await Assert.That(priorityAcquisition.StreamId).IsEqualTo(second);

        var reverseScheduler = CreateScheduler(clock);
        reverseScheduler.Register(new(first, Weight: LightWeight));
        reverseScheduler.Register(new(second, Weight: PriorityTieWeight));
        reverseScheduler.Ready(first, PriorityTieOffset, clock.GetUtcNow());
        reverseScheduler.Ready(second, NormalPriority, clock.GetUtcNow());

        var reverseAcquisition = await AcquireAsync(reverseScheduler);
        await Assert.That(reverseAcquisition.StreamId).IsEqualTo(first);
    }

    /// <summary>Verifies equal priority and equal credit keep the earlier registration first.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task EqualPriorityAndCreditKeepEarlierRegistrationFirst()
    {
        var clock = new FakeTimeProvider();
        var scheduler = CreateScheduler(clock);
        var first = new StreamId(FirstStreamName);
        var second = new StreamId(SecondStreamName);

        scheduler.Register(new(first, Weight: LightWeight));
        scheduler.Register(new(second, Weight: LightWeight));
        scheduler.Ready(first, NormalPriority, clock.GetUtcNow());
        scheduler.Ready(second, NormalPriority, clock.GetUtcNow());

        var acquisition = await AcquireAsync(scheduler);
        await Assert.That(acquisition.StreamId).IsEqualTo(first);
    }

    /// <summary>Verifies extreme priority bounds are validated without overflowing scheduling arithmetic.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ExtremePriorityBoundsDoNotOverflowAcquire()
    {
        var clock = new FakeTimeProvider();
        var scheduler = new FairStreamScheduler(
            new(
                MaximumStreams: TwoStreams,
                MaximumEncodedDescriptorBytes: DefaultDescriptorLimit,
                MinimumPriority: int.MinValue,
                MaximumPriority: int.MaxValue,
                AgingInterval: AgingInterval),
            clock);
        var low = new StreamId("low");
        var high = new StreamId("high");

        scheduler.Register(new(low, Weight: LightWeight));
        scheduler.Register(new(high, Weight: LightWeight));
        scheduler.Ready(low, int.MinValue, clock.GetUtcNow());
        scheduler.Ready(high, int.MaxValue, clock.GetUtcNow());

        var acquisition = await AcquireAsync(scheduler);
        await Assert.That(acquisition.StreamId).IsEqualTo(high);
    }

    /// <summary>Verifies extremely old heads saturate their aging contribution.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task VeryOldHeadsSaturateAgingContribution()
    {
        var start = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var clock = new FakeTimeProvider(start);
        var scheduler = CreateScheduler(clock);
        var aged = new StreamId("aged");
        var fresh = new StreamId("fresh");

        scheduler.Register(new(aged, Weight: LightWeight));
        scheduler.Register(new(fresh, Weight: LightWeight));
        scheduler.Ready(aged, NormalPriority, clock.GetUtcNow());
        clock.Advance(TimeSpan.FromSeconds(SaturatedAgingSeconds));
        scheduler.Ready(fresh, HighPriority, clock.GetUtcNow());

        var acquisition = await AcquireAsync(scheduler);
        await Assert.That(acquisition.StreamId).IsEqualTo(aged);
    }

    /// <summary>Verifies clock boundary reads are outside the scheduler lock and aging arithmetic saturates.</summary>
    /// <returns>A task representing the assertions.</returns>
    [Test]
    public async Task ClockCallbacksRunOutsideSchedulerLockAndBoundaryAgingDoesNotOverflow()
    {
        var stream = new StreamId(StreamName);
        LockReadingTimeProvider? clock = null;
        FairStreamScheduler? scheduler = null;
        clock = new(DateTimeOffset.MaxValue - TimeSpan.FromTicks(1), () => scheduler?.RegisteredStreamCount ?? 0);
        scheduler = CreateScheduler(clock);

        scheduler.Register(new(stream, Weight: LightWeight));
        scheduler.Ready(stream, NormalPriority, DateTimeOffset.MaxValue);

        clock.Advance(TimeSpan.FromTicks(1));

        var acquisition = await AcquireAsync(scheduler);
        await Assert.That(acquisition.StreamId).IsEqualTo(stream);
        await Assert.That(clock.ReadCount).IsGreaterThan(0);
    }

    /// <summary>Creates a scheduler with common test bounds.</summary>
    /// <param name="clock">The deterministic clock.</param>
    /// <param name="maximumStreams">The maximum registered streams.</param>
    /// <param name="minimumPriority">The minimum configured priority.</param>
    /// <param name="maximumPriority">The maximum configured priority.</param>
    /// <returns>A configured fair scheduler.</returns>
    private static FairStreamScheduler CreateScheduler(
        TimeProvider clock,
        int maximumStreams = DefaultMaximumStreams,
        int minimumPriority = NormalPriority,
        int maximumPriority = HighPriority) =>
        new(
            new(
                MaximumStreams: maximumStreams,
                MaximumEncodedDescriptorBytes: DefaultDescriptorLimit,
                MinimumPriority: minimumPriority,
                MaximumPriority: maximumPriority,
                MaximumWeight: MaximumWeight,
                AgingInterval: AgingInterval),
            clock);

    /// <summary>Acquires the next scheduler head and returns the non-null acquisition.</summary>
    /// <param name="scheduler">The scheduler under test.</param>
    /// <returns>The acquired stream head.</returns>
    /// <exception cref="InvalidOperationException">The scheduler reports acquisition without an acquisition object.</exception>
    private static async Task<FairStreamAcquisition> AcquireAsync(FairStreamScheduler scheduler)
    {
        var acquired = scheduler.TryAcquire(out var acquisition);
        await Assert.That(acquired).IsTrue();
        return acquisition ?? throw new InvalidOperationException("The scheduler reported acquisition without returning an acquisition.");
    }

    /// <summary>Provides a clock that calls back into the scheduler while reading time.</summary>
    private sealed class LockReadingTimeProvider : TimeProvider
    {
        /// <summary>Stores the callback invoked before the clock returns.</summary>
        private readonly Func<int> _readSchedulerCount;

        /// <summary>Stores the current timestamp.</summary>
        private DateTimeOffset _now;

        /// <summary>Initializes a new instance of the <see cref="LockReadingTimeProvider"/> class.</summary>
        /// <param name="now">The starting time.</param>
        /// <param name="readSchedulerCount">The callback that reads scheduler state.</param>
        internal LockReadingTimeProvider(DateTimeOffset now, Func<int> readSchedulerCount)
        {
            _now = now;
            _readSchedulerCount = readSchedulerCount;
        }

        /// <summary>Gets the number of scheduler-state reads performed by this clock.</summary>
        internal int ReadCount { get; private set; }

        /// <inheritdoc />
        public override DateTimeOffset GetUtcNow()
        {
            ReadCount += _readSchedulerCount();
            return _now;
        }

        /// <summary>Advances the current time.</summary>
        /// <param name="interval">The interval to add.</param>
        internal void Advance(TimeSpan interval) => _now = _now.Add(interval);
    }
}
