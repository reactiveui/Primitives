// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for the replay signal type.</summary>
public class ReplaySignalTests
{
    /// <summary>Value emitted while checking observer state.</summary>
    private const int ReplayValue = 42;

    /// <summary>Buffer size of two used across replay signal tests.</summary>
    private const int Two = 2;

    /// <summary>Buffer size of three used across replay signal tests.</summary>
    private const int Three = 3;

    /// <summary>The integer constant one.</summary>
    private const int One = 1;

    /// <summary>The integer constant four.</summary>
    private const int Four = 4;

    /// <summary>The integer constant five.</summary>
    private const int Five = 5;

    /// <summary>The integer constant ten.</summary>
    private const int Ten = 10;

    /// <summary>Constructors the argument checking.</summary>
    [Test]
    public void Constructor_ArgumentChecking()
    {
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => CreateAndDispose(() => new(-1)));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => CreateAndDispose(() => new(-1, EmptySequencer.Instance)));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => CreateAndDispose(() => new(-1, TimeSpan.Zero)));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() =>
            CreateAndDispose(() => new(-1, TimeSpan.Zero, EmptySequencer.Instance)));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => CreateAndDispose(() => new(TimeSpan.FromTicks(-1))));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() =>
            CreateAndDispose(() => new(TimeSpan.FromTicks(-1), EmptySequencer.Instance)));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => CreateAndDispose(() => new(0, TimeSpan.FromTicks(-1))));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() =>
            CreateAndDispose(() => new(0, TimeSpan.FromTicks(-1), EmptySequencer.Instance)));
        _ = Assert.Throws<ArgumentNullException>(() => CreateAndDispose(() => new(null!)));
        _ = Assert.Throws<ArgumentNullException>(() => CreateAndDispose(() => new(0, null!)));
        _ = Assert.Throws<ArgumentNullException>(() => CreateAndDispose(() => new(TimeSpan.Zero, null!)));
        _ = Assert.Throws<ArgumentNullException>(() => CreateAndDispose(() => new(0, TimeSpan.Zero, null!)));

        // zero allowed
        CreateAndDispose(() => new(0));
        CreateAndDispose(() => new(TimeSpan.Zero));
        CreateAndDispose(() => new(0, TimeSpan.Zero));
        CreateAndDispose(() => new(0, EmptySequencer.Instance));
        CreateAndDispose(() => new(TimeSpan.Zero, EmptySequencer.Instance));
        CreateAndDispose(() => new(0, TimeSpan.Zero, EmptySequencer.Instance));
        CreateAndDispose(() => new ReplaySignal<int>());
        CreateAndDispose(() => new ReplaySignal<int>(EmptySequencer.Instance));
        CreateAndDispose(() => new ReplaySignal<int>(0));
        CreateAndDispose(() => new ReplaySignal<int>(0, EmptySequencer.Instance));
        CreateAndDispose(() => new ReplaySignal<int>(TimeSpan.Zero));
        CreateAndDispose(() => new ReplaySignal<int>(TimeSpan.Zero, EmptySequencer.Instance));
        CreateAndDispose(() => new ReplaySignal<int>(0, TimeSpan.Zero));
        CreateAndDispose(() => new ReplaySignal<int>(0, TimeSpan.Zero, EmptySequencer.Instance));
    }

    /// <summary>Determines whether this instance has observers.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers()
    {
        await HasObserversImpl(new());
        await HasObserversImpl(new(1));
        await HasObserversImpl(new(Three));
        await HasObserversImpl(new(TimeSpan.FromSeconds(1)));
    }

    /// <summary>Determines whether [has observers dispose1].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_Dispose1()
    {
        await HasObservers_Dispose1Impl(new());
        await HasObservers_Dispose1Impl(new(1));
        await HasObservers_Dispose1Impl(new(Three));
        await HasObservers_Dispose1Impl(new(TimeSpan.FromSeconds(1)));
    }

    /// <summary>Determines whether [has observers dispose2].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_Dispose2()
    {
        await HasObservers_Dispose2Impl(new());
        await HasObservers_Dispose2Impl(new(1));
        await HasObservers_Dispose2Impl(new(Three));
        await HasObservers_Dispose2Impl(new(TimeSpan.FromSeconds(1)));
    }

    /// <summary>Determines whether [has observers dispose3].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_Dispose3()
    {
        await HasObservers_Dispose3Impl(new());
        await HasObservers_Dispose3Impl(new(1));
        await HasObservers_Dispose3Impl(new(Three));
        await HasObservers_Dispose3Impl(new(TimeSpan.FromSeconds(1)));
    }

    /// <summary>Determines whether [has observers on completed].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_OnCompleted()
    {
        await HasObservers_OnCompletedImpl(new());
        await HasObservers_OnCompletedImpl(new(1));
        await HasObservers_OnCompletedImpl(new(Three));
        await HasObservers_OnCompletedImpl(new(TimeSpan.FromSeconds(1)));
    }

    /// <summary>Determines whether [has observers on error].</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task HasObservers_OnError()
    {
        await HasObservers_OnErrorImpl(new());
        await HasObservers_OnErrorImpl(new(1));
        await HasObservers_OnErrorImpl(new(Three));
        await HasObservers_OnErrorImpl(new(TimeSpan.FromSeconds(1)));
    }

    /// <summary>Called when [error argument checking].</summary>
    [Test]
    public void OnError_ArgumentChecking()
    {
        _ = Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>().OnError(null!));
        _ = Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(1).OnError(null!));
        _ = Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(Two).OnError(null!));
        _ = Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(EmptySequencer.Instance).OnError(null!));
    }

    /// <summary>Subscribes the argument checking.</summary>
    [Test]
    public void Subscribe_ArgumentChecking()
    {
        _ = Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>().Subscribe(null!));
        _ = Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(1).Subscribe(null!));
        _ = Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(Two).Subscribe(null!));
        _ = Assert.Throws<ArgumentNullException>(() => new ReplaySignal<int>(EmptySequencer.Instance).Subscribe(null!));
    }

    /// <summary>Verifies subjects, replay, behavior, state, and connectable aliases cover late terminal branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SubjectsReplayBehaviorStateAndConnectableAliasesCoverLateTerminalBranches()
    {
        BehaviorSignal<int> behavior = new(One);
        await Assert.That(behavior.ToString()!.Contains(nameof(BehaviorSignal<>), StringComparison.Ordinal))
            .IsTrue();
        RecordingWitness<int> initial = new();
        using var behaviorSubscription = behavior.Subscribe(initial);
        behavior.OnCompleted();
        behavior.OnCompleted();
        behavior.OnNext(Two);
        RecordingWitness<int> lateCompleted = new();
        behavior.Subscribe(lateCompleted).Dispose();
        int[] expectedInitial = [One];
        await Assert.That(initial.Values.SequenceEqual(expectedInitial)).IsTrue();
        await Assert.That(lateCompleted.Completed).IsEqualTo(1);
        BehaviorSignal<int> behaviorError = new(One);
        behaviorError.OnError(new InvalidOperationException("behavior"));
        behaviorError.OnError(new InvalidOperationException("late"));
        RecordingWitness<int> lateError = new();
        behaviorError.Subscribe(lateError).Dispose();
        await Assert.That(lateError.Errors[0].Message).IsEqualTo("behavior");
        behaviorError.Dispose();
        behaviorError.Dispose();
        await Assert.That(behaviorError.TryGetValue(out _)).IsFalse();
        ReplaySignal<int> replayCompleted = new(
            Two,
            TimeSpan.MaxValue,
            Sequencer.CurrentThread);
        replayCompleted.OnNext(One);
        replayCompleted.OnNext(Two);
        replayCompleted.OnNext(Three);
        replayCompleted.OnCompleted();
        replayCompleted.OnCompleted();
        replayCompleted.OnNext(Four);
        RecordingWitness<int> replayLateCompleted = new();
        replayCompleted.Subscribe(replayLateCompleted).Dispose();
        int[] expectedReplayLateCompleted = [Two, Three];
        await Assert.That(replayLateCompleted.Values.SequenceEqual(expectedReplayLateCompleted)).IsTrue();
        await Assert.That(replayLateCompleted.Completed).IsEqualTo(1);
        ReplaySignal<int> replayError = new(
            1,
            TimeSpan.MaxValue,
            Sequencer.CurrentThread);
        replayError.OnNext(Five);
        replayError.OnError(new InvalidOperationException("replay"));
        replayError.OnError(new InvalidOperationException("late"));
        RecordingWitness<int> replayLateError = new();
        replayError.Subscribe(replayLateError).Dispose();
        int[] expectedReplayLateError = [Five];
        await Assert.That(replayLateError.Values.SequenceEqual(expectedReplayLateError)).IsTrue();
        await Assert.That(replayLateError.Errors[0].Message).IsEqualTo("replay");
        replayError.Dispose();
        replayError.Dispose();
        _ = Assert.Throws<ObjectDisposedException>(() => replayError.Subscribe(new RecordingWitness<int>()));
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        ReplaySignal<int> windowedReplay = new(Ten, TimeSpan.FromTicks(Two), clock);
        windowedReplay.OnNext(One);
        clock.AdvanceBy(TimeSpan.FromTicks(Three));
        windowedReplay.OnNext(Two);
        RecordingWitness<int> windowedLate = new();
        windowedReplay.Subscribe(windowedLate).Dispose();
        int[] expectedWindowedLate = [Two];
        await Assert.That(windowedLate.Values.SequenceEqual(expectedWindowedLate)).IsTrue();
        var shared = Signal.Sequence(One, Three).Share();
        var replayed = Signal.Sequence(One, Three).Replay(Two);
        await Assert.That(shared).IsNotNull();
        await Assert.That(replayed).IsNotNull();
        var state = Assert.Throws<ArgumentNullException>(() => new StateSignal<int>(One).ToReadOnlyState<int>(null!));
        await Assert.That(state.ParamName).IsEqualTo("selector");
    }

    /// <summary>
    /// A new subscriber that races a live <see cref="ReplaySignal{T}.OnNext"/> must receive each value exactly
    /// once: the replayed buffer must not duplicate or reorder a value that is also delivered live.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Subscribe_RacingOnNext_DeliversEachValueOnce()
    {
        await RaceSubscribeAgainstProducer(() => new(1));
        await RaceSubscribeAgainstProducer(() => new(Three));
    }

    /// <summary>
    /// Continuously emits increasing values from one thread while another thread repeatedly subscribes and
    /// disposes, asserting that no subscriber ever receives a value out of order or twice.
    /// </summary>
    /// <param name = "factory">Factory used to create the replay signal under test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task RaceSubscribeAgainstProducer(Func<ReplaySignal<int>> factory)
    {
        const int subscribeAttempts = 50_000;

        using var signal = factory();
        using CancellationTokenSource stop = new();
        var firstFailure = default(OrderingWitness<int>.OutOfOrderDelivery);

        var producer = Task.Run(() =>
        {
            var value = 1;
            while (!stop.IsCancellationRequested)
            {
                signal.OnNext(value++);
            }
        });

        for (var attempt = 0; attempt < subscribeAttempts && firstFailure is null; attempt++)
        {
            OrderingWitness<int> witness = new();
            signal.Subscribe(witness).Dispose();
            firstFailure = witness.OutOfOrder;
        }

        await stop.CancelAsync();
        await producer;

        await Assert.That(firstFailure).IsNull();
    }

    /// <summary>Creates a replay signal and disposes it immediately.</summary>
    /// <param name = "factory">Factory used to create the signal.</param>
    private static void CreateAndDispose(Func<ReplaySignal<int>> factory)
    {
        using var signal = factory();
    }

    /// <summary>Verifies observer state when the source is disposed before subscription disposal.</summary>
    /// <param name = "s">Signal to test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task HasObservers_Dispose1Impl(ReplaySignal<int> s)
    {
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsFalse();
        var d = s.Subscribe(_ => { });
        await Assert.That(s.HasObservers).IsTrue();
        await Assert.That(s.IsDisposed).IsFalse();
        s.Dispose();
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsTrue();
        d.Dispose();
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsTrue();
    }

    /// <summary>Verifies observer state when the subscription is disposed before the source.</summary>
    /// <param name = "s">Signal to test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task HasObservers_Dispose2Impl(ReplaySignal<int> s)
    {
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsFalse();
        var d = s.Subscribe(_ => { });
        await Assert.That(s.HasObservers).IsTrue();
        await Assert.That(s.IsDisposed).IsFalse();
        d.Dispose();
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsFalse();
        s.Dispose();
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsTrue();
    }

    /// <summary>Verifies observer state when the source is disposed without subscribers.</summary>
    /// <param name = "s">Signal to test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task HasObservers_Dispose3Impl(ReplaySignal<int> s)
    {
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsFalse();
        s.Dispose();
        await Assert.That(s.HasObservers).IsFalse();
        await Assert.That(s.IsDisposed).IsTrue();
    }

    /// <summary>Verifies observer state after completion.</summary>
    /// <param name = "s">Signal to test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task HasObservers_OnCompletedImpl(ReplaySignal<int> s)
    {
        await Assert.That(s.HasObservers).IsFalse();
        using var subscription = s.Subscribe(_ => { });
        await Assert.That(s.HasObservers).IsTrue();
        s.OnNext(ReplayValue);
        await Assert.That(s.HasObservers).IsTrue();
        s.OnCompleted();
        await Assert.That(s.HasObservers).IsFalse();
    }

    /// <summary>Verifies observer state after error.</summary>
    /// <param name = "s">Signal to test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task HasObservers_OnErrorImpl(ReplaySignal<int> s)
    {
        await Assert.That(s.HasObservers).IsFalse();
        using var subscription = s.Subscribe(
            _ => { },
            _ => { });
        await Assert.That(s.HasObservers).IsTrue();
        s.OnNext(ReplayValue);
        await Assert.That(s.HasObservers).IsTrue();
        s.OnError(new InvalidOperationException());
        await Assert.That(s.HasObservers).IsFalse();
    }

    /// <summary>Verifies observer state as subscriptions are added and removed.</summary>
    /// <param name = "s">Signal to test.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    private static async Task HasObserversImpl(ReplaySignal<int> s)
    {
        await Assert.That(s.HasObservers).IsFalse();
        var d1 = s.Subscribe(_ => { });
        await Assert.That(s.HasObservers).IsTrue();
        d1.Dispose();
        await Assert.That(s.HasObservers).IsFalse();
        var d2 = s.Subscribe(_ => { });
        await Assert.That(s.HasObservers).IsTrue();
        var d3 = s.Subscribe(_ => { });
        await Assert.That(s.HasObservers).IsTrue();
        d2.Dispose();
        await Assert.That(s.HasObservers).IsTrue();
        d3.Dispose();
        await Assert.That(s.HasObservers).IsFalse();
    }
}
