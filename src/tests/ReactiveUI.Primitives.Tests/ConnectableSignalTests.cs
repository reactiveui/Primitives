// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="ConnectableSignal{T}"/> sharing, replay, and auto-connect contracts.</summary>
public sealed class ConnectableSignalTests
{
    /// <summary>Subscriber count that must be reached before <c>AutoConnect</c> connects to the source.</summary>
    private const int RequiredSubscribers = 2;

    /// <summary>First value observed through a shared signal.</summary>
    private const int FirstSharedValue = 1;

    /// <summary>Second value observed through a shared signal.</summary>
    private const int SecondSharedValue = 2;

    /// <summary>Value emitted after shared subscriptions are disposed.</summary>
    private const int UnobservedSharedValue = 3;

    /// <summary>First value observed through replay.</summary>
    private const int FirstReplayValue = 4;

    /// <summary>Second value observed through replay.</summary>
    private const int SecondReplayValue = 5;

    /// <summary>A replay window wide enough that no value expires while a test runs.</summary>
    private const int ReplayWindowSeconds = 30;

    /// <summary>Expected values for the first shared subscription.</summary>
    private static readonly int[] ExpectedFirstSharedValues = [FirstSharedValue];

    /// <summary>Expected values for the second shared subscription.</summary>
    private static readonly int[] ExpectedSecondSharedValues = [FirstSharedValue, SecondSharedValue];

    /// <summary>Expected replayed values.</summary>
    private static readonly int[] ExpectedReplayValues = [FirstReplayValue, SecondReplayValue];

    /// <summary>Verifies shared and replayed connectable signals control source subscriptions.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConnectableShareAndReplayLiveControlSourceSubscriptions()
    {
        Signal<int> source = new();
        var sourceSubscriptions = 0;
        var cold = Signal.Create<int>(observer =>
        {
            sourceSubscriptions++;
            return source.Subscribe(observer);
        });
        var shared = cold.ShareLatest();
        List<int> first = [];
        List<int> second = [];
        using var firstSubscription = shared.Subscribe(first.Add);
        using var secondSubscription = shared.Subscribe(second.Add);
        source.OnNext(FirstSharedValue);
        firstSubscription.Dispose();
        source.OnNext(SecondSharedValue);
        secondSubscription.Dispose();
        source.OnNext(UnobservedSharedValue);
        await Assert.That(sourceSubscriptions).IsEqualTo(1);
        await Assert.That(first.SequenceEqual(ExpectedFirstSharedValues)).IsTrue();
        await Assert.That(second.SequenceEqual(ExpectedSecondSharedValues)).IsTrue();
        var replayed = cold.ReplayLive(1);
        var replayConnection = replayed.Connect();
        List<int> replayFirst = [];
        List<int> replaySecond = [];
        _ = replayed.Subscribe(replayFirst.Add);
        source.OnNext(FirstReplayValue);
        _ = replayed.Subscribe(replaySecond.Add);
        source.OnNext(SecondReplayValue);
        replayConnection.Dispose();
        await Assert.That(replayFirst.SequenceEqual(ExpectedReplayValues)).IsTrue();
        await Assert.That(replaySecond.SequenceEqual(ExpectedReplayValues)).IsTrue();
    }

    /// <summary>Verifies connectable aliases, auto-connect validation, and replay window overloads.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConnectableAliasesValidateAndConnectAtThreshold()
    {
        Signal<int> source = new();
        var sourceSubscriptions = 0;
        var cold = Signal.Create<int>(observer =>
        {
            sourceSubscriptions++;
            return source.Subscribe(observer);
        });
        var auto = cold.Share().AutoConnect(RequiredSubscribers);
        List<int> first = [];
        List<int> second = [];
        using var firstSubscription = auto.Subscribe(first.Add);
        source.OnNext(FirstSharedValue);
        using var secondSubscription = auto.Subscribe(second.Add);
        source.OnNext(SecondSharedValue);
        await Assert.That(sourceSubscriptions).IsEqualTo(1);
        await Assert.That(first.SequenceEqual(ExpectedSecondSharedValues[1..])).IsTrue();
        await Assert.That(second.SequenceEqual(ExpectedSecondSharedValues[1..])).IsTrue();
        _ = Assert.Throws<ArgumentNullException>(static () => ConnectableSignalExtensions.Multicast(null!, new Signal<int>()));
        _ = Assert.Throws<ArgumentNullException>(static () => Signal.Silent<int>().Multicast(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => ConnectableSignalExtensions.AutoShare<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () => ConnectableSignalExtensions.AutoConnect<int>(null!));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => cold.ShareLive().AutoConnect(-1));
        var replayed = cold.Replay(1, TimeSpan.FromSeconds(1));
        using var connection = replayed.Connect();
        source.OnNext(FirstReplayValue);
        List<int> replayValues = [];
        _ = replayed.Subscribe(replayValues.Add);
        await Assert.That(replayValues.SequenceEqual(ExpectedReplayValues[..1])).IsTrue();
    }

    /// <summary>Multicast routes source values through the supplied hub only once the signal is connected.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task MulticastRoutesSourceValuesThroughTheSuppliedHub()
    {
        Signal<int> source = new();
        Signal<int> hub = new();
        var multicast = source.Multicast(hub);

        List<int> observed = [];
        using var subscription = multicast.Subscribe(observed.Add);

        // No connection yet, so the hub must not see the source at all.
        source.OnNext(UnobservedSharedValue);
        await Assert.That(observed.Count).IsEqualTo(0);

        using var connection = multicast.Connect();
        source.OnNext(FirstSharedValue);
        source.OnNext(SecondSharedValue);

        await Assert.That(observed.SequenceEqual(ExpectedSecondSharedValues)).IsTrue();
    }

    /// <summary>An unbounded replay hub replays every connected value to a late subscriber.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task UnboundedReplayLiveReplaysEveryValueToALateSubscriber()
    {
        Signal<int> source = new();
        var replayed = source.ReplayLive();

        using var connection = replayed.Connect();
        source.OnNext(FirstReplayValue);
        source.OnNext(SecondReplayValue);

        List<int> late = [];
        using var subscription = replayed.Subscribe(late.Add);

        await Assert.That(late.SequenceEqual(ExpectedReplayValues)).IsTrue();
    }

    /// <summary>A windowed replay hub still honours its buffer-size bound.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WindowedReplayLiveHonoursItsBufferSizeBound()
    {
        Signal<int> source = new();
        var replayed = source.ReplayLive(1, TimeSpan.FromSeconds(ReplayWindowSeconds));

        using var connection = replayed.Connect();
        source.OnNext(FirstReplayValue);
        source.OnNext(SecondReplayValue);

        List<int> late = [];
        using var subscription = replayed.Subscribe(late.Add);

        // The window is wide enough to keep both values, so only the buffer size may trim the replay.
        await Assert.That(late.SequenceEqual(ExpectedReplayValues[1..])).IsTrue();
    }

    /// <summary>AutoConnect without a count connects to the source on the very first subscription.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AutoConnectWithoutACountConnectsOnTheFirstSubscription()
    {
        Signal<int> source = new();
        var sourceSubscriptions = 0;
        var cold = Signal.Create<int>(observer =>
        {
            sourceSubscriptions++;
            return source.Subscribe(observer);
        });

        var auto = cold.ShareLive().AutoConnect();
        await Assert.That(sourceSubscriptions).IsEqualTo(0);

        List<int> observed = [];
        using var subscription = auto.Subscribe(observed.Add);

        await Assert.That(sourceSubscriptions).IsEqualTo(1);

        source.OnNext(FirstSharedValue);

        await Assert.That(observed.SequenceEqual(ExpectedFirstSharedValues)).IsTrue();
    }

    /// <summary>Verifies AutoConnect reports the connection disposable once the subscriber threshold is reached.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AutoConnectReportsConnectionDisposableAtThreshold()
    {
        Signal<int> source = new();
        var sourceSubscriptions = 0;
        var sourceDisposals = 0;
        var cold = Signal.Create<int>(observer =>
        {
            sourceSubscriptions++;
            var inner = source.Subscribe(observer);
            return new ActionDisposable(() =>
            {
                sourceDisposals++;
                inner.Dispose();
            });
        });

        List<IDisposable> connections = [];
        var auto = cold.Publish().AutoConnect(RequiredSubscribers, connections.Add);
        List<int> first = [];
        List<int> second = [];
        using var firstSubscription = auto.Subscribe(first.Add);
        source.OnNext(FirstSharedValue);
        using var secondSubscription = auto.Subscribe(second.Add);
        source.OnNext(SecondSharedValue);

        await Assert.That(connections.Count).IsEqualTo(1);
        await Assert.That(sourceSubscriptions).IsEqualTo(1);
        await Assert.That(first.SequenceEqual(ExpectedSecondSharedValues[1..])).IsTrue();
        await Assert.That(second.SequenceEqual(ExpectedSecondSharedValues[1..])).IsTrue();

        connections[0].Dispose();
        source.OnNext(UnobservedSharedValue);

        await Assert.That(sourceDisposals).IsEqualTo(1);
        await Assert.That(first.SequenceEqual(ExpectedSecondSharedValues[1..])).IsTrue();
        await Assert.That(second.SequenceEqual(ExpectedSecondSharedValues[1..])).IsTrue();
        _ = Assert.Throws<ArgumentNullException>(() => cold.Publish().AutoConnect(1, null!));
    }

    /// <summary>Verifies AutoShare disposes the source connection when release happens during Connect().</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AutoShareDisposesConnectionWhenRefcountDropsDuringConnect()
    {
        var sourceSubscriptions = 0;
        var sourceDisposals = 0;
        var cold = Signal.Create<int>(observer =>
        {
            sourceSubscriptions++;
            observer.OnNext(FirstSharedValue);
            return new ActionDisposable(() => sourceDisposals++);
        });

        AutoShareSignal<int> shared = new(cold.Share());

        var reentrantReleaseInvoked = false;

        void OnNext(int _)
        {
            reentrantReleaseInvoked = true;
            using AutoShareSubscription<int> reentrantRelease = new(shared, Scope.Empty);
            reentrantRelease.Dispose();
        }

        using var subscription = shared.Subscribe((Action<int>)OnNext);

        await Assert.That(reentrantReleaseInvoked).IsTrue();
        await Assert.That(sourceSubscriptions).IsEqualTo(1);
        await Assert.That(sourceDisposals).IsEqualTo(1);
    }

    /// <summary>Verifies AutoShare connects on the first subscriber and disconnects when the last one leaves.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AutoShareConnectsOnFirstSubscriberAndDisconnectsOnLast()
    {
        Signal<int> source = new();
        var sourceSubscriptions = 0;
        var sourceDisposals = 0;
        var cold = Signal.Create<int>(observer =>
        {
            sourceSubscriptions++;
            var inner = source.Subscribe(observer);
            return new ActionDisposable(() =>
            {
                sourceDisposals++;
                inner.Dispose();
            });
        });

        var shared = cold.Share().AutoShare();
        List<int> first = [];
        List<int> second = [];

        var firstSubscription = shared.Subscribe(first.Add);
        await Assert.That(sourceSubscriptions).IsEqualTo(1);

        var secondSubscription = shared.Subscribe(second.Add);
        source.OnNext(FirstSharedValue);

        // The single upstream connection feeds every observer.
        await Assert.That(sourceSubscriptions).IsEqualTo(1);

        firstSubscription.Dispose();
        await Assert.That(sourceDisposals).IsEqualTo(0);

        secondSubscription.Dispose();

        // The connection is disposed only once the final subscriber leaves.
        await Assert.That(sourceDisposals).IsEqualTo(1);
        await Assert.That(first.SequenceEqual(ExpectedFirstSharedValues)).IsTrue();
        await Assert.That(second.SequenceEqual(ExpectedFirstSharedValues)).IsTrue();
    }

    /// <summary>Verifies AutoShare reconnects to the source after every subscriber leaves and a new one arrives.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AutoShareReconnectsAfterAllSubscribersLeave()
    {
        const int ExpectedConnections = 2;
        Signal<int> source = new();
        var sourceSubscriptions = 0;
        var sourceDisposals = 0;
        var cold = Signal.Create<int>(observer =>
        {
            sourceSubscriptions++;
            var inner = source.Subscribe(observer);
            return new ActionDisposable(() =>
            {
                sourceDisposals++;
                inner.Dispose();
            });
        });

        var shared = cold.Share().AutoShare();

        shared.Subscribe(static _ => { }).Dispose();
        await Assert.That(sourceSubscriptions).IsEqualTo(1);
        await Assert.That(sourceDisposals).IsEqualTo(1);

        // A fresh subscriber after the count returned to zero forces a new connection.
        using var second = shared.Subscribe(static _ => { });
        await Assert.That(sourceSubscriptions).IsEqualTo(ExpectedConnections);
        await Assert.That(sourceDisposals).IsEqualTo(1);
    }

    /// <summary>Verifies a throwing Connect propagates and unwinds the refcount so a later subscribe reconnects.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AutoShareConnectFailureUnwindsRefcount()
    {
        const int ExpectedSubscribeAttempts = 2;
        var subscribeAttempts = 0;
        var shouldThrow = true;
        InvalidOperationException expected = new("connect");
        var cold = Signal.Create<int>(observer =>
        {
            subscribeAttempts++;
            if (shouldThrow)
            {
                throw expected;
            }

            observer.OnNext(FirstSharedValue);
            return Scope.Empty;
        });

        var shared = cold.Share().AutoShare();

        // Connect runs outside the gate; a synchronous failure surfaces to the caller.
        var thrown = Assert.Throws<InvalidOperationException>(() => shared.Subscribe(static _ => { }));
        await Assert.That(thrown).IsSameReferenceAs(expected);
        await Assert.That(subscribeAttempts).IsEqualTo(1);

        // The failed attempt unwound the count, so the next subscriber reconnects rather than stalling.
        shouldThrow = false;
        List<int> values = [];
        using var recovered = shared.Subscribe(values.Add);
        await Assert.That(subscribeAttempts).IsEqualTo(ExpectedSubscribeAttempts);
        await Assert.That(values.SequenceEqual(ExpectedFirstSharedValues)).IsTrue();
    }

    /// <summary>Verifies AutoShare maintains a single connection under concurrent subscribe and dispose churn.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task AutoShareKeepsSingleConnectionUnderConcurrentChurn()
    {
        const int Workers = 8;
        const int IterationsPerWorker = 200;
        var peakConnections = 0;
        var liveConnections = 0;
        var cold = Signal.Create<int>(observer =>
        {
            var live = Interlocked.Increment(ref liveConnections);
            var peak = Volatile.Read(ref peakConnections);
            while (live > peak && Interlocked.CompareExchange(ref peakConnections, live, peak) != peak)
            {
                peak = Volatile.Read(ref peakConnections);
            }

            observer.OnNext(FirstSharedValue);
            return new ActionDisposable(() => Interlocked.Decrement(ref liveConnections));
        });

        var shared = cold.Share().AutoShare();

        var workers = new Task[Workers];
        for (var worker = 0; worker < Workers; worker++)
        {
            workers[worker] = Task.Run(() =>
            {
                for (var iteration = 0; iteration < IterationsPerWorker; iteration++)
                {
                    shared.Subscribe(static _ => { }).Dispose();
                }
            });
        }

        await Task.WhenAll(workers);

        // Refcount churn must never run two upstream connections at once and must release the last one.
        await Assert.That(peakConnections).IsEqualTo(1);
        await Assert.That(liveConnections).IsEqualTo(0);
    }

    /// <summary>Verifies direct connect handles reuse, terminate, and dispose idempotently.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConnectableSignalDirectConnectReusesConnectionAndForwardsTerminalError()
    {
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            ConnectableSignal<int> invalid = new(null!, new Signal<int>());
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(static () =>
        {
            ConnectableSignal<int> invalid = new(Signal.Silent<int>(), null!);
            GC.KeepAlive(invalid);
        });

        Signal<int> source = new();
        var sourceSubscriptions = 0;
        var sourceDisposals = 0;
        var cold = Signal.Create<int>(observer =>
        {
            sourceSubscriptions++;
            var inner = source.Subscribe(observer);
            return new ActionDisposable(() =>
            {
                sourceDisposals++;
                inner.Dispose();
            });
        });

        ConnectableSignal<int> connectable = new(cold, new Signal<int>());
        RecordingWitness<int> recorded = new();
        using var recordedSubscription = connectable.Subscribe(recorded);

        var firstConnection = connectable.Connect();
        var secondConnection = connectable.Connect();
        await Assert.That(secondConnection).IsSameReferenceAs(firstConnection);
        await Assert.That(sourceSubscriptions).IsEqualTo(1);

        InvalidOperationException expected = new("connectable");
        source.OnError(expected);
        await Assert.That(recorded.Errors.Count).IsEqualTo(1);
        await Assert.That(recorded.Errors[0]).IsSameReferenceAs(expected);

        firstConnection.Dispose();
        firstConnection.Dispose();
        await Assert.That(sourceDisposals).IsEqualTo(1);
        await Assert.That(connectable.Connect()).IsSameReferenceAs(Scope.Empty);
    }

    /// <summary>Verifies Rx connectable aliases share, replay, and route selector failures.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConnectableRxAliasesShareReplayAndRouteSelectorFailures()
    {
        Signal<int> source = new();
        var sourceSubscriptions = 0;
        var sourceDisposals = 0;
        var cold = Signal.Create<int>(observer =>
        {
            sourceSubscriptions++;
            var inner = source.Subscribe(observer);
            return new ActionDisposable(() =>
            {
                sourceDisposals++;
                inner.Dispose();
            });
        });

        List<int> refCountValues = [];
        var refCountSubscription = cold.Publish().RefCount().Subscribe(refCountValues.Add);
        source.OnNext(FirstSharedValue);
        refCountSubscription.Dispose();
        source.OnNext(SecondSharedValue);

        await Assert.That(refCountValues.SequenceEqual(ExpectedFirstSharedValues)).IsTrue();
        await Assert.That(sourceSubscriptions).IsEqualTo(1);
        await Assert.That(sourceDisposals).IsEqualTo(1);

        List<int> selectedValues = [];
        using var selectedSubscription = ConnectableSignalRxNameExtensions
            .Publish<int, int>(cold, static shared => shared.Map(static value => value + SecondSharedValue))
            .Subscribe(selectedValues.Add);
        source.OnNext(FirstSharedValue);

        await Assert.That(selectedValues.SequenceEqual([FirstSharedValue + SecondSharedValue])).IsTrue();

        Exception? selectorError = null;
        InvalidOperationException expectedSelectorError = new("selector");
        _ = ConnectableSignalRxNameExtensions.Publish<int, int>(cold, _ => throw expectedSelectorError)
            .Subscribe(static _ => { }, error => selectorError = error);

        Exception? nullSelectorError = null;
        _ = ConnectableSignalRxNameExtensions.Publish<int, int>(cold, static _ => null!)
            .Subscribe(static _ => { }, error => nullSelectorError = error);

        await Assert.That(selectorError).IsSameReferenceAs(expectedSelectorError);
        await Assert.That(nullSelectorError is InvalidOperationException).IsTrue();

        var replay = cold.Replay();
        using var replayConnection = replay.Connect();
        source.OnNext(FirstReplayValue);
        List<int> replayValues = [];
        _ = replay.Subscribe(replayValues.Add);
        source.OnNext(SecondReplayValue);

        await Assert.That(replayValues.SequenceEqual(ExpectedReplayValues)).IsTrue();

        _ = Assert.Throws<ArgumentNullException>(static () => ConnectableSignalRxNameExtensions.RefCount<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(static () =>
            ConnectableSignalRxNameExtensions.Publish<int, int>(null!, static source => source));
        _ = Assert.Throws<ArgumentNullException>(() => ConnectableSignalRxNameExtensions.Publish<int, int>(cold, null!));
    }

    /// <summary>Verifies <c>RefCount</c> does not reconnect a published source after its hub has completed.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RefCountDoesNotReconnectAfterPublishedSourceCompletes()
    {
        const int ExpectedCompletions = 2;
        var sourceSubscriptions = 0;
        var completions = 0;
        var cold = Signal.Create<int>(observer =>
        {
            sourceSubscriptions++;
            observer.OnCompleted();
            return Scope.Empty;
        });
        var shared = cold.Publish().RefCount();

        shared.Subscribe(static _ => { }, () => completions++).Dispose();
        shared.Subscribe(static _ => { }, () => completions++).Dispose();

        await Assert.That(completions).IsEqualTo(ExpectedCompletions);
        await Assert.That(sourceSubscriptions).IsEqualTo(1);
    }
}
