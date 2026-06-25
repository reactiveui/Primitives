// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Verifies <see cref="ConnectableSignal{T}"/> sharing, replay, and auto-connect contracts.</summary>
public sealed class ConnectableSignalTests
{
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
        var auto = cold.Share().AutoConnect(2);
        List<int> first = [];
        List<int> second = [];
        using var firstSubscription = auto.Subscribe(first.Add);
        source.OnNext(FirstSharedValue);
        using var secondSubscription = auto.Subscribe(second.Add);
        source.OnNext(SecondSharedValue);
        await Assert.That(sourceSubscriptions).IsEqualTo(1);
        await Assert.That(first.SequenceEqual(ExpectedSecondSharedValues[1..])).IsTrue();
        await Assert.That(second.SequenceEqual(ExpectedSecondSharedValues[1..])).IsTrue();
        _ = Assert.Throws<ArgumentNullException>(() => ConnectableSignalExtensions.Multicast(null!, new Signal<int>()));
        _ = Assert.Throws<ArgumentNullException>(() => Signal.Silent<int>().Multicast(null!));
        _ = Assert.Throws<ArgumentNullException>(() => ConnectableSignalExtensions.AutoShare<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(() => ConnectableSignalExtensions.AutoConnect<int>(null!));
        _ = Assert.Throws<ArgumentOutOfRangeException>(() => cold.ShareLive().AutoConnect(-1));
        var replayed = cold.Replay(1, TimeSpan.FromSeconds(1));
        using var connection = replayed.Connect();
        source.OnNext(FirstReplayValue);
        List<int> replayValues = [];
        _ = replayed.Subscribe(replayValues.Add);
        await Assert.That(replayValues.SequenceEqual(ExpectedReplayValues[..1])).IsTrue();
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
        var auto = cold.Publish().AutoConnect(2, connections.Add);
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

    /// <summary>Verifies direct connect handles reuse, terminate, and dispose idempotently.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ConnectableSignalDirectConnectReusesConnectionAndForwardsTerminalError()
    {
        _ = Assert.Throws<ArgumentNullException>(() =>
        {
            ConnectableSignal<int> invalid = new(null!, new Signal<int>());
            GC.KeepAlive(invalid);
        });
        _ = Assert.Throws<ArgumentNullException>(() =>
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
        using var selectedSubscription = ConnectableSignalExtensions
            .Publish<int, int>(cold, shared => shared.Map(static value => value + SecondSharedValue))
            .Subscribe(selectedValues.Add);
        source.OnNext(FirstSharedValue);

        await Assert.That(selectedValues.SequenceEqual([FirstSharedValue + SecondSharedValue])).IsTrue();

        Exception? selectorError = null;
        InvalidOperationException expectedSelectorError = new("selector");
        _ = ConnectableSignalExtensions.Publish<int, int>(cold, _ => throw expectedSelectorError)
            .Subscribe(_ => { }, error => selectorError = error);

        Exception? nullSelectorError = null;
        _ = ConnectableSignalExtensions.Publish<int, int>(cold, _ => null!)
            .Subscribe(_ => { }, error => nullSelectorError = error);

        await Assert.That(selectorError).IsSameReferenceAs(expectedSelectorError);
        await Assert.That(nullSelectorError is InvalidOperationException).IsTrue();

        var replay = cold.Replay();
        using var replayConnection = replay.Connect();
        source.OnNext(FirstReplayValue);
        List<int> replayValues = [];
        _ = replay.Subscribe(replayValues.Add);
        source.OnNext(SecondReplayValue);

        await Assert.That(replayValues.SequenceEqual(ExpectedReplayValues)).IsTrue();

        _ = Assert.Throws<ArgumentNullException>(() => ConnectableSignalExtensions.RefCount<int>(null!));
        _ = Assert.Throws<ArgumentNullException>(() =>
            ConnectableSignalExtensions.Publish<int, int>(null!, static source => source));
        _ = Assert.Throws<ArgumentNullException>(() => ConnectableSignalExtensions.Publish<int, int>(cold, null!));
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
