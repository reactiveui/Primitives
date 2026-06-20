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
        replayed.Subscribe(replayFirst.Add);
        source.OnNext(FirstReplayValue);
        replayed.Subscribe(replaySecond.Add);
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
        Assert.Throws<ArgumentNullException>(() => ConnectableSignalExtensions.Multicast(null!, new Signal<int>()));
        Assert.Throws<ArgumentNullException>(() => Signal.Silent<int>().Multicast(null!));
        Assert.Throws<ArgumentNullException>(() => ConnectableSignalExtensions.AutoShare<int>(null!));
        Assert.Throws<ArgumentNullException>(() => ConnectableSignalExtensions.AutoConnect<int>(null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => cold.ShareLive().AutoConnect(-1));
        var replayed = cold.Replay(1, TimeSpan.FromSeconds(1));
        using var connection = replayed.Connect();
        source.OnNext(FirstReplayValue);
        List<int> replayValues = [];
        replayed.Subscribe(replayValues.Add);
        await Assert.That(replayValues.SequenceEqual(ExpectedReplayValues[..1])).IsTrue();
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
        ConnectableSignalExtensions.Publish<int, int>(cold, _ => throw expectedSelectorError)
            .Subscribe(_ => { }, error => selectorError = error);

        Exception? nullSelectorError = null;
        ConnectableSignalExtensions.Publish<int, int>(cold, _ => null!)
            .Subscribe(_ => { }, error => nullSelectorError = error);

        await Assert.That(selectorError).IsSameReferenceAs(expectedSelectorError);
        await Assert.That(nullSelectorError is InvalidOperationException).IsTrue();

        var replay = cold.Replay();
        using var replayConnection = replay.Connect();
        source.OnNext(FirstReplayValue);
        List<int> replayValues = [];
        replay.Subscribe(replayValues.Add);
        source.OnNext(SecondReplayValue);

        await Assert.That(replayValues.SequenceEqual(ExpectedReplayValues)).IsTrue();

        Assert.Throws<ArgumentNullException>(() => ConnectableSignalExtensions.RefCount<int>(null!));
        Assert.Throws<ArgumentNullException>(() =>
            ConnectableSignalExtensions.Publish<int, int>(null!, static source => source));
        Assert.Throws<ArgumentNullException>(() => ConnectableSignalExtensions.Publish<int, int>(cold, null!));
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

    /// <summary>Verifies disposing a connectable signal disconnects the active source and prevents reconnects.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeDisconnectsActiveSourceAndPreventsReconnect()
    {
        var sourceSubscriptions = 0;
        var sourceDisposals = 0;
        var cold = Signal.Create<int>(_ =>
        {
            sourceSubscriptions++;
            return Scope.Create(() => sourceDisposals++);
        });
        ConnectableSignal<int> connectable = new(cold, new Signal<int>());

        var connection = connectable.Connect();
        await Assert.That(sourceSubscriptions).IsEqualTo(1);

        connectable.Dispose();
        connectable.Dispose();
        connection.Dispose();

        connectable.Connect().Dispose();

        await Assert.That(sourceDisposals).IsEqualTo(1);
        await Assert.That(sourceSubscriptions).IsEqualTo(1);
    }
}
