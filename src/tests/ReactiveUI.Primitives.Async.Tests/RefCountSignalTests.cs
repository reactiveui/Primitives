// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests managed connection ownership during reference-counted disposal.</summary>
public sealed class RefCountSignalTests
{
    /// <summary>Finalizer-style disposal leaves managed subscriptions available to their owners.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task Dispose_UnmanagedOnly_PreservesManagedConnection()
    {
        const int IgnoredValue = 2;
        var source = Signal.Create<int>();
        var connected = source.Values.Publish();
        using SignalAsyncExtensions.RefCountSignal<int> signal = new(connected);
        List<int> values = [];
        await using var subscription = await ((IObservableAsync<int>)signal).SubscribeAsync(values.Add);

        signal.Dispose(false);
        await source.OnNextAsync(1, CancellationToken.None);
        await subscription.DisposeAsync();
        await source.OnNextAsync(IgnoredValue, CancellationToken.None);

        await Assert.That(values).IsCollectionEqualTo([1]);
    }

    /// <summary>A first subscriber already completed by the multicast signal does not connect the source.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task SubscribeAsync_FirstSubscriberCompletedDuringSubscribe_DoesNotConnect()
    {
        var subscribed = 0;
        var source = SignalAsync.Create<int>((_, _) =>
        {
            subscribed++;
            return new ValueTask<IAsyncDisposable>(DisposableAsync.Empty);
        });
        var multicast = Signal.Create<int>();
        await multicast.OnCompletedAsync(Result.Success);
        using ConnectableSignalAsync<int> connectable = new(source, multicast);
        using SignalAsyncExtensions.RefCountSignal<int> signal = new(connectable);

        await using var subscription = await ((IObservableAsync<int>)signal).SubscribeAsync(static _ => { });

        await Assert.That(subscribed).IsEqualTo(0);
    }

    /// <summary>A second subscriber shares the connection made for the first.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task SubscribeAsync_SecondSubscriber_SharesExistingConnection()
    {
        var subscribed = 0;
        var source = SignalAsync.Create<int>((_, _) =>
        {
            subscribed++;
            return new ValueTask<IAsyncDisposable>(DisposableAsync.Empty);
        });
        using ConnectableSignalAsync<int> connectable = new(source, Signal.Create<int>());
        using SignalAsyncExtensions.RefCountSignal<int> signal = new(connectable);

        await using var first = await ((IObservableAsync<int>)signal).SubscribeAsync(static _ => { });
        await using var second = await ((IObservableAsync<int>)signal).SubscribeAsync(static _ => { });

        await Assert.That(subscribed).IsEqualTo(1);
    }
}
