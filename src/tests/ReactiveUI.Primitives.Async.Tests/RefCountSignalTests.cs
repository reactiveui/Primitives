// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

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
}
