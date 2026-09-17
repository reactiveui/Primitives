// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests connection establishment for connectable signal state.</summary>
public sealed class ConnectableSignalAsyncHelperTests
{
    /// <summary>Connecting with the state's own disposal token subscribes the source once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task ConnectAsync_WithStateDisposalToken_SubscribesSource()
    {
        var subscribed = 0;
        var source = SignalAsync.Create<int>((_, _) =>
        {
            subscribed++;
            return new ValueTask<IAsyncDisposable>(DisposableAsync.Empty);
        });
        using ConnectableSignalAsyncState<int> state = new(source, Signal.Create<int>());

        await using var connection = await ConnectableSignalAsyncHelper.ConnectAsync(state, state.DisposedCancellationToken);

        await Assert.That(subscribed).IsEqualTo(1);
    }
}
