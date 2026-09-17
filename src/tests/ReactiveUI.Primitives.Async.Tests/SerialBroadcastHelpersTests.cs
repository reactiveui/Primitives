// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests ordering across asynchronous serial observer notifications.</summary>
public sealed class SerialBroadcastHelpersTests
{
    /// <summary>A pending first observer prevents the second observer from receiving the value early.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task BroadcastOnNextAsync_PendingFirstObserver_WaitsBeforeNotifyingSecond()
    {
        var source = Signal.Create<int>();
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> firstValues = [];
        List<int> secondValues = [];
        await using var first = await source.Values.SubscribeAsync(async (value, _) =>
        {
            firstValues.Add(value);
            await release.Task;
        });
        await using var second = await source.Values.SubscribeAsync(secondValues.Add);

        var pending = source.OnNextAsync(1, CancellationToken.None).AsTask();
        await Assert.That(firstValues).IsCollectionEqualTo([1]);
        await Assert.That(secondValues).IsEmpty();
        await Assert.That(pending.IsCompleted).IsFalse();
        release.SetResult();
        await pending;

        await Assert.That(secondValues).IsCollectionEqualTo([1]);
    }
}
