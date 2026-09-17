// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests ownership of the subscription following leading values.</summary>
public sealed class LeadSubscriptionTests
{
    /// <summary>Repeated disposal releases the upstream subscription once.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task DisposeAsync_Twice_DisposesSourceOnce()
    {
        List<int> disposals = [];
        var source = SignalAsync.Create<int>((_, _) => new(DisposableAsync.Create(disposals, static state =>
        {
            state.Add(1);
            return default;
        })));
        CallbackWitnessAsync<int> observer = new(static (_, _) => default);
        await using LeadSubscription<int> subscription = new(source, [], observer, CancellationToken.None);
        subscription.Start();

        await subscription.DisposeAsync();
        await subscription.DisposeAsync();

        await Assert.That(disposals).Count().IsEqualTo(1);
    }
}
