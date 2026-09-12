// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests forwarding of recoverable errors through retry subscriptions.</summary>
public sealed class ReattemptSubscriptionTests
{
    /// <summary>A recoverable error preserves the source connection and subsequent values.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task RelayErrorAsync_RecoverableError_ForwardsErrorAndContinues()
    {
        DirectSource<int> source = new();
        List<int> values = [];
        List<Exception> errors = [];
        Exception expected = new InvalidOperationException("recoverable");
        CallbackWitnessAsync<int> observer = new(
            (value, _) =>
            {
                values.Add(value);
                return default;
            },
            (error, _) =>
            {
                errors.Add(error);
                return default;
            });
        await using ReattemptSubscription<int> subscription = new(source, observer, 1, CancellationToken.None);
        await subscription.SubscribeOnceAsync();

        await source.EmitError(expected);
        await source.EmitNext(1);

        await Assert.That(errors).IsCollectionEqualTo([expected]);
        await Assert.That(values).IsCollectionEqualTo([1]);
    }
}
