// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests the teardown of the take-until notification lifecycle.</summary>
public sealed class TakeUntilLifecycleTests
{
    /// <summary>A throwing cancellation callback still releases the lifecycle before the failure surfaces.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposeAsync_CancellationCallbackThrows_ReleasesAndRethrows()
    {
        CallbackWitnessAsync<int> observer = new(static (_, _) => default);
        TakeUntilLifecycle<int> lifecycle = new(observer);
        Exception expected = new InvalidOperationException("cancellation callback failed");
        await using var registration = lifecycle.DisposeToken.UnsafeRegister(static state => throw (Exception)state!, expected);

        var error = await Assert.That(async () => await lifecycle.DisposeAsync()).ThrowsExactly<AggregateException>();

        await Assert.That(error!.InnerExceptions).IsCollectionEqualTo([expected]);
        await Assert.That(lifecycle.DisposeToken.IsCancellationRequested).IsTrue();
    }
}
