// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the run-once teardown shared by witness sinks.</summary>
public sealed class WitnessTeardownTests
{
    /// <summary>The first disposal releases the subscription and a second disposal reports that teardown already ran.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_CalledTwice_ReleasesTheSubscriptionOnce()
    {
        var disposed = 0;
        RecordingDisposable subscription = new();
        IDisposable? cancel = subscription;

        var first = WitnessTeardown.Dispose(ref disposed, ref cancel);
        var second = WitnessTeardown.Dispose(ref disposed, ref cancel);

        await Assert.That(first).IsTrue();
        await Assert.That(second).IsFalse();
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
        await Assert.That(cancel).IsNull();
    }

    /// <summary>A sink torn down before its subscription was assigned still latches.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_WithoutASubscription_Latches()
    {
        var disposed = 0;
        IDisposable? cancel = null;

        var first = WitnessTeardown.Dispose(ref disposed, ref cancel);

        await Assert.That(first).IsTrue();
        await Assert.That(disposed).IsEqualTo(1);
    }
}
