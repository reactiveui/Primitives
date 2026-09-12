// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests timestamped notification teardown.</summary>
public sealed class TimeIntervalWitnessTests
{
    /// <summary>A source error is forwarded unchanged and releases its subscription.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task OnError_WhenSubscribed_ThenForwardsTheErrorAndDisposes()
    {
        RecordingWitness<TimeInterval<int>> observer = new();
        using TimeIntervalWitness<int> witness = new(observer, new VirtualClock());
        BooleanDisposable subscription = new();
        InvalidOperationException error = new("source failure");
        witness.SetSubscription(subscription);

        witness.OnError(error);

        await Assert.That(observer.Errors.Count).IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);
        await Assert.That(subscription.IsDisposed).IsTrue();
    }
}
