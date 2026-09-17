// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests dematerialized notification teardown.</summary>
public sealed class UnsparkWitnessTests
{
    /// <summary>An error in the notification source is forwarded and releases its subscription.</summary>
    /// <returns>The test operation.</returns>
    [Test]
    public async Task OnError_WhenSubscribed_ThenForwardsTheErrorAndDisposes()
    {
        RecordingWitness<int> observer = new();
        using UnsparkWitness<int> witness = new(observer);
        BooleanDisposable subscription = new();
        InvalidOperationException error = new("source failure");
        witness.SetSubscription(subscription);

        witness.OnError(error);

        await Assert.That(observer.Errors.Count).IsEqualTo(1);
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);
        await Assert.That(subscription.IsDisposed).IsTrue();
    }
}
