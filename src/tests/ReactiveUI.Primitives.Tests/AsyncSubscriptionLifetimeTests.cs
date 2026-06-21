// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="AsyncSubscriptionLifetime"/>.</summary>
public sealed class AsyncSubscriptionLifetimeTests
{
    /// <summary>Verifies disposal before completion cancels once and disposes the assigned subscription.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeBeforeCompletionCancelsAndDisposesSubscriptionOnce()
    {
        AsyncSubscriptionLifetime lifetime = new();
        var token = lifetime.Token;
        RecordingDisposable subscription = new();
        lifetime.SetSubscription(subscription);

        lifetime.Dispose();
        lifetime.Dispose();

        await Assert.That(token.IsCancellationRequested).IsTrue();
        await Assert.That(lifetime.IsCancellationRequested).IsTrue();
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies a subscription assigned after disposal is disposed immediately.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SetSubscriptionAfterDisposalDisposesAssignedSubscription()
    {
        AsyncSubscriptionLifetime lifetime = new();
        RecordingDisposable subscription = new();

        lifetime.Dispose();
        lifetime.Complete();
        lifetime.SetSubscription(subscription);

        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies completion releases cancellation without requesting cancellation.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task CompleteBeforeDisposeDisposesSubscriptionWithoutCancellation()
    {
        AsyncSubscriptionLifetime lifetime = new();
        var token = lifetime.Token;
        RecordingDisposable subscription = new();
        lifetime.SetSubscription(subscription);

        lifetime.Complete();
        lifetime.Dispose();

        await Assert.That(token.IsCancellationRequested).IsFalse();
        await Assert.That(lifetime.IsCancellationRequested).IsFalse();
        await Assert.That(subscription.DisposeCount).IsEqualTo(1);
    }
}
