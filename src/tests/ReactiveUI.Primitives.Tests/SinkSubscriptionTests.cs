// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>Direct tests for the <see cref = "SinkSubscription"/> single-subscription helper.</summary>
public class SinkSubscriptionTests
{
    /// <summary>The first subscription is stored and left untouched.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SetStoresTheFirstSubscription()
    {
        IDisposable? field = null;
        TrackingDisposable first = new();
        SinkSubscription.Set(ref field, first);
        await Assert.That(field!).IsSameReferenceAs(first);
        await Assert.That(first.DisposeCount).IsEqualTo(0);
    }

    /// <summary>Assigning a second subscription disposes it immediately and keeps the first.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SetDisposesASecondSubscriptionAndKeepsTheFirst()
    {
        IDisposable? field = null;
        TrackingDisposable first = new();
        TrackingDisposable second = new();
        SinkSubscription.Set(ref field, first);
        SinkSubscription.Set(ref field, second);
        await Assert.That(field!).IsSameReferenceAs(first);
        await Assert.That(first.DisposeCount).IsEqualTo(0);
        await Assert.That(second.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Disposing releases the held subscription exactly once, even across repeated disposal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task DisposeReleasesTheHeldSubscriptionOnce()
    {
        IDisposable? field = null;
        TrackingDisposable held = new();
        SinkSubscription.Set(ref field, held);
        SinkSubscription.Dispose(ref field);
        SinkSubscription.Dispose(ref field);
        await Assert.That(held.DisposeCount).IsEqualTo(1);
    }

    /// <summary>A subscription assigned after disposal is torn down immediately and never stored.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SetAfterDisposeImmediatelyDisposesTheLateSubscription()
    {
        IDisposable? field = null;
        SinkSubscription.Dispose(ref field);
        TrackingDisposable late = new();
        SinkSubscription.Set(ref field, late);
        await Assert.That(late.DisposeCount).IsEqualTo(1);
        await Assert.That(ReferenceEquals(late, field)).IsFalse();
    }

    /// <summary>A disposable that counts how many times it has been disposed.</summary>
    private sealed class TrackingDisposable : IDisposable
    {
        /// <summary>Gets the number of times <see cref = "Dispose"/> has been called.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
    }
}
