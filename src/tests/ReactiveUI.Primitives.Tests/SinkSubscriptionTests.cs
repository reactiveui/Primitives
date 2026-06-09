// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>Direct tests for the <see cref="SinkSubscription"/> single-subscription helper.</summary>
public class SinkSubscriptionTests
{
    /// <summary>The first subscription is stored and left untouched.</summary>
    [Test]
    public void SetStoresTheFirstSubscription()
    {
        IDisposable? field = null;
        var first = new TrackingDisposable();

        SinkSubscription.Set(ref field, first);

        Assert.Same(first, field!);
        Assert.Equal(0, first.DisposeCount);
    }

    /// <summary>Assigning a second subscription disposes it immediately and keeps the first.</summary>
    [Test]
    public void SetDisposesASecondSubscriptionAndKeepsTheFirst()
    {
        IDisposable? field = null;
        var first = new TrackingDisposable();
        var second = new TrackingDisposable();

        SinkSubscription.Set(ref field, first);
        SinkSubscription.Set(ref field, second);

        Assert.Same(first, field!);
        Assert.Equal(0, first.DisposeCount);
        Assert.Equal(1, second.DisposeCount);
    }

    /// <summary>Disposing releases the held subscription exactly once, even across repeated disposal.</summary>
    [Test]
    public void DisposeReleasesTheHeldSubscriptionOnce()
    {
        IDisposable? field = null;
        var held = new TrackingDisposable();
        SinkSubscription.Set(ref field, held);

        SinkSubscription.Dispose(ref field);
        SinkSubscription.Dispose(ref field);

        Assert.Equal(1, held.DisposeCount);
    }

    /// <summary>A subscription assigned after disposal is torn down immediately and never stored.</summary>
    [Test]
    public void SetAfterDisposeImmediatelyDisposesTheLateSubscription()
    {
        IDisposable? field = null;
        SinkSubscription.Dispose(ref field);
        var late = new TrackingDisposable();

        SinkSubscription.Set(ref field, late);

        Assert.Equal(1, late.DisposeCount);
        Assert.False(ReferenceEquals(late, field));
    }

    /// <summary>A disposable that counts how many times it has been disposed.</summary>
    private sealed class TrackingDisposable : IDisposable
    {
        /// <summary>Gets the number of times <see cref="Dispose"/> has been called.</summary>
        public int DisposeCount { get; private set; }

        /// <inheritdoc/>
        public void Dispose() => DisposeCount++;
    }
}
