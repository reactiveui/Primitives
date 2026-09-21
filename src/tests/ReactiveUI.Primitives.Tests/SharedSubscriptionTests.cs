// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests the subscription that stays alive until its handle and every lease are disposed.</summary>
public class SharedSubscriptionTests
{
    /// <summary>Disposing the handle with no lease releases the underlying subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_WithoutLeases_ReleasesTheUnderlyingSubscription()
    {
        RecordingDisposable underlying = new();
        SharedSubscription shared = new();
        shared.Attach(underlying);

        shared.Dispose();

        await Assert.That(underlying.DisposeCount).IsEqualTo(1);
        await Assert.That(shared.IsDisposed).IsTrue();
    }

    /// <summary>Disposing the handle while a lease is held keeps the underlying subscription until the lease is disposed.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_WithLease_KeepsTheUnderlyingSubscriptionUntilTheLeaseIsDisposed()
    {
        RecordingDisposable underlying = new();
        SharedSubscription shared = new();
        shared.Attach(underlying);
        var lease = shared.Acquire();

        shared.Dispose();
        var afterHandle = underlying.DisposeCount;
        lease.Dispose();

        await Assert.That(afterHandle).IsEqualTo(0);
        await Assert.That(underlying.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Disposing a lease while the handle is alive leaves the underlying subscription running.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposeLease_WhileHandleAlive_KeepsTheUnderlyingSubscription()
    {
        RecordingDisposable underlying = new();
        SharedSubscription shared = new();
        shared.Attach(underlying);

        shared.Acquire().Dispose();

        await Assert.That(underlying.DisposeCount).IsEqualTo(0);
    }

    /// <summary>The last of several leases releases the underlying subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposeLease_LastOfSeveral_ReleasesTheUnderlyingSubscription()
    {
        RecordingDisposable underlying = new();
        SharedSubscription shared = new();
        shared.Attach(underlying);
        var first = shared.Acquire();
        var second = shared.Acquire();
        shared.Dispose();

        first.Dispose();
        var afterFirst = underlying.DisposeCount;
        second.Dispose();

        await Assert.That(afterFirst).IsEqualTo(0);
        await Assert.That(underlying.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Disposing a lease twice returns it once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task DisposeLease_Twice_ReturnsTheLeaseOnce()
    {
        RecordingDisposable underlying = new();
        SharedSubscription shared = new();
        shared.Attach(underlying);
        var first = shared.Acquire();
        var second = shared.Acquire();
        shared.Dispose();

        first.Dispose();
        first.Dispose();

        await Assert.That(underlying.DisposeCount).IsEqualTo(0);
        second.Dispose();
        await Assert.That(underlying.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Disposing the handle twice releases the underlying subscription once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_Twice_ReleasesTheUnderlyingSubscriptionOnce()
    {
        RecordingDisposable underlying = new();
        SharedSubscription shared = new();
        shared.Attach(underlying);

        shared.Dispose();
        shared.Dispose();

        await Assert.That(underlying.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Releasing tears the underlying subscription down whatever leases remain and later leases are empty.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Release_WithLeases_ReleasesNowAndLaterLeasesDoNothing()
    {
        RecordingDisposable underlying = new();
        SharedSubscription shared = new();
        shared.Attach(underlying);
        var lease = shared.Acquire();

        shared.Release();
        var afterRelease = underlying.DisposeCount;
        shared.Acquire().Dispose();
        lease.Dispose();
        shared.Dispose();

        await Assert.That(afterRelease).IsEqualTo(1);
        await Assert.That(underlying.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Attaching after release disposes the incoming subscription.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Attach_AfterRelease_DisposesTheIncomingSubscription()
    {
        RecordingDisposable incoming = new();
        SharedSubscription shared = new();
        shared.Release();

        shared.Attach(incoming);

        await Assert.That(incoming.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Attaching a second subscription disposes it and keeps the first.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Attach_WhenOneIsAttached_DisposesTheIncomingSubscription()
    {
        RecordingDisposable first = new();
        RecordingDisposable second = new();
        SharedSubscription shared = new();
        shared.Attach(first);

        shared.Attach(second);

        await Assert.That(first.DisposeCount).IsEqualTo(0);
        await Assert.That(second.DisposeCount).IsEqualTo(1);
    }

    /// <summary>A null subscription is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Attach_Null_ThrowsArgumentNull()
    {
        SharedSubscription shared = new();

        await Assert.That(() => shared.Attach(null!)).ThrowsExactly<ArgumentNullException>();
    }
}
