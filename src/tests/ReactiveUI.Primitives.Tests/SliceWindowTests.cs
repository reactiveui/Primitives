// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests one window of a sliced sequence.</summary>
public class SliceWindowTests
{
    /// <summary>Published values reach every current subscriber, and a completion ends them.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Publish_ReachesEverySubscriberInOrder()
    {
        SliceWindow<string> window = new(new());
        RecordingWitness<string> first = new();
        RecordingWitness<string> second = new();
        using var firstSubscription = window.Subscribe(first);
        using var secondSubscription = window.Subscribe(second);

        window.Publish("a");
        window.Publish("b");
        window.Complete();

        await Assert.That(first.Values.SequenceEqual(["a", "b"])).IsTrue();
        await Assert.That(second.Values.SequenceEqual(["a", "b"])).IsTrue();
        await Assert.That(first.Completed).IsEqualTo(1);
        await Assert.That(second.Completed).IsEqualTo(1);
    }

    /// <summary>A fault reaches every current subscriber.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Fault_ReachesEverySubscriber()
    {
        SliceWindow<string> window = new(new());
        RecordingWitness<string> observer = new();
        using var subscription = window.Subscribe(observer);
        InvalidOperationException error = new("boom");

        window.Fault(error);

        await Assert.That(observer.Errors.Single()).IsSameReferenceAs(error);
    }

    /// <summary>A subscriber that arrives after completion is completed at once and holds no lease.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_AfterComplete_CompletesAtOnce()
    {
        RecordingDisposable underlying = new();
        SharedSubscription owner = new();
        owner.Attach(underlying);
        SliceWindow<string> window = new(owner);
        window.Complete();
        RecordingWitness<string> late = new();

        using var subscription = window.Subscribe(late);
        owner.Dispose();

        await Assert.That(late.Completed).IsEqualTo(1);
        await Assert.That(underlying.DisposeCount).IsEqualTo(1);
    }

    /// <summary>A subscriber that arrives after a fault receives the error at once.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Subscribe_AfterFault_ReceivesTheErrorAtOnce()
    {
        SliceWindow<string> window = new(new());
        InvalidOperationException error = new("boom");
        window.Fault(error);
        RecordingWitness<string> late = new();

        using var subscription = window.Subscribe(late);

        await Assert.That(late.Errors.Single()).IsSameReferenceAs(error);
    }

    /// <summary>A terminated window ignores later notifications.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Publish_AfterTermination_IsIgnored()
    {
        SliceWindow<string> window = new(new());
        RecordingWitness<string> observer = new();
        using var subscription = window.Subscribe(observer);
        window.Complete();

        window.Publish("a");
        window.Fault(new InvalidOperationException("late"));
        window.Complete();

        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(observer.Errors.Count).IsEqualTo(0);
        await Assert.That(observer.Completed).IsEqualTo(1);
    }

    /// <summary>A disposed subscription stops receiving and returns its lease.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Dispose_Subscription_StopsDeliveryAndReturnsTheLease()
    {
        RecordingDisposable underlying = new();
        SharedSubscription owner = new();
        owner.Attach(underlying);
        SliceWindow<string> window = new(owner);
        RecordingWitness<string> observer = new();
        var subscription = window.Subscribe(observer);
        owner.Dispose();
        var whileSubscribed = underlying.DisposeCount;

        subscription.Dispose();
        subscription.Dispose();
        window.Publish("a");

        await Assert.That(whileSubscribed).IsEqualTo(0);
        await Assert.That(observer.Values.Count).IsEqualTo(0);
        await Assert.That(underlying.DisposeCount).IsEqualTo(1);
    }

    /// <summary>A null owner, observer or error is rejected.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task InvalidArguments_ThrowArgumentNull()
    {
        SliceWindow<string> window = new(new());

        await Assert.That(static () => new SliceWindow<string>(null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => window.Subscribe(null!)).ThrowsExactly<ArgumentNullException>();
        await Assert.That(() => window.Fault(null!)).ThrowsExactly<ArgumentNullException>();
    }
}
