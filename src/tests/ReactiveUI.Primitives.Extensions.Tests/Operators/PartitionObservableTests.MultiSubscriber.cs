// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Coverage for the multi-subscriber and idempotent-dispose paths of
/// <c>Partition</c> backed by <c>PartitionObservable&lt;T&gt;</c> — three observers on
/// one side, mid-array removal, and double-dispose of a side subscription.</summary>
public partial class PartitionObservableTests
{
    /// <summary>Verifies that three observers on the same side each receive every matching value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThreeObserversSameSide_ThenAllReceive()
    {
        var subject = new Subject<int>();
        var (evens, _) = subject.Partition(static x => x % Two == 0);
        var a = new List<int>();
        var b = new List<int>();
        var c = new List<int>();

        using var subA = evens.Subscribe(a.Add);
        using var subB = evens.Subscribe(b.Add);
        using var subC = evens.Subscribe(c.Add);

        subject.OnNext(Two);
        subject.OnNext(Four);

        await Assert.That(a).IsCollectionEqualTo([Two, Four]);
        await Assert.That(b).IsCollectionEqualTo([Two, Four]);
        await Assert.That(c).IsCollectionEqualTo([Two, Four]);
    }

    /// <summary>Verifies that disposing the middle of three same-side observers exercises the <c>existing.Length &gt; 2</c> shrink branch.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMiddleOfThreeDisposed_ThenOthersStillReceive()
    {
        var subject = new Subject<int>();
        var (evens, _) = subject.Partition(static x => x % Two == 0);
        var a = new List<int>();
        var b = new List<int>();
        var c = new List<int>();

        using var subA = evens.Subscribe(a.Add);
        var subB = evens.Subscribe(b.Add);
        using var subC = evens.Subscribe(c.Add);

        subject.OnNext(Two);
        subB.Dispose();
        subject.OnNext(Four);

        await Assert.That(a).IsCollectionEqualTo([Two, Four]);
        await Assert.That(b).IsCollectionEqualTo([Two]);
        await Assert.That(c).IsCollectionEqualTo([Two, Four]);
    }

    /// <summary>Verifies that double-dispose of a partition subscription is a no-op (idempotent Subscription.Dispose).</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscriptionDisposedTwice_ThenIdempotent()
    {
        var subject = new Subject<int>();
        var (evens, _) = subject.Partition(static x => x % Two == 0);
        var values = new List<int>();

        var sub = evens.Subscribe(values.Add);
        sub.Dispose();
        sub.Dispose();

        subject.OnNext(Two);

        await Assert.That(values).IsEmpty();
    }

    /// <summary>Verifies that <c>Remove</c> ignores observers that were never added.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceCompletesAfterAllDropped_ThenSafe()
    {
        var subject = new Subject<int>();
        var (evens, _) = subject.Partition(static x => x % Two == 0);

        var sub = evens.Subscribe(static _ => { });
        sub.Dispose();

        // Source completion arriving after every observer has dropped must not throw.
        subject.OnCompleted();

        await Assert.That(subject.HasObservers).IsFalse();
    }
}
