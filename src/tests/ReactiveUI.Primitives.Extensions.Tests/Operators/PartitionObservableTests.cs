// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for <c>Partition</c> backed by
/// <c>PartitionObservable&lt;T&gt;</c> — both-sides routing, single-side disposal,
/// error broadcast, completion broadcast, and re-subscription after both sides drop.</summary>
public partial class PartitionObservableTests
{
    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Even-modulus divisor.</summary>
    private const int Two = 2;

    /// <summary>First odd value.</summary>
    private const int One = 1;

    /// <summary>Second odd value.</summary>
    private const int Three = 3;

    /// <summary>Second even value.</summary>
    private const int Four = 4;

    /// <summary>Verifies that elements route to the correct side based on the predicate.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPartitionWithBothSidesSubscribed_ThenRoutesByPredicate()
    {
        Subject<int> subject = new();
        var (evens, odds) = subject.Partition(static x => x % Two == 0);
        List<int> evenResults = [];
        List<int> oddResults = [];
        using var evenSub = evens.Subscribe(evenResults.Add);
        using var oddSub = odds.Subscribe(oddResults.Add);
        subject.OnNext(One);
        subject.OnNext(Two);
        subject.OnNext(Three);
        subject.OnNext(Four);
        await Assert.That(evenResults).IsCollectionEqualTo([Two, Four]);
        await Assert.That(oddResults).IsCollectionEqualTo([One, Three]);
    }

    /// <summary>Verifies that a value with no observer on its side is silently dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPartitionOnlyOneSideSubscribed_ThenOtherSideValuesDropped()
    {
        Subject<int> subject = new();
        var (evens, _) = subject.Partition(static x => x % Two == 0);
        List<int> evenResults = [];
        using var evenSub = evens.Subscribe(evenResults.Add);
        subject.OnNext(One);
        subject.OnNext(Two);
        await Assert.That(evenResults).IsCollectionEqualTo([Two]);
    }

    /// <summary>Verifies that errors are broadcast to both sides.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPartitionSourceErrors_ThenBroadcastsToBothSides()
    {
        Subject<int> subject = new();
        var (evens, odds) = subject.Partition(static x => x % Two == 0);
        Exception? evenError = null;
        Exception? oddError = null;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var evenSub = evens.Subscribe(
            static _ => { },
            ex => evenError = ex);
        using var oddSub = odds.Subscribe(
            static _ => { },
            ex => oddError = ex);
        subject.OnError(expected);
        await Assert.That(evenError).IsSameReferenceAs(expected);
        await Assert.That(oddError).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that completion is broadcast to both sides.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPartitionSourceCompletes_ThenBroadcastsToBothSides()
    {
        Subject<int> subject = new();
        var (evens, odds) = subject.Partition(static x => x % Two == 0);
        var evenCompleted = false;
        var oddCompleted = false;
        using var evenSub = evens.Subscribe(
            static _ => { },
            () => evenCompleted = true);
        using var oddSub = odds.Subscribe(
            static _ => { },
            () => oddCompleted = true);
        subject.OnCompleted();
        await Assert.That(evenCompleted).IsTrue();
        await Assert.That(oddCompleted).IsTrue();
    }

    /// <summary>Verifies that disposing one side stops it from receiving further emissions while the other side keeps receiving.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPartitionOneSideDisposed_ThenOnlyOtherSideReceives()
    {
        Subject<int> subject = new();
        var (evens, odds) = subject.Partition(static x => x % Two == 0);
        List<int> evenResults = [];
        List<int> oddResults = [];
        var evenSub = evens.Subscribe(evenResults.Add);
        using var oddSub = odds.Subscribe(oddResults.Add);
        subject.OnNext(Two);
        evenSub.Dispose();
        subject.OnNext(Four);
        subject.OnNext(Three);
        await Assert.That(evenResults).IsCollectionEqualTo([Two]);
        await Assert.That(oddResults).IsCollectionEqualTo([Three]);
    }

    /// <summary>Verifies that the partition can be resubscribed after all sides drop — the source subscription is re-established.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPartitionResubscribedAfterAllSidesDropped_ThenSourceRebound()
    {
        Subject<int> subject = new();
        var (evens, _) = subject.Partition(static x => x % Two == 0);
        var firstSub = evens.Subscribe(static _ => { });
        firstSub.Dispose();
        List<int> secondResults = [];
        using var secondSub = evens.Subscribe(secondResults.Add);
        subject.OnNext(Two);
        await Assert.That(secondResults).IsCollectionEqualTo([Two]);
    }

    /// <summary>Verifies the mid-array remove path on the false-side observer set — subscribes
    /// three odd-side observers, disposes the middle one, and confirms the remaining two still
    /// see odd values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMiddleFalseSideObserverDisposed_ThenOthersStillReceiveValues()
    {
        Subject<int> subject = new();
        var (_, odds) = subject.Partition(static x => x % Two == 0);
        List<int> firstResults = [];
        List<int> middleResults = [];
        List<int> lastResults = [];
        using var first = odds.Subscribe(firstResults.Add);
        var middle = odds.Subscribe(middleResults.Add);
        using var last = odds.Subscribe(lastResults.Add);
        subject.OnNext(One);
        middle.Dispose();
        subject.OnNext(Three);
        await Assert.That(firstResults).IsCollectionEqualTo([One, Three]);
        await Assert.That(middleResults).IsCollectionEqualTo([One]);
        await Assert.That(lastResults).IsCollectionEqualTo([One, Three]);
    }

    /// <summary>Verifies that disposing a subscription whose parent sink has already been torn down (last subscriber path) is a no-op.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubscriptionDisposedAfterParentSinkTornDown_ThenNoOp()
    {
        Subject<int> subject = new();
        var (evens, _) = subject.Partition(static x => x % Two == 0);
        var first = evens.Subscribe(static _ => { });
        first.Dispose();

        // Subscribe again to create a fresh sink, then dispose the OLD disposable a second time
        // (which now finds _sink == null because the prior tear-down already nulled it).
        using var second = evens.Subscribe(static _ => { });
        first.Dispose();
        List<int> results = [];
        using var third = evens.Subscribe(results.Add);
        subject.OnNext(Two);

        // Third subscriber must receive the value emitted after the stale-subscription
        // dispose, confirming the dispose was a safe no-op.
        await Assert.That(results).IsCollectionEqualTo([Two]);
    }
}
