// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests sampling without a source value and source or trigger termination.</summary>
public class SampleLatestObservableTests
{
    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Synthetic error message attached to trigger errors.</summary>
    private const string TriggerErrorMessage = "trigger error";

    /// <summary>Trigger token reused across tests.</summary>
    private static readonly object TriggerToken = new();

    /// <summary>Verifies that a trigger arriving before any source value does not emit.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSampleLatestTriggerBeforeAnyValue_ThenNoEmission()
    {
        Subject<int> source = new();
        Subject<object> trigger = new();
        List<int> results = [];
        using var sub = source.SampleLatest(trigger).Subscribe(results.Add);
        trigger.OnNext(TriggerToken);
        await Assert.That(results).IsEmpty();
    }

    /// <summary>Verifies that <c>SampleLatest</c> emits the latest source value on each trigger.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSampleLatestTriggerAfterValues_ThenEmitsLatest()
    {
        const int First = 1;
        const int Second = 2;
        Subject<int> source = new();
        Subject<object> trigger = new();
        List<int> results = [];
        using var sub = source.SampleLatest(trigger).Subscribe(results.Add);
        source.OnNext(First);
        source.OnNext(Second);
        trigger.OnNext(TriggerToken);
        trigger.OnNext(TriggerToken);
        await Assert.That(results).IsCollectionEqualTo([Second, Second]);
    }

    /// <summary>Verifies that <c>SampleLatest</c> forwards source errors.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSampleLatestSourceErrors_ThenForwardsError()
    {
        Subject<int> source = new();
        Subject<object> trigger = new();
        Exception? caught = null;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var sub = source.SampleLatest(trigger).Subscribe(
            static _ => { },
            ex => caught = ex);
        source.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>SampleLatest</c> forwards trigger errors.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSampleLatestTriggerErrors_ThenForwardsError()
    {
        Subject<int> source = new();
        Subject<object> trigger = new();
        Exception? caught = null;
        InvalidOperationException expected = new(TriggerErrorMessage);
        using var sub = source.SampleLatest(trigger).Subscribe(
            static _ => { },
            ex => caught = ex);
        trigger.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that source completion is forwarded downstream.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSampleLatestSourceCompletes_ThenForwardsCompletion()
    {
        Subject<int> source = new();
        Subject<object> trigger = new();
        var completed = false;
        using var sub = source.SampleLatest(trigger).Subscribe(
            static _ => { },
            () => completed = true);
        source.OnCompleted();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that trigger completion alone does NOT complete the downstream sequence.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSampleLatestTriggerCompletes_ThenDownstreamRemainsOpen()
    {
        const int Value = 7;
        Subject<int> source = new();
        Subject<object> trigger = new();
        List<int> results = [];
        var completed = false;
        using var sub = source.SampleLatest(trigger).Subscribe(results.Add, () => completed = true);
        source.OnNext(Value);
        trigger.OnCompleted();

        // After trigger completion, source values are still tracked. No emission, no termination.
        await Assert.That(completed).IsFalse();
        await Assert.That(results).IsEmpty();
    }

    /// <summary>Verifies that <c>OnNext</c>, <c>OnError</c> and a duplicate <c>OnCompleted</c>
    /// arriving from the source after the combined sequence has already terminated are silently
    /// dropped via the <c>_done</c> guard.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceEventsAfterTerminated_ThenDropped()
    {
        SyncDirectSource<int> source = new();
        SyncDirectSource<object> trigger = new();
        List<int> values = [];
        Exception? caught = null;
        var completedCount = 0;
        using var sub = source.SampleLatest(trigger).Subscribe(values.Add, ex => caught = ex, () => completedCount++);

        // Terminate via trigger error first.
        InvalidOperationException expected = new("trigger");
        trigger.Observer.OnError(expected);
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        source.Observer.OnCompleted();
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(values).IsEmpty();
        await Assert.That(completedCount).IsEqualTo(0);
    }

    /// <summary>Verifies source completion disposes both the source and trigger subscriptions.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceCompletes_ThenDisposesSourceAndTriggerSubscriptions()
    {
        SyncDirectSource<int> source = new();
        SyncDirectSource<object> trigger = new();
        var completed = false;
        using var sub = source.SampleLatest(trigger).Subscribe(static _ => { }, () => completed = true);

        source.Observer.OnCompleted();

        await Assert.That(completed).IsTrue();
        await Assert.That(source.DisposeCount).IsEqualTo(1);
        await Assert.That(trigger.DisposeCount).IsEqualTo(1);
    }

    /// <summary>Verifies a trigger that still fires after the source completed samples nothing.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTriggerFiresAfterSourceCompleted_ThenNoEmission()
    {
        SyncDirectSource<int> source = new();
        SyncDirectSource<object> trigger = new();
        List<int> values = [];
        using var sub = source.SampleLatest(trigger).Subscribe(values.Add);

        source.Observer.OnNext(1);
        trigger.Observer.OnNext(TriggerToken);
        source.Observer.OnCompleted();
        trigger.Observer.OnNext(TriggerToken);

        await Assert.That(values).IsCollectionEqualTo([1]);
    }

    /// <summary>Verifies throwing downstream <c>OnNext</c> and <c>OnCompleted</c> callbacks dispose both upstream subscriptions and rethrow.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDownstreamOnNextOrOnCompletedThrows_ThenDisposesBothAndRethrows()
    {
        SyncDirectSource<int> source1 = new();
        SyncDirectSource<object> trigger1 = new();
        InvalidOperationException nextFailure = new("next");
        using var sub1 = source1.SampleLatest(trigger1).Subscribe(
            ReactiveUI.Primitives.Advanced.Witness.Create<int>(_ => throw nextFailure));
        source1.Observer.OnNext(1);
        _ = Assert.Throws<InvalidOperationException>(() => trigger1.Observer.OnNext(TriggerToken));
        await Assert.That(source1.DisposeCount).IsEqualTo(1);
        await Assert.That(trigger1.DisposeCount).IsEqualTo(1);

        SyncDirectSource<int> source2 = new();
        SyncDirectSource<object> trigger2 = new();
        InvalidOperationException completeFailure = new("complete");
        using var sub2 = source2.SampleLatest(trigger2).Subscribe(
            ReactiveUI.Primitives.Advanced.Witness.Create<int>(static _ => { }, static _ => { }, () => throw completeFailure));
        _ = Assert.Throws<InvalidOperationException>(() => source2.Observer.OnCompleted());
        await Assert.That(source2.DisposeCount).IsEqualTo(1);
        await Assert.That(trigger2.DisposeCount).IsEqualTo(1);
    }
}
