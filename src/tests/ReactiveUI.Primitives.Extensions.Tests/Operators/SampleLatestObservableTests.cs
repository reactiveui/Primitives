// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for <c>SampleLatest</c> backed by
/// <c>SampleLatestObservable&lt;T&gt;</c> — trigger before any source value,
/// source completion, source error, trigger error, and trigger completion not
/// terminating downstream.</summary>
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
        var source = new Subject<int>();
        var trigger = new Subject<object>();
        var results = new List<int>();
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
        var source = new Subject<int>();
        var trigger = new Subject<object>();
        var results = new List<int>();
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
        var source = new Subject<int>();
        var trigger = new Subject<object>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);
        using var sub = source.SampleLatest(trigger).Subscribe(
            static _ =>
        {
        },
            ex => caught = ex);
        source.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>SampleLatest</c> forwards trigger errors.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSampleLatestTriggerErrors_ThenForwardsError()
    {
        var source = new Subject<int>();
        var trigger = new Subject<object>();
        Exception? caught = null;
        var expected = new InvalidOperationException(TriggerErrorMessage);
        using var sub = source.SampleLatest(trigger).Subscribe(
            static _ =>
        {
        },
            ex => caught = ex);
        trigger.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that source completion is forwarded downstream.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSampleLatestSourceCompletes_ThenForwardsCompletion()
    {
        var source = new Subject<int>();
        var trigger = new Subject<object>();
        var completed = false;
        using var sub = source.SampleLatest(trigger).Subscribe(
            static _ =>
        {
        },
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
        var source = new Subject<int>();
        var trigger = new Subject<object>();
        var results = new List<int>();
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
        var source = new SyncDirectSource<int>();
        var trigger = new SyncDirectSource<object>();
        var values = new List<int>();
        Exception? caught = null;
        var completedCount = 0;
        using var sub = source.SampleLatest(trigger).Subscribe(values.Add, ex => caught = ex, () => completedCount++);

        // Terminate via trigger error first.
        var expected = new InvalidOperationException("trigger");
        trigger.Observer.OnError(expected);
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        source.Observer.OnCompleted();
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(values).IsEmpty();
        await Assert.That(completedCount).IsEqualTo(0);
    }
}
