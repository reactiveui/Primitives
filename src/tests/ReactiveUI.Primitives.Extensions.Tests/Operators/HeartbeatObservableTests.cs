// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for <c>Heartbeat</c> backed by
/// <c>HeartbeatObservable&lt;T&gt;</c> — heartbeat-on-quiet, error/completion
/// forwarding, and post-terminal timer suppression.</summary>
public class HeartbeatObservableTests
{
    /// <summary>Heartbeat period for the scheduler-driven tests.</summary>
    private const int HeartbeatTicks = 100;

    /// <summary>Tick advance large enough to fire several heartbeats.</summary>
    private const int LargeAdvanceTicks = 500;

    /// <summary>Message attached to synthetic source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Verifies that the heartbeat scheduler injects heartbeats when the source is quiet.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHeartbeatSourceQuiet_ThenEmitsHeartbeats()
    {
        const int ModestAdvanceTicks = 350;
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        var heartbeats = 0;
        using var sub = subject.Heartbeat(TimeSpan.FromTicks(HeartbeatTicks), scheduler)
            .Subscribe(hb => heartbeats += hb.IsHeartbeat ? 1 : 0);
        scheduler.AdvanceBy(ModestAdvanceTicks);
        await Assert.That(heartbeats).IsGreaterThanOrEqualTo(1);
    }

    /// <summary>Verifies that <c>Heartbeat</c> forwards source errors and stops the timer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHeartbeatSourceErrors_ThenForwardsErrorAndStopsTimer()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        Exception? caught = null;
        var postErrorHeartbeats = 0;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var sub = subject.Heartbeat(TimeSpan.FromTicks(HeartbeatTicks), scheduler)
            .Subscribe(hb => postErrorHeartbeats += hb.IsHeartbeat && caught is not null ? 1 : 0, ex => caught = ex);
        subject.OnError(expected);
        scheduler.AdvanceBy(LargeAdvanceTicks);
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(postErrorHeartbeats).IsEqualTo(0);
    }

    /// <summary>Verifies that <c>Heartbeat</c> forwards completion and stops the timer.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHeartbeatSourceCompletes_ThenForwardsCompletionAndStopsTimer()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        var completed = false;
        var postCompletionHeartbeats = 0;
        using var sub = subject.Heartbeat(TimeSpan.FromTicks(HeartbeatTicks), scheduler)
            .Subscribe(hb => postCompletionHeartbeats += hb.IsHeartbeat && completed ? 1 : 0, () => completed = true);
        subject.OnCompleted();
        scheduler.AdvanceBy(LargeAdvanceTicks);
        await Assert.That(completed).IsTrue();
        await Assert.That(postCompletionHeartbeats).IsEqualTo(0);
    }

    /// <summary>Verifies that a source emission wraps the value as a non-heartbeat update.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHeartbeatSourceEmits_ThenForwardsValueUpdate()
    {
        const int Value = 42;
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> updates = [];
        using var sub = subject.Heartbeat(TimeSpan.FromTicks(HeartbeatTicks), scheduler).Subscribe(hb =>
        {
            if (hb.IsHeartbeat)
            {
                return;
            }

            updates.Add(hb.Update);
        });
        subject.OnNext(Value);
        await Assert.That(updates).IsCollectionEqualTo([Value]);
    }

    /// <summary>Verifies that <c>OnNext</c>, <c>OnError</c> and a duplicate <c>OnCompleted</c>
    /// arriving after the source has already completed are silently dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenEventsAfterCompleted_ThenDropped()
    {
        VirtualClock scheduler = new();
        SyncDirectSource<int> source = new();
        List<int> values = [];
        Exception? caught = null;
        var completedCount = 0;
        using var sub = source.Heartbeat(TimeSpan.FromTicks(HeartbeatTicks), scheduler).Subscribe(
            hb =>
            {
                if (hb.IsHeartbeat)
                {
                    return;
                }

                values.Add(hb.Update);
            },
            ex => caught = ex,
            () => completedCount++);
        source.Observer.OnCompleted();
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        source.Observer.OnCompleted();
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsEmpty();
        await Assert.That(caught).IsNull();
    }
}
