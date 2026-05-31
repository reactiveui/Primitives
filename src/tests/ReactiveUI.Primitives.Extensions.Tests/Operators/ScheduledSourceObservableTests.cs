// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Direct coverage for <c>ScheduledSourceObservable&lt;T&gt;</c>'s
/// no-op terminal handlers and the <c>EmitState</c> action/transform catch block —
/// branches the happy-path scheduler tests don't reach.</summary>
public class ScheduledSourceObservableTests
{
    /// <summary>Sentinel value used by the emission tests.</summary>
    private const int Sentinel = 5;

    /// <summary>Standard scheduler tick window.</summary>
    private const int WindowTicks = 50;

    /// <summary>Advance amount that exceeds the window once.</summary>
    private const int AdvancePastWindowTicks = 60;

    /// <summary>Exercises the intentionally-empty <c>OnError</c> body — source errors
    /// after a delayed-schedule subscribe are silently dropped, matching the original
    /// <c>Observable.Create</c> + <c>Subscribe(Action&lt;T&gt;)</c> semantics.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceErrors_ThenSilentlySwallowed()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        Exception? caught = null;
        var results = new List<int>();

        using var sub = ((IObservable<int>)subject)
            .Schedule(TimeSpan.FromTicks(WindowTicks), scheduler)
            .Subscribe(results.Add, ex => caught = ex, () => { });

        subject.OnError(new InvalidOperationException("dropped"));

        await Assert.That(caught).IsNull();
        await Assert.That(results).IsEmpty();
    }

    /// <summary>Exercises the intentionally-empty <c>OnCompleted</c> body — source
    /// completion after a delayed-schedule subscribe is silently dropped.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceCompletes_ThenSilentlySwallowed()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        var completed = false;
        var results = new List<int>();

        using var sub = ((IObservable<int>)subject)
            .Schedule(TimeSpan.FromTicks(WindowTicks), scheduler)
            .Subscribe(results.Add, () => completed = true);

        subject.OnCompleted();

        await Assert.That(completed).IsFalse();
        await Assert.That(results).IsEmpty();
    }

    /// <summary>Exercises the <c>EmitState.Emit</c> catch block — when the configured
    /// side-effect throws inside the scheduled callback, the exception is forwarded to
    /// the downstream <c>OnError</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScheduledActionThrows_ThenForwardsErrorToDownstream()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException("action-threw");
        Action<int> throwing = _ => throw expected;

        using var sub = ((IObservable<int>)subject)
            .Schedule(TimeSpan.FromTicks(WindowTicks), scheduler, throwing)
            .Subscribe(static _ => { }, ex => caught = ex);

        subject.OnNext(Sentinel);
        scheduler.AdvanceBy(AdvancePastWindowTicks);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Exercises the same catch block via the transform overload — when the
    /// transform throws inside the scheduled callback, the exception flows to
    /// downstream <c>OnError</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenScheduledTransformThrows_ThenForwardsErrorToDownstream()
    {
        var scheduler = new VirtualClock();
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException("transform-threw");
        Func<int, int> throwing = _ => throw expected;

        using var sub = ((IObservable<int>)subject)
            .Schedule(TimeSpan.FromTicks(WindowTicks), scheduler, throwing)
            .Subscribe(static _ => { }, ex => caught = ex);

        subject.OnNext(Sentinel);
        scheduler.AdvanceBy(AdvancePastWindowTicks);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }
}
