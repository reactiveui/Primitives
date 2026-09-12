// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests predicate bypass, delayed values, termination, and cancellation of pending values.</summary>
public class ThrottleUntilTrueObservableTests
{
    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Throttle window in virtual ticks.</summary>
    private const int ThrottleWindowTicks = 50;

    /// <summary>Virtual ticks to advance to take the clock one tick past the throttle window.</summary>
    private const int AdvancePastWindowTicks = ThrottleWindowTicks + 1;

    /// <summary>Throttle window for tests.</summary>
    private static readonly TimeSpan ThrottleWindow = TimeSpan.FromTicks(ThrottleWindowTicks);

    /// <summary>Verifies that elements matching the predicate emit immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleUntilTruePredicateTrue_ThenEmitsImmediately()
    {
        const int MatchingValue = 1;
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> emitted = [];
        using var sub = Throttled(subject, scheduler, static x => x == MatchingValue).Subscribe(emitted.Add);
        subject.OnNext(MatchingValue);
        await Assert.That(emitted).IsCollectionEqualTo([MatchingValue]);
    }

    /// <summary>Verifies that non-matching elements are held until the throttle window elapses.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleUntilTruePredicateFalse_ThenEmitsAfterWindow()
    {
        const int NonMatchingValue = 99;
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> emitted = [];
        using var sub = Throttled(subject, scheduler, static _ => false).Subscribe(emitted.Add);
        subject.OnNext(NonMatchingValue);
        await Assert.That(emitted).IsEmpty();
        scheduler.AdvanceBy(AdvancePastWindowTicks);
        await Assert.That(emitted).IsCollectionEqualTo([NonMatchingValue]);
    }

    /// <summary>Verifies a later throttled value replaces the pending value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleUntilTrueFastReplacements_ThenLatestWins()
    {
        const int Earlier = 1;
        const int Later = 2;
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> emissions = [];
        using var sub = Throttled(subject, scheduler, static _ => false).Subscribe(emissions.Add);
        subject.OnNext(Earlier);
        subject.OnNext(Later);
        scheduler.AdvanceBy(AdvancePastWindowTicks);
        await Assert.That(emissions).IsCollectionEqualTo([Later]);
    }

    /// <summary>Verifies that source errors are forwarded.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleUntilTrueSourceErrors_ThenForwardsError()
    {
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        Exception? caught = null;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var sub = Throttled(subject, scheduler, static _ => true).Subscribe(
            static _ => { },
            ex => caught = ex);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that source completion is forwarded and post-completion values ignored.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleUntilTrueSourceCompletes_ThenForwardsCompletion()
    {
        const int IgnoredAfterCompletion = 9;
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        var completed = false;
        using var sub = Throttled(subject, scheduler, static _ => true).Subscribe(
            static _ => { },
            () => completed = true);
        subject.OnCompleted();
        subject.OnNext(IgnoredAfterCompletion);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that disposing before a throttled emission fires suppresses it.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleUntilTrueDisposedBeforeFire_ThenNoEmission()
    {
        const int NonMatchingValue = 1;
        VirtualClock scheduler = new();
        Subject<int> subject = new();
        List<int> results = [];
        var sub = Throttled(subject, scheduler, static _ => false).Subscribe(results.Add);
        subject.OnNext(NonMatchingValue);
        sub.Dispose();

        // Moving past the throttle window confirms the cancelled timer never fires.
        scheduler.AdvanceBy(AdvancePastWindowTicks);
        await Assert.That(results).IsEmpty();
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
        using var sub = Throttled(source, scheduler, static _ => true)
            .Subscribe(values.Add, ex => caught = ex, () => completedCount++);
        source.Observer.OnCompleted();
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        source.Observer.OnCompleted();
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsEmpty();
        await Assert.That(caught).IsNull();
    }

    /// <summary>Creates a throttle using the supplied clock and bypass predicate.</summary>
    /// <param name="source">The source sequence.</param>
    /// <param name="scheduler">The virtual clock timing throttled emissions.</param>
    /// <param name="predicate">The bypass predicate.</param>
    /// <returns>The throttled sequence.</returns>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static ThrottleUntilTrueObservable<int> Throttled(
        IObservable<int> source,
        VirtualClock scheduler,
        Func<int, bool> predicate) =>
        new(source, ThrottleWindow, predicate, scheduler);
}
