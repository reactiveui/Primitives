// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for <c>ThrottleUntilTrue</c> backed by
/// <c>ThrottleUntilTrueObservable&lt;T&gt;</c> — predicate-true bypass, predicate-false
/// throttling, error forwarding, completion forwarding, and dispose-before-fire.</summary>
public class ThrottleUntilTrueObservableTests
{
    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Throttle window in milliseconds for tests.</summary>
    private const int ThrottleWindowMilliseconds = 50;

    /// <summary>Long throttle window in milliseconds used by the dispose-before-fire test.</summary>
    private const int LongThrottleWindowMilliseconds = 500;

    /// <summary>Settle delay in milliseconds used to confirm a throttled emission never fires.</summary>
    private const int SettleDelayMilliseconds = 150;

    /// <summary>Throttle window for tests.</summary>
    private static readonly TimeSpan ThrottleWindow = TimeSpan.FromMilliseconds(ThrottleWindowMilliseconds);

    /// <summary>Verifies that elements matching the predicate emit immediately.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleUntilTruePredicateTrue_ThenEmitsImmediately()
    {
        const int MatchingValue = 1;
        var subject = new Subject<int>();
        var emitted = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = subject.ThrottleUntilTrue(ThrottleWindow, static x => x == MatchingValue).Subscribe(v => emitted.TrySetResult(v));
        subject.OnNext(MatchingValue);
        var got = await emitted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(got).IsEqualTo(MatchingValue);
    }

    /// <summary>Verifies that non-matching elements are throttled but eventually emit.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleUntilTruePredicateFalse_ThenEmitsAfterDelay()
    {
        const int NonMatchingValue = 99;
        var subject = new Subject<int>();
        var emitted = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = subject.ThrottleUntilTrue(ThrottleWindow, static _ => false).Subscribe(v => emitted.TrySetResult(v));
        subject.OnNext(NonMatchingValue);
        var got = await emitted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(got).IsEqualTo(NonMatchingValue);
    }

    /// <summary>Verifies that a later throttled value replaces an earlier still-pending one.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleUntilTrueFastReplacements_ThenLatestWins()
    {
        const int Earlier = 1;
        const int Later = 2;
        var subject = new Subject<int>();
        var emitted = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var sub = subject.ThrottleUntilTrue(ThrottleWindow, static _ => false).Subscribe(v => emitted.TrySetResult(v));
        subject.OnNext(Earlier);
        subject.OnNext(Later);
        var got = await emitted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(got).IsEqualTo(Later);
    }

    /// <summary>Verifies that source errors are forwarded.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenThrottleUntilTrueSourceErrors_ThenForwardsError()
    {
        var subject = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);
        using var sub = subject.ThrottleUntilTrue(ThrottleWindow, static _ => true).Subscribe(
            static _ =>
        {
        },
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
        var subject = new Subject<int>();
        var completed = false;
        using var sub = subject.ThrottleUntilTrue(ThrottleWindow, static _ => true).Subscribe(
            static _ =>
        {
        },
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
        var subject = new Subject<int>();
        var results = new List<int>();
        var sub = subject.ThrottleUntilTrue(TimeSpan.FromMilliseconds(LongThrottleWindowMilliseconds), static _ => false).Subscribe(results.Add);
        subject.OnNext(NonMatchingValue);
        sub.Dispose();

        // Wait past the throttle window to confirm nothing fires.
        await Task.Delay(SettleDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(results).IsEmpty();
    }

    /// <summary>Verifies that <c>OnNext</c>, <c>OnError</c> and a duplicate <c>OnCompleted</c>
    /// arriving after the source has already completed are silently dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenEventsAfterCompleted_ThenDropped()
    {
        var source = new SyncDirectSource<int>();
        var values = new List<int>();
        Exception? caught = null;
        var completedCount = 0;
        using var sub = source.ThrottleUntilTrue(ThrottleWindow, static _ => true).Subscribe(values.Add, ex => caught = ex, () => completedCount++);
        source.Observer.OnCompleted();
        source.Observer.OnNext(1);
        source.Observer.OnError(new InvalidOperationException("late"));
        source.Observer.OnCompleted();
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsEmpty();
        await Assert.That(caught).IsNull();
    }
}
