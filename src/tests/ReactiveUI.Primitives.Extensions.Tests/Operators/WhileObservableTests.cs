// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for the <c>While</c> operator backed by
/// <c>WhileObservable</c> — inline iteration, scheduler dispatch, predicate-throws,
/// action-throws, and dispose-during-iteration paths.</summary>
public class WhileObservableTests
{
    /// <summary>Synthetic error message attached to predicate failures.</summary>
    private const string PredicateFailedMessage = "predicate failed";

    /// <summary>Synthetic error message attached to action failures.</summary>
    private const string ActionFailedMessage = "action failed";

    /// <summary>Number of inline iterations to run.</summary>
    private const int IterationCount = 3;

    /// <summary>Settle delay in milliseconds used to confirm a disposed loop stops ticking.</summary>
    private const int SettleDelayMilliseconds = 50;

    /// <summary>Maximum tolerated extra iterations after Dispose() returns.</summary>
    private const int MaxStragglerIterations = 10;

    /// <summary>Verifies that the inline form runs until the predicate returns <c>false</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhileInline_ThenRunsUntilPredicateFalseAndCompletes()
    {
        var remaining = IterationCount;
        var emitted = 0;
        var completed = false;

        using var sub = ReactiveExtensions.While(() => remaining > 0, () => remaining--)
            .Subscribe(_ => emitted++, () => completed = true);

        await Assert.That(emitted).IsEqualTo(IterationCount);
        await Assert.That(completed).IsTrue();
        await Assert.That(remaining).IsEqualTo(0);
    }

    /// <summary>Verifies that the scheduler form dispatches every iteration via the scheduler.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhileWithScheduler_ThenRunsUntilPredicateFalse()
    {
        var remaining = IterationCount;
        var completed = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var emitted = 0;

        using var sub = ReactiveExtensions.While(() => remaining > 0, () => remaining--, TaskPoolSequencer.Default)
            .Subscribe(_ => emitted++, () => completed.TrySetResult(emitted));

        var final = await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(final).IsEqualTo(IterationCount);
    }

    /// <summary>Verifies that an exception thrown by the predicate is forwarded.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhilePredicateThrows_ThenForwardsError()
    {
        Exception? caught = null;
        var expected = new InvalidOperationException(PredicateFailedMessage);

        using var sub = ReactiveExtensions.While(() => throw expected, static () => { })
            .Subscribe(static _ => { }, ex => caught = ex);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that an exception thrown by the action is forwarded.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhileActionThrows_ThenForwardsError()
    {
        Exception? caught = null;
        var expected = new InvalidOperationException(ActionFailedMessage);

        using var sub = ReactiveExtensions.While(static () => true, () => throw expected)
            .Subscribe(static _ => { }, ex => caught = ex);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that disposing the scheduled loop stops further iterations.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhileScheduledThenDisposed_ThenIterationStops()
    {
        var ran = 0;
        var gate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        var sub = ReactiveExtensions.While(
                static () => true,
                () => SignalFirstIteration(ref ran, gate),
                TaskPoolSequencer.Default)
            .Subscribe(static _ => { });

        await gate.Task.WaitAsync(TimeSpan.FromSeconds(5));
        sub.Dispose();

        var snapshot = Volatile.Read(ref ran);
        await Task.Delay(SettleDelayMilliseconds).ConfigureAwait(false);
        var later = Volatile.Read(ref ran);

        // The loop may execute a few more iterations between Dispose() being called
        // and the next disposal-check, but it must not keep ticking forever.
        await Assert.That(later - snapshot).IsLessThanOrEqualTo(MaxStragglerIterations);
    }

    /// <summary>Increments <paramref name="counter"/> and signals <paramref name="gate"/> on the first iteration.</summary>
    /// <param name="counter">Shared iteration counter.</param>
    /// <param name="gate">Completion source signalled after the first iteration.</param>
    private static void SignalFirstIteration(ref int counter, TaskCompletionSource<bool> gate)
    {
        if (Interlocked.Increment(ref counter) != 1)
        {
            return;
        }

        gate.TrySetResult(true);
    }
}
