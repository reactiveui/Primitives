// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Concurrency;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for the <c>Start(Func{TResult}, ISequencer?)</c> overload
/// backed by <c>StartFuncObservable&lt;TResult&gt;</c> — paths missed by the happy-path tests
/// (inline vs scheduler dispatch and function-throws on both paths).</summary>
public class StartFuncObservableTests
{
    /// <summary>Result returned by the Start tests.</summary>
    private const int StartResult = 17;

    /// <summary>Message attached to a thrown <c>Start</c> function.</summary>
    private const string FunctionFailedMessage = "function failed";

    /// <summary>Guard timeout so a hung rendezvous fails this test rather than stalling the run.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies that the inline (null-scheduler) overload runs the function, emits the result and completes.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartFuncInline_ThenEmitsResultAndCompletes()
    {
        List<int> results = [];
        var completed = false;

        using var sub = ReactiveExtensions.Start(static () => StartResult, null)
            .Subscribe(results.Add, () => completed = true);

        await Assert.That(results).IsCollectionEqualTo([StartResult]);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that the scheduler overload defers execution but still emits and completes.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartFuncOnScheduler_ThenRunsOnSchedulerAndCompletes()
    {
        List<int> results = [];
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);

        using var sub = ReactiveExtensions.Start(static () => StartResult, Sequencer.Default)
            .Subscribe(results.Add, () => completed.TrySetResult());

        await completed.Task.WaitAsync(GuardTimeout);
        await Assert.That(results).IsCollectionEqualTo([StartResult]);
    }

    /// <summary>Verifies that an exception thrown by the function is surfaced as <c>OnError</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartFuncThrows_ThenForwardsError()
    {
        Exception? caught = null;
        InvalidOperationException expected = new(FunctionFailedMessage);

        using var sub = ReactiveExtensions.Start((Func<int>)(() => throw expected), null)
            .Subscribe(static _ => { }, ex => caught = ex);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that the scheduler path also forwards function errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartFuncOnSchedulerThrows_ThenForwardsError()
    {
        TaskCompletionSource<Exception> faulted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        InvalidOperationException expected = new(FunctionFailedMessage);

        using var sub = ReactiveExtensions.Start((Func<int>)(() => throw expected), Sequencer.Default)
            .Subscribe(static _ => { }, ex => faulted.TrySetResult(ex));

        var caught = await faulted.Task.WaitAsync(GuardTimeout);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }
}
