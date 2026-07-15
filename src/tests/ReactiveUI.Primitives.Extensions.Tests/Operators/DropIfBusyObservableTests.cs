// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for <c>DropIfBusyObservable&lt;T&gt;</c>.</summary>
public class DropIfBusyObservableTests
{
    /// <summary>Delay used to let fire-and-forget async continuations settle.</summary>
    private const int SettleDelayMilliseconds = 50;

    /// <summary>Guard timeout so a hung rendezvous fails this test rather than stalling the run.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies a handler completion after source completion does not emit the value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHandlerCompletesAfterSourceDone_ThenValueDropped()
    {
        Subject<int> subject = new();
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> values = [];
        var completed = false;
        using var sub = subject.DropIfBusy(async _ => await release.Task.ConfigureAwait(false))
            .Subscribe(values.Add, () => completed = true);
        subject.OnNext(1);
        subject.OnCompleted();
        release.SetResult();
        await Task.Delay(SettleDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(values).IsEmpty();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies a handler fault after source completion does not report an error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHandlerThrowsAfterSourceDone_ThenErrorDropped()
    {
        Subject<int> subject = new();
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        InvalidOperationException expected = new("late-handler");
        Exception? caught = null;
        var completed = false;
        using var sub = subject.DropIfBusy(async _ =>
        {
            await release.Task.ConfigureAwait(false);
            throw expected;
        }).Subscribe(
            static _ => { },
            ex => caught = ex,
            () => completed = true);
        subject.OnNext(1);
        subject.OnCompleted();
        release.SetResult();
        await Task.Delay(SettleDelayMilliseconds).ConfigureAwait(false);
        await Assert.That(caught).IsNull();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies a source error before termination is forwarded downstream.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceErrorsBeforeDone_ThenForwardsError()
    {
        Subject<int> subject = new();
        InvalidOperationException expected = new("source-error");
        Exception? caught = null;
        using var sub = subject.DropIfBusy(static _ => default).Subscribe(
            static _ => { },
            ex => caught = ex);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies a handler fault before termination is forwarded downstream.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHandlerThrowsBeforeDone_ThenForwardsError()
    {
        Subject<int> subject = new();
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<Exception> error = new(TaskCreationOptions.RunContinuationsAsynchronously);
        InvalidOperationException expected = new("handler");
        using var sub = subject.DropIfBusy(async _ =>
        {
            await release.Task.ConfigureAwait(false);
            throw expected;
        }).Subscribe(
            static _ => { },
            ex => error.TrySetResult(ex));
        subject.OnNext(1);
        release.SetResult();
        var caught = await error.Task.WaitAsync(GuardTimeout).ConfigureAwait(false);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }
}
