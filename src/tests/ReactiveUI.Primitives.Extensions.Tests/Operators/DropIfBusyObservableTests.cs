// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive;
using System.Reactive.Subjects;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests handler completion and error delivery around source termination.</summary>
public class DropIfBusyObservableTests
{
    /// <summary>Verifies a handler completion after source completion does not emit the value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHandlerCompletesAfterSourceDone_ThenValueDropped()
    {
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> values = [];
        var completed = false;
        using DropIfBusyObservable<int>.DropIfBusySink sink = new(Observer.Create<int>(values.Add, () => completed = true), _ => new ValueTask(release.Task));
        var processing = sink.OnNextAsync(1);
        sink.OnCompleted();
        release.SetResult();
        await processing;
        await Assert.That(values).IsEmpty();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies a handler fault after source completion does not report an error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenHandlerThrowsAfterSourceDone_ThenErrorDropped()
    {
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        InvalidOperationException expected = new("late-handler");
        Exception? caught = null;
        var completed = false;
        using DropIfBusyObservable<int>.DropIfBusySink sink = new(
            Observer.Create<int>(
                static _ => { },
                ex => caught = ex,
                () => completed = true),
            async _ =>
            {
                await release.Task.ConfigureAwait(false);
                throw expected;
            });
        var processing = sink.OnNextAsync(1);
        sink.OnCompleted();
        release.SetResult();
        await processing;
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
        var caught = await error.Task;
        await Assert.That(caught).IsSameReferenceAs(expected);
    }
}
