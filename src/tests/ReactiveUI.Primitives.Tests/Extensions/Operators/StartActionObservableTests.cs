// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Extensions.Internal;
using ReactiveUI.Primitives.Extensions.Operators;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.IO;
using ReactiveUI.Primitives.Extensions.Tests;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for the action-form <c>Start</c> operator backed by
/// <c>StartActionObservable</c> — synchronous inline path, scheduler dispatch,
/// and action-throws forwarding.</summary>
public class StartActionObservableTests
{
    /// <summary>Synthetic error message attached to action failures.</summary>
    private const string ActionFailedMessage = "action failed";

    /// <summary>Verifies that <c>Start</c> with a null scheduler runs synchronously and completes.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartActionInline_ThenRunsAndCompletes()
    {
        var ran = false;
        var completed = false;
        var emitted = 0;

        using var sub = ReactiveExtensions.Start(() => ran = true, scheduler: null)
            .Subscribe(_ => emitted++, () => completed = true);

        await Assert.That(ran).IsTrue();
        await Assert.That(emitted).IsEqualTo(1);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <c>Start</c> with a scheduler dispatches the action via that scheduler.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartActionWithScheduler_ThenRunsViaScheduler()
    {
        var completed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var ran = false;

        using var sub = ReactiveExtensions.Start(() => ran = true, TaskPoolSequencer.Default)
            .Subscribe(static _ => { }, () => completed.TrySetResult(true));

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(ran).IsTrue();
    }

    /// <summary>Verifies that an exception thrown by the inline action is forwarded to <c>OnError</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartActionThrowsInline_ThenForwardsError()
    {
        Exception? caught = null;
        var expected = new InvalidOperationException(ActionFailedMessage);

        using var sub = ReactiveExtensions.Start(() => throw expected, scheduler: null)
            .Subscribe(static _ => { }, ex => caught = ex);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that an exception thrown by the scheduled action is forwarded to <c>OnError</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenStartActionThrowsScheduled_ThenForwardsError()
    {
        var faulted = new TaskCompletionSource<Exception>(TaskCreationOptions.RunContinuationsAsynchronously);
        var expected = new InvalidOperationException(ActionFailedMessage);

        using var sub = ReactiveExtensions.Start(() => throw expected, TaskPoolSequencer.Default)
            .Subscribe(static _ => { }, ex => faulted.TrySetResult(ex));

        var caught = await faulted.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }
}
