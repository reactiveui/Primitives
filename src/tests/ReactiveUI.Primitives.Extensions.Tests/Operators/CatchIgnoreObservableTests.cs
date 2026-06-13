// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for <c>CatchIgnore&lt;TSource, TException&gt;</c> backed by
/// <c>CatchIgnoreObservable&lt;TSource, TException&gt;</c> — exception filtering and the
/// action-throws branch.</summary>
public class CatchIgnoreObservableTests
{
    /// <summary>Verifies that <c>CatchIgnore</c> invokes the action and completes on a matching exception.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchIgnoreMatchesException_ThenInvokesActionAndCompletes()
    {
        Subject<int> subject = new();
        Exception? observed = null;
        var completed = false;
        InvalidOperationException expected = new("caught");
        using var sub = subject.CatchIgnore<int, InvalidOperationException>(ex => observed = ex).Subscribe(
            static _ => { },
            static _ => Assert.Fail("OnError should not run when ignored."),
            () => completed = true);
        subject.OnError(expected);
        await Assert.That(observed).IsSameReferenceAs(expected);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <c>CatchIgnore</c> forwards non-matching exceptions.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchIgnoreDoesNotMatchException_ThenForwardsError()
    {
        Subject<int> subject = new();
        Exception? caught = null;
        var actionInvocations = 0;
        ArgumentException expected = new("not matching");
        using var sub = subject.CatchIgnore<int, InvalidOperationException>(_ => actionInvocations++).Subscribe(
            static _ => { },
            ex => caught = ex);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(actionInvocations).IsEqualTo(0);
    }

    /// <summary>Verifies that when the catch action itself throws, the new exception is forwarded.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchIgnoreActionThrows_ThenForwardsActionException()
    {
        Subject<int> subject = new();
        Exception? caught = null;
        InvalidOperationException fromAction = new("action failed");
        using var sub = subject.CatchIgnore<int, InvalidOperationException>(_ => throw fromAction).Subscribe(
            static _ => { },
            ex => caught = ex);
        subject.OnError(new InvalidOperationException("original"));
        await Assert.That(caught).IsSameReferenceAs(fromAction);
    }

    /// <summary>Verifies that values arriving before the error are forwarded unchanged.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCatchIgnoreSourceEmitsValues_ThenForwardedThenCompletedOnIgnoredError()
    {
        const int FirstValue = 1;
        const int SecondValue = 2;
        Subject<int> subject = new();
        List<int> values = [];
        var completed = false;
        using var sub = subject.CatchIgnore<int, InvalidOperationException>(static _ => { })
            .Subscribe(values.Add, () => completed = true);
        subject.OnNext(FirstValue);
        subject.OnNext(SecondValue);
        subject.OnError(new InvalidOperationException("swallowed"));
        await Assert.That(values).IsCollectionEqualTo([FirstValue, SecondValue]);
        await Assert.That(completed).IsTrue();
    }
}
