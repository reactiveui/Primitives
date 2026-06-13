// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.
namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests for <c>WaitUntilObservable</c> — covers the after-terminal guards
/// on <c>OnNext</c>, <c>OnError</c>, and <c>OnCompleted</c> that fire only when an
/// upstream pushes events past its own completion.</summary>
public class WaitUntilObservableTests
{
    /// <summary>Verifies that an <c>OnNext</c> arriving after the predicate has already fired and completed the sequence is silently dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnNextAfterCompleted_ThenDropped()
    {
        const int Match = 1;
        var source = new SyncDirectSource<int>();
        var values = new List<int>();
        var completedCount = 0;
        using var sub = source.WaitUntil(static x => x == Match).Subscribe(values.Add, () => completedCount++);
        source.Observer.OnNext(Match);
        source.Observer.OnNext(Match);
        source.Observer.OnCompleted();
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsCollectionEqualTo([Match]);
    }

    /// <summary>Verifies that an <c>OnError</c> arriving after the predicate has fired is silently dropped via the <c>_done</c> guard.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnErrorAfterCompleted_ThenDropped()
    {
        const int Match = 1;
        var source = new SyncDirectSource<int>();
        Exception? caught = null;
        var completedCount = 0;
        using var sub = source.WaitUntil(static x => x == Match).Subscribe(
            static _ =>
        {
        },
            ex => caught = ex,
            () => completedCount++);
        source.Observer.OnNext(Match);
        source.Observer.OnError(new InvalidOperationException("late"));
        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(caught).IsNull();
    }

    /// <summary>Verifies that a duplicate <c>OnCompleted</c> after an error is silently dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnCompletedAfterError_ThenDropped()
    {
        var source = new SyncDirectSource<int>();
        Exception? caught = null;
        var completedCount = 0;
        var expected = new InvalidOperationException("first");
        using var sub = source.WaitUntil(static _ => false).Subscribe(
            static _ =>
        {
        },
            ex => caught = ex,
            () => completedCount++);
        source.Observer.OnError(expected);
        source.Observer.OnCompleted();
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(completedCount).IsEqualTo(0);
    }
}
