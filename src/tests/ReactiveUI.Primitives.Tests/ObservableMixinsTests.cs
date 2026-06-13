// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for internal observable helper operators.</summary>
public class ObservableMixinsTests
{
    /// <summary>First emitted source value.</summary>
    private const int FirstValue = 1;

    /// <summary>Second emitted source value.</summary>
    private const int SecondValue = 2;

    /// <summary>Stopper signal value.</summary>
    private const string StopValue = "stop";

    /// <summary>Verifies that <c>TakeUntil</c> completes when the other observable emits.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenOtherEmits_ThenCompletesAndStopsForwardingSource()
    {
        using Subject<int> source = new();
        using Subject<string> other = new();
        List<int> values = [];
        var completed = false;
        using var subscription =
            source.TakeUntil(other).Subscribe(values.Add, ThrowUnexpectedError, () => completed = true);
        source.OnNext(FirstValue);
        other.OnNext(StopValue);
        source.OnNext(SecondValue);
        await Assert.That(values.SequenceEqual([FirstValue])).IsTrue();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <c>TakeUntil</c> keeps the source alive when the other observable completes without a value.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenOtherCompletesWithoutValue_ThenSourceContinues()
    {
        using Subject<int> source = new();
        using Subject<string> other = new();
        List<int> values = [];
        var completed = false;
        using var subscription =
            source.TakeUntil(other).Subscribe(values.Add, ThrowUnexpectedError, () => completed = true);
        source.OnNext(FirstValue);
        other.OnCompleted();
        source.OnNext(SecondValue);
        source.OnCompleted();
        await Assert.That(values.SequenceEqual([FirstValue, SecondValue])).IsTrue();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <c>TakeUntil</c> forwards errors from the other observable.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task WhenOtherErrors_ThenErrorIsForwardedAndSourceStops()
    {
        using Subject<int> source = new();
        using Subject<string> other = new();
        InvalidOperationException expected = new("expected");
        List<int> values = [];
        Exception? observed = null;
        var completed = false;
        using var subscription = source.TakeUntil(other)
            .Subscribe(values.Add, exception => observed = exception, () => completed = true);
        source.OnNext(FirstValue);
        other.OnError(expected);
        source.OnNext(SecondValue);
        await Assert.That(values.SequenceEqual([FirstValue])).IsTrue();
        await Assert.That(observed!).IsSameReferenceAs(expected);
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Throws when an unexpected error arrives.</summary>
    /// <param name = "exception">The unexpected exception.</param>
    private static void ThrowUnexpectedError(Exception exception) =>
        throw new InvalidOperationException("Unexpected error.", exception);
}
