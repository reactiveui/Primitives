// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Tests for internal observable helper operators.
/// </summary>
public class ObservableMixinsTests
{
    /// <summary>
    /// First emitted source value.
    /// </summary>
    private const int FirstValue = 1;

    /// <summary>
    /// Second emitted source value.
    /// </summary>
    private const int SecondValue = 2;

    /// <summary>
    /// Stopper signal value.
    /// </summary>
    private const string StopValue = "stop";

    /// <summary>
    /// Verifies that <c>TakeUntil</c> completes when the other observable emits.
    /// </summary>
    [Test]
    public void WhenOtherEmits_ThenCompletesAndStopsForwardingSource()
    {
        using var source = new Subject<int>();
        using var other = new Subject<string>();
        var values = new List<int>();
        var completed = false;

        using var subscription = source.TakeUntil(other)
            .Subscribe(values.Add, ThrowUnexpectedError, () => completed = true);

        source.OnNext(FirstValue);
        other.OnNext(StopValue);
        source.OnNext(SecondValue);

        Assert.Equal<int>([FirstValue], values);
        Assert.True(completed);
    }

    /// <summary>
    /// Verifies that <c>TakeUntil</c> keeps the source alive when the other observable completes without a value.
    /// </summary>
    [Test]
    public void WhenOtherCompletesWithoutValue_ThenSourceContinues()
    {
        using var source = new Subject<int>();
        using var other = new Subject<string>();
        var values = new List<int>();
        var completed = false;

        using var subscription = source.TakeUntil(other)
            .Subscribe(values.Add, ThrowUnexpectedError, () => completed = true);

        source.OnNext(FirstValue);
        other.OnCompleted();
        source.OnNext(SecondValue);
        source.OnCompleted();

        Assert.Equal<int>([FirstValue, SecondValue], values);
        Assert.True(completed);
    }

    /// <summary>
    /// Verifies that <c>TakeUntil</c> forwards errors from the other observable.
    /// </summary>
    [Test]
    public void WhenOtherErrors_ThenErrorIsForwardedAndSourceStops()
    {
        using var source = new Subject<int>();
        using var other = new Subject<string>();
        var expected = new InvalidOperationException("expected");
        var values = new List<int>();
        Exception? observed = null;
        var completed = false;

        using var subscription = source.TakeUntil(other)
            .Subscribe(values.Add, exception => observed = exception, () => completed = true);

        source.OnNext(FirstValue);
        other.OnError(expected);
        source.OnNext(SecondValue);

        Assert.Equal<int>([FirstValue], values);
        Assert.Same(expected, observed!);
        Assert.False(completed);
    }

    /// <summary>
    /// Throws when an unexpected error arrives.
    /// </summary>
    /// <param name="exception">The unexpected exception.</param>
    private static void ThrowUnexpectedError(Exception exception) =>
        throw new InvalidOperationException("Unexpected error.", exception);
}
