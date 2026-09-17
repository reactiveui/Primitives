// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests two-stage projection and propagation of source, projection, and inner errors.</summary>
public class SelectManyThenObservableTests
{
    /// <summary>Synthetic error messages.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Synthetic error for the first projection.</summary>
    private const string FirstProjectionFailedMessage = "first failed";

    /// <summary>Synthetic error for the second projection.</summary>
    private const string SecondProjectionFailedMessage = "second failed";

    /// <summary>Synthetic error for the inner observable.</summary>
    private const string InnerErrorMessage = "inner error";

    /// <summary>Multiplier used to derive intermediate values from the source.</summary>
    private const int IntermediateMultiplier = 10;

    /// <summary>Multiplier used to derive final values from the intermediate.</summary>
    private const int FinalMultiplier = 7;

    /// <summary>Source sentinel value.</summary>
    private const int SourceValue = 3;

    /// <summary>Verifies that values flow through both projections to downstream.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManyThenTwoStages_ThenComposesProjections()
    {
        List<int> results = [];
        using var sub = Observable.Return(SourceValue)
            .SelectManyThen(
                static x => Observable.Return(x * IntermediateMultiplier),
                static mid => Observable.Return(mid * FinalMultiplier)).Subscribe(results.Add);
        await Assert.That(results).IsCollectionEqualTo([SourceValue * IntermediateMultiplier * FinalMultiplier]);
    }

    /// <summary>Verifies that a throwing first projection forwards the error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManyThenFirstThrows_ThenForwardsError()
    {
        Exception? caught = null;
        InvalidOperationException expected = new(FirstProjectionFailedMessage);
        using var sub = Observable.Return(SourceValue)
            .SelectManyThen<int, int, int>(_ => throw expected, static _ => Observable.Return(0)).Subscribe(
                static _ => { },
                ex => caught = ex);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that a throwing second projection forwards the error.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManyThenSecondThrows_ThenForwardsError()
    {
        Exception? caught = null;
        InvalidOperationException expected = new(SecondProjectionFailedMessage);
        using var sub = Observable.Return(SourceValue)
            .SelectManyThen<int, int, int>(Observable.Return, _ => throw expected).Subscribe(
                static _ => { },
                ex => caught = ex);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that a source error is forwarded.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManyThenSourceErrors_ThenForwardsError()
    {
        Subject<int> subject = new();
        Exception? caught = null;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var sub = subject.SelectManyThen(Observable.Return, Observable.Return)
            .Subscribe(
                static _ => { },
                ex => caught = ex);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that a source completion is forwarded.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManyThenSourceCompletes_ThenForwardsCompletion()
    {
        Subject<int> subject = new();
        var completed = false;
        using var sub = subject.SelectManyThen(Observable.Return, Observable.Return)
            .Subscribe(
                static _ => { },
                () => completed = true);
        subject.OnCompleted();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that completion arrives exactly once for a single source value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManyThenOneSourceValue_ThenCompletesOnce()
    {
        Subject<int> subject = new();
        var completions = 0;
        using var sub = subject.SelectManyThen(Observable.Return, Observable.Return)
            .Subscribe(
                static _ => { },
                () => completions++);
        subject.OnNext(SourceValue);
        subject.OnCompleted();
        await Assert.That(completions).IsEqualTo(1);
    }

    /// <summary>Verifies that completion arrives exactly once however many sequences the projections opened.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManyThenSeveralSourceValues_ThenCompletesOnce()
    {
        const int SecondSourceValue = 4;
        Subject<int> subject = new();
        List<int> results = [];
        var completions = 0;
        using var sub = subject.SelectManyThen(
                static x => Observable.Return(x * IntermediateMultiplier),
                static mid => Observable.Return(mid * FinalMultiplier))
            .Subscribe(results.Add, () => completions++);
        subject.OnNext(SourceValue);
        subject.OnNext(SecondSourceValue);
        subject.OnCompleted();
        await Assert.That(completions).IsEqualTo(1);
        await Assert.That(results).IsCollectionEqualTo(
        [
            SourceValue * IntermediateMultiplier * FinalMultiplier,
            SecondSourceValue * IntermediateMultiplier * FinalMultiplier,
        ]);
    }

    /// <summary>Verifies that completion waits for a second-stage sequence that outlives the source.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManyThenInnerOutlivesSource_ThenCompletionWaitsForIt()
    {
        Subject<int> subject = new();
        Subject<int> inner = new();
        List<int> results = [];
        var completed = false;
        using var sub = subject.SelectManyThen(Observable.Return, _ => inner)
            .Subscribe(results.Add, () => completed = true);
        subject.OnNext(SourceValue);
        subject.OnCompleted();

        await Assert.That(completed).IsFalse();

        inner.OnNext(SourceValue * FinalMultiplier);
        inner.OnCompleted();

        await Assert.That(completed).IsTrue();
        await Assert.That(results).IsCollectionEqualTo([SourceValue * FinalMultiplier]);
    }

    /// <summary>Verifies that disposal stops delivery from a sequence still running.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManyThenDisposed_ThenInnerValuesStop()
    {
        Subject<int> subject = new();
        Subject<int> inner = new();
        List<int> results = [];
        var sub = subject.SelectManyThen(Observable.Return, _ => inner).Subscribe(results.Add);
        subject.OnNext(SourceValue);
        inner.OnNext(1);
        sub.Dispose();
        inner.OnNext(SourceValue);

        await Assert.That(results).IsCollectionEqualTo([1]);
    }

    /// <summary>Verifies that an inner-observable error is forwarded.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManyThenInnerErrors_ThenForwardsError()
    {
        Exception? caught = null;
        InvalidOperationException expected = new(InnerErrorMessage);
        using var sub = Observable.Return(SourceValue)
            .SelectManyThen(
                static _ => Observable.Throw<int>(new InvalidOperationException("first-inner")),
                Observable.Return).Subscribe(
                static _ => { },
                ex => caught = ex);
        await Assert.That(caught).IsNotNull();
        Exception? caughtSecond = null;
        using var sub2 = Observable.Return(SourceValue)
            .SelectManyThen(Observable.Return, _ => Observable.Throw<int>(expected)).Subscribe(
                static _ => { },
                ex => caughtSecond = ex);
        await Assert.That(caughtSecond).IsSameReferenceAs(expected);
    }
}
