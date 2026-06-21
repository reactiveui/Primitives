// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for the <c>GetMin</c> / <c>GetMax</c> operators
/// backed by <c>MinMaxObservable&lt;T&gt;</c> — partial-source suppression,
/// max/min selection over multiple updates, and source-error propagation.</summary>
public class MinMaxObservableTests
{
    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Sentinel value for the first source.</summary>
    private const int LowValue = 1;

    /// <summary>Sentinel value for the second source (initial).</summary>
    private const int MidValue = 5;

    /// <summary>Sentinel value used as an update to the second source.</summary>
    private const int HighValue = 10;

    /// <summary>Verifies that <c>GetMax</c> emits the largest of the latest values from every source.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGetMaxTwoSources_ThenEmitsMaxAcrossUpdates()
    {
        Subject<int> a = new();
        Subject<int> b = new();
        List<int> results = [];
        using var sub = a.GetMax(b).Subscribe(results.Add);
        a.OnNext(LowValue);
        b.OnNext(MidValue);
        b.OnNext(HighValue);
        a.OnNext(MidValue);
        await Assert.That(results).IsCollectionEqualTo([MidValue, HighValue, HighValue]);
    }

    /// <summary>Verifies that <c>GetMin</c> emits the smallest of the latest values from every source.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGetMinTwoSources_ThenEmitsMinAcrossUpdates()
    {
        Subject<int> a = new();
        Subject<int> b = new();
        List<int> results = [];
        using var sub = a.GetMin(b).Subscribe(results.Add);
        a.OnNext(HighValue);
        b.OnNext(MidValue);
        a.OnNext(LowValue);
        await Assert.That(results).IsCollectionEqualTo([MidValue, LowValue]);
    }

    /// <summary>Verifies that partial sources do not emit until every source has a value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGetMaxPartialSources_ThenSuppressesEmission()
    {
        Subject<int> a = new();
        Subject<int> b = new();
        List<int> results = [];
        using var sub = a.GetMax(b).Subscribe(results.Add);
        a.OnNext(LowValue);
        await Assert.That(results).IsEmpty();
    }

    /// <summary>Verifies that a source error propagates downstream.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGetMaxSourceErrors_ThenForwardsError()
    {
        Subject<int> a = new();
        Subject<int> b = new();
        Exception? caught = null;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var sub = a.GetMax(b).Subscribe(
            static _ => { },
            ex => caught = ex);
        a.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies <c>GetMax</c> with no additional sources still emits the source's own values verbatim.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGetMaxSingleSource_ThenEmitsSourceValues()
    {
        Subject<int> subject = new();
        List<int> results = [];
        using var sub = subject.GetMax().Subscribe(results.Add);
        subject.OnNext(LowValue);
        subject.OnNext(MidValue);
        subject.OnNext(HighValue);
        await Assert.That(results).IsCollectionEqualTo([LowValue, MidValue, HighValue]);
    }

    /// <summary>Verifies <c>GetMin</c> with no additional sources still emits the source's own values verbatim.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGetMinSingleSource_ThenEmitsSourceValues()
    {
        Subject<int> subject = new();
        List<int> results = [];
        using var sub = subject.GetMin().Subscribe(results.Add);
        subject.OnNext(HighValue);
        subject.OnNext(MidValue);
        subject.OnNext(LowValue);
        await Assert.That(results).IsCollectionEqualTo([HighValue, MidValue, LowValue]);
    }

    /// <summary>Verifies that <see cref = "MinMaxObservable{T}"/> with an empty source list completes immediately without emitting.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMinMaxObservableNoSources_ThenCompletesImmediately()
    {
        MinMaxObservable<int> observable = new([], true);
        var completed = false;
        var emitted = 0;
        using var sub = observable.Subscribe(_ => emitted++, () => completed = true);
        await Assert.That(emitted).IsEqualTo(0);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <see cref = "MinMaxObservable{T}"/> throws when subscribed with a null observer.</summary>
    [Test]
    public void WhenMinMaxObservableNullObserver_ThenSubscribeThrows()
    {
        MinMaxObservable<int> observable = new([new Subject<int>()], false);
        _ = Assert.Throws<ArgumentNullException>(() => observable.Subscribe(null!));
    }

    /// <summary>Verifies that when every source completes, the combined sequence completes via the per-source <c>OnCompleted</c> path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAllSourcesComplete_ThenForwardsCompletion()
    {
        Subject<int> a = new();
        Subject<int> b = new();
        var completed = false;
        using var sub = a.GetMax(b).Subscribe(
            static _ => { },
            () => completed = true);
        a.OnNext(LowValue);
        b.OnNext(MidValue);
        a.OnCompleted();
        b.OnCompleted();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that an <c>OnNext</c> arriving after the combined sequence has terminated
    /// is silently dropped via the <c>_state.IsDone</c> guard.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnNextAfterTerminated_ThenDropped()
    {
        SyncDirectSource<int> a = new();
        SyncDirectSource<int> b = new();
        List<int> results = [];
        Exception? caught = null;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var sub = a.GetMax(b).Subscribe(results.Add, ex => caught = ex);
        a.Observer.OnError(expected);
        b.Observer.OnNext(HighValue);
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(results).IsEmpty();
    }

    /// <summary>Verifies the general three-source path drops notifications after terminal state.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenManySourceMinMaxTerminates_ThenLateSignalsAreDropped()
    {
        SyncDirectSource<int> a = new();
        SyncDirectSource<int> b = new();
        SyncDirectSource<int> c = new();
        List<int> results = [];
        var completions = 0;
        Exception? caught = null;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var sub = a.GetMax(b, c).Subscribe(results.Add, ex => caught = ex, () => completions++);
        a.Observer.OnError(expected);
        b.Observer.OnNext(HighValue);
        c.Observer.OnCompleted();
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(results).IsEmpty();
        await Assert.That(completions).IsEqualTo(0);
    }

    /// <summary>Verifies that a second error after termination is ignored by the binary fast path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBinarySourceErrorsAfterError_ThenDropped()
    {
        SyncDirectSource<int> a = new();
        SyncDirectSource<int> b = new();
        var receivedErrors = 0;
        Exception? caught = null;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var sub = a.GetMax(b).Subscribe(
            static _ => { },
            ex =>
            {
                receivedErrors++;
                caught = ex;
            });
        a.Observer.OnError(expected);
        b.Observer.OnError(new InvalidOperationException("late error"));
        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(receivedErrors).IsEqualTo(1);
    }

    /// <summary>Verifies that completion from an empty left source completes the binary fast path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBinaryLeftCompletesEmpty_ThenCompletesAndDropsLaterCompletion()
    {
        SyncDirectSource<int> a = new();
        SyncDirectSource<int> b = new();
        var completions = 0;
        using var sub = a.GetMax(b).Subscribe(
            static _ => { },
            () => completions++);
        a.Observer.OnCompleted();
        b.Observer.OnCompleted();
        await Assert.That(completions).IsEqualTo(1);
    }

    /// <summary>Verifies that completion from an empty right source completes the binary fast path.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBinaryRightCompletesEmpty_ThenCompletesAndDropsLaterCompletion()
    {
        SyncDirectSource<int> a = new();
        SyncDirectSource<int> b = new();
        var completions = 0;
        using var sub = a.GetMin(b).Subscribe(
            static _ => { },
            () => completions++);
        b.Observer.OnCompleted();
        a.Observer.OnCompleted();
        await Assert.That(completions).IsEqualTo(1);
    }

    /// <summary>Verifies duplicate left completion is ignored until the right source completes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBinaryLeftCompletesTwiceBeforeRight_ThenDuplicateIgnored()
    {
        SyncDirectSource<int> a = new();
        SyncDirectSource<int> b = new();
        var completions = 0;
        using var sub = a.GetMax(b).Subscribe(
            static _ => { },
            () => completions++);
        a.Observer.OnNext(LowValue);
        b.Observer.OnNext(MidValue);
        a.Observer.OnCompleted();
        a.Observer.OnCompleted();
        await Assert.That(completions).IsEqualTo(0);
        b.Observer.OnCompleted();
        await Assert.That(completions).IsEqualTo(1);
    }

    /// <summary>Verifies duplicate right completion is ignored until the left source completes.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenBinaryRightCompletesTwiceBeforeLeft_ThenDuplicateIgnored()
    {
        SyncDirectSource<int> a = new();
        SyncDirectSource<int> b = new();
        var completions = 0;
        using var sub = a.GetMin(b).Subscribe(
            static _ => { },
            () => completions++);
        a.Observer.OnNext(MidValue);
        b.Observer.OnNext(HighValue);
        b.Observer.OnCompleted();
        b.Observer.OnCompleted();
        await Assert.That(completions).IsEqualTo(0);
        a.Observer.OnCompleted();
        await Assert.That(completions).IsEqualTo(1);
    }
}
