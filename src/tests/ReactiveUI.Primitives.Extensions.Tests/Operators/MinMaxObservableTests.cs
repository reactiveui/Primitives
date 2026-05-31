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
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGetMaxTwoSources_ThenEmitsMaxAcrossUpdates()
    {
        var a = new Subject<int>();
        var b = new Subject<int>();
        var results = new List<int>();

        using var sub = a.GetMax(b).Subscribe(results.Add);

        a.OnNext(LowValue);
        b.OnNext(MidValue);
        b.OnNext(HighValue);
        a.OnNext(MidValue);

        await Assert.That(results).IsCollectionEqualTo([MidValue, HighValue, HighValue]);
    }

    /// <summary>Verifies that <c>GetMin</c> emits the smallest of the latest values from every source.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGetMinTwoSources_ThenEmitsMinAcrossUpdates()
    {
        var a = new Subject<int>();
        var b = new Subject<int>();
        var results = new List<int>();

        using var sub = a.GetMin(b).Subscribe(results.Add);

        a.OnNext(HighValue);
        b.OnNext(MidValue);
        a.OnNext(LowValue);

        await Assert.That(results).IsCollectionEqualTo([MidValue, LowValue]);
    }

    /// <summary>Verifies that partial sources do not emit until every source has a value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGetMaxPartialSources_ThenSuppressesEmission()
    {
        var a = new Subject<int>();
        var b = new Subject<int>();
        var results = new List<int>();

        using var sub = a.GetMax(b).Subscribe(results.Add);

        a.OnNext(LowValue);

        await Assert.That(results).IsEmpty();
    }

    /// <summary>Verifies that a source error propagates downstream.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGetMaxSourceErrors_ThenForwardsError()
    {
        var a = new Subject<int>();
        var b = new Subject<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);

        using var sub = a.GetMax(b).Subscribe(static _ => { }, ex => caught = ex);

        a.OnError(expected);

        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies <c>GetMax</c> with no additional sources still emits the source's own values verbatim.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenGetMaxSingleSource_ThenEmitsSourceValues()
    {
        var subject = new Subject<int>();
        var results = new List<int>();
        using var sub = subject.GetMax().Subscribe(results.Add);

        subject.OnNext(LowValue);
        subject.OnNext(MidValue);
        subject.OnNext(HighValue);

        await Assert.That(results).IsCollectionEqualTo([LowValue, MidValue, HighValue]);
    }

    /// <summary>Verifies that <see cref="MinMaxObservable{T}"/>
    /// with an empty source list completes immediately without emitting.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenMinMaxObservableNoSources_ThenCompletesImmediately()
    {
        var observable = new MinMaxObservable<int>([], emitMaximum: true);
        var completed = false;
        var emitted = 0;

        using var sub = observable.Subscribe(_ => emitted++, () => completed = true);

        await Assert.That(emitted).IsEqualTo(0);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <see cref="MinMaxObservable{T}"/>
    /// throws when subscribed with a null observer.</summary>
    [Test]
    public void WhenMinMaxObservableNullObserver_ThenSubscribeThrows()
    {
        var observable = new MinMaxObservable<int>([new Subject<int>()], emitMaximum: false);

        Assert.Throws<ArgumentNullException>(() => observable.Subscribe(null!));
    }

    /// <summary>Verifies that when every source completes, the combined sequence completes via
    /// the per-source <c>OnCompleted</c> path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAllSourcesComplete_ThenForwardsCompletion()
    {
        var a = new Subject<int>();
        var b = new Subject<int>();
        var completed = false;

        using var sub = a.GetMax(b).Subscribe(static _ => { }, () => completed = true);

        a.OnNext(LowValue);
        b.OnNext(MidValue);
        a.OnCompleted();
        b.OnCompleted();

        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that an <c>OnNext</c> arriving after the combined sequence has terminated
    /// is silently dropped via the <c>_state.IsDone</c> guard.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOnNextAfterTerminated_ThenDropped()
    {
        var a = new SyncDirectSource<int>();
        var b = new SyncDirectSource<int>();
        var results = new List<int>();
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);

        using var sub = a.GetMax(b).Subscribe(results.Add, ex => caught = ex);

        a.Observer.OnError(expected);
        b.Observer.OnNext(HighValue);

        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(results).IsEmpty();
    }
}
