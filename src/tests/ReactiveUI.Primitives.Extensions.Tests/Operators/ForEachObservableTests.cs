// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests for <see cref = "ForEachObservable{T}"/> — null-batch ignore semantics, the
/// scheduler-marshalled delivery path, error forwarding, and the null-observer subscribe guard.</summary>
public class ForEachObservableTests
{
    /// <summary>Sentinel batch element.</summary>
    private const int ValueOne = 1;

    /// <summary>Sentinel batch element.</summary>
    private const int ValueTwo = 2;

    /// <summary>Sentinel batch element.</summary>
    private const int ValueThree = 3;

    /// <summary>Scheduler-delivered sentinel.</summary>
    private const int ScheduledTen = 10;

    /// <summary>Scheduler-delivered sentinel.</summary>
    private const int ScheduledTwenty = 20;

    /// <summary>Scheduler-delivered sentinel.</summary>
    private const int ScheduledThirty = 30;

    /// <summary>Guard timeout so a hung rendezvous fails this test rather than stalling the run.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies that a null inner enumerable is ignored and subsequent batches continue flowing.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForEachReceivesNullBatch_ThenIgnoresNullAndProcessesNext()
    {
        Subject<IEnumerable<int>> subject = new();
        List<int> results = [];
        using var sub = subject.ForEach().Subscribe(results.Add);
        subject.OnNext(null!);
        subject.OnNext([ValueOne, ValueTwo]);
        subject.OnNext([ValueThree]);
        await Assert.That(results).IsCollectionEqualTo([ValueOne, ValueTwo, ValueThree]);
    }

    /// <summary>Verifies the scheduler overload delivers every value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForEachWithScheduler_ThenDeliversAllValues()
    {
        IEnumerable<int>[] batches = [[ScheduledTen, ScheduledTwenty], [ScheduledThirty]];
        var source = batches.ToObservable();
        TaskCompletionSource<List<int>> done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> results = [];
        using var sub = source.ForEach(Sequencer.Default).Subscribe(results.Add, () => done.TrySetResult(results));
        var output = await done.Task.WaitAsync(GuardTimeout);
        await Assert.That(output).IsCollectionEqualTo([ScheduledTen, ScheduledTwenty, ScheduledThirty]);
    }

    /// <summary>Verifies source errors are forwarded.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForEachSourceErrors_ThenErrorForwarded()
    {
        Subject<IEnumerable<int>> subject = new();
        Exception? caught = null;
        using var sub = subject.ForEach().Subscribe(
            static _ => { },
            ex => caught = ex);
        InvalidOperationException expected = new("boom");
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies subscribing with a null observer throws.</summary>
    [Test]
    public void WhenForEachObserverNull_ThenSubscribeThrows()
    {
        ForEachObservable<int> observable = new(new Subject<IEnumerable<int>>(), null);
        _ = Assert.Throws<ArgumentNullException>(() => observable.Subscribe(null!));
    }
}
