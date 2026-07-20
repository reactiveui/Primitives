// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Core;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Focused coverage for Rx-style alias factories and parity shortcut branches.</summary>
public class SignalAliasCoverageTests
{
    /// <summary>Reusable value one.</summary>
    private const int One = 1;

    /// <summary>Reusable value two.</summary>
    private const int Two = 2;

    /// <summary>Reusable value three.</summary>
    private const int Three = 3;

    /// <summary>Reusable value four.</summary>
    private const int Four = 4;

    /// <summary>Reusable value five.</summary>
    private const int Five = 5;

    /// <summary>Verifies Rx factory aliases cover scheduled, empty, timeout, and switch range branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task RxFactoryAliasesCoverScheduledAndSwitchRangeBranches()
    {
        VirtualClock clock = new(DateTimeOffset.UnixEpoch);
        await Assert.That(Signal.Return(One, clock)).IsNotNull();
        await Assert.That(Signal.Empty<int>(clock)).IsNotNull();
        await Assert.That(Signal.Throw<int>(new InvalidOperationException("scheduled"), clock)).IsNotNull();
        await Assert.That(Signal.Range(One, 0, clock)).IsNotNull();
        await Assert.That(Signal.Range(One, Two, clock)).IsNotNull();
        await Assert.That(Signal.Timeout(Signal.Emit(One), TimeSpan.FromTicks(One))).IsNotNull();
        await Assert.That(Signal.Timeout(Signal.Emit(One), TimeSpan.FromTicks(One), null)).IsNotNull();

        List<int> switchedRanges = [];
        _ = Signal.Switch(Signal.FromEnumerable([Signal.Range(One, Two), Signal.Range(Three, Two)]))
            .Subscribe(switchedRanges.Add);
        await Assert.That(switchedRanges.SequenceEqual([One, Two, Three, Four])).IsTrue();

        List<int> emptySwitch = [];
        var emptySwitchCompleted = 0;
        _ = Signal.Switch(Signal.FromEnumerable<IObservable<int>>([]))
            .Subscribe(emptySwitch.Add, static ex => throw ex, () => emptySwitchCompleted++);
        await Assert.That(emptySwitch.Count).IsEqualTo(0);
        await Assert.That(emptySwitchCompleted).IsEqualTo(One);

        List<int> nonRangeSwitch = [];
        _ = Signal.Switch(Signal.FromEnumerable([Signal.Emit(Five)])).Subscribe(nonRangeSwitch.Add);
        await Assert.That(nonRangeSwitch.SequenceEqual([Five])).IsTrue();
    }

    /// <summary>Verifies parity aliases cover task terminal shortcut branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ParityAliasesCoverTaskObservableBranches()
    {
        List<int> completedTaskValues = [];
        _ = Task.FromResult(One).ToObservable().Subscribe(completedTaskValues.Add);
        await Assert.That(completedTaskValues.SequenceEqual([One])).IsTrue();

        List<string> taskErrors = [];
        _ = Task.FromCanceled<int>(new(true)).ToObservable()
            .Subscribe(static _ => { }, ex => taskErrors.Add(ex.GetType().Name));
        InvalidOperationException expected = new("task-fault");
        Exception? observed = null;
        _ = Task.FromException<int>(expected).ToObservable().Subscribe(static _ => { }, ex => observed = ex);
        await Assert.That(taskErrors.SequenceEqual([nameof(TaskCanceledException)])).IsTrue();
        await Assert.That(observed).IsSameReferenceAs(expected);

        TaskCompletionSource<int> pending = new(TaskCreationOptions.RunContinuationsAsynchronously);
        List<int> pendingValues = [];
        _ = pending.Task.ToObservable().Subscribe(pendingValues.Add);
        pending.SetResult(Two);
        await TestPolling.SpinUntil(() => pendingValues.Count == One, TimeSpan.FromSeconds(One));
        await Assert.That(pendingValues.SequenceEqual([Two])).IsTrue();
    }

    /// <summary>Verifies parity operators cover remaining public range and alias branches.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ParityOperatorsCoverRangeAndAliasBranches()
    {
        List<int> rangeValues = [];
        _ = Signal.Range(One, Two).DefaultIfEmpty().Subscribe(rangeValues.Add);
        await Assert.That(rangeValues.SequenceEqual([One, Two])).IsTrue();

        List<int> uniqueValues = [];
        _ = Signal.FromEnumerable([One, One, Two]).UniqueBy(static value => value, EqualityComparer<int>.Default)
            .Subscribe(uniqueValues.Add);
        await Assert.That(uniqueValues.SequenceEqual([One, Two])).IsTrue();

        await Assert.That(Signal.Emit(One).Calm(TimeSpan.Zero)).IsNotNull();

        List<Moment<int>> moments = [];
        _ = Signal.Range(One, Two).Timestamp().Subscribe(moments.Add);
        await Assert.That(moments.Select(static moment => moment.Value).SequenceEqual([One, Two])).IsTrue();

        List<TimeInterval<int>> intervals = [];
        _ = Signal.Range(One, Two).TimeInterval().Subscribe(intervals.Add);
        await Assert.That(intervals.Select(static interval => interval.Value).SequenceEqual([One, Two])).IsTrue();

        List<int> concatRanges = [];
        _ = Signal.Concat(Signal.Range(One, Two), Signal.Range(Three, Two)).Subscribe(concatRanges.Add);
        await Assert.That(concatRanges.SequenceEqual([One, Two, Three, Four])).IsTrue();

        List<int> mergeRanges = [];
        _ = Signal.Merge(Signal.Range(One, Two), Signal.Range(Three, Two)).Subscribe(mergeRanges.Add);
        await Assert.That(mergeRanges.SequenceEqual([One, Two, Three, Four])).IsTrue();

        List<int> latestRanges = [];
        _ = Signal.PairLatest(Signal.Range(One, Two), Signal.Range(Three, Two), static (left, right) => left + right)
            .Subscribe(latestRanges.Add);
        await Assert.That(latestRanges.SequenceEqual([Two + Three, Two + Four])).IsTrue();
    }

    /// <summary>Verifies direct from-async subscriptions cover constructor and synchronous completion paths.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task FromAsyncSubscriptionsCoverConstructorAndSynchronousCompletionPaths()
    {
        RecordingWitness<int> successful = new();
        FromAsyncSubscription<int> success = new(successful, static _ => Task.FromResult(One));
        using (success.Start())
        {
            await Assert.That(successful.Values.SequenceEqual([One])).IsTrue();
            await Assert.That(successful.Completed).IsEqualTo(One);
        }

        RecordingWitness<int> disposedObserver = new();
        FromAsyncSubscription<int>? disposed = null;
        disposed = new(
            disposedObserver,
            _ =>
            {
                disposed!.Dispose();
                return Task.FromResult(Two);
            });
        using (disposed.Start())
        {
            await Assert.That(disposedObserver.Values.SequenceEqual([Two])).IsTrue();
            await Assert.That(disposedObserver.Completed).IsEqualTo(One);
        }
    }
}
