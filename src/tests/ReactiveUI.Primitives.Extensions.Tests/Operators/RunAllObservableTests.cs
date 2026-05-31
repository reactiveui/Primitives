// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using System.Reactive.Subjects;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Edge-case coverage for <c>RunAll</c> backed by <c>RunAllObservable</c> —
/// empty-list short-circuit, sequential walk through synchronous and asynchronous
/// sources, error propagation, and disposal mid-walk.</summary>
public class RunAllObservableTests
{
    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Verifies that an empty list emits <see cref="RxVoid"/> and completes immediately.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRunAllEmptyList_ThenEmitsAndCompletes()
    {
        var emitted = 0;
        var completed = false;

        using var sub = Array.Empty<IObservable<RxVoid>>().RunAll()
            .Subscribe(_ => emitted++, () => completed = true);

        await Assert.That(emitted).IsEqualTo(1);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that synchronous sources iterate without stack growth and complete.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRunAllSyncSources_ThenWalksAllAndCompletes()
    {
        const int SourceCount = 5;
        var runOrder = new List<int>();
        var sources = new IObservable<RxVoid>[SourceCount];
        for (var i = 0; i < SourceCount; i++)
        {
            var index = i;
            sources[i] = Observable.Defer(() =>
            {
                runOrder.Add(index);
                return Observable.Return(RxVoid.Default);
            });
        }

        var emitted = 0;
        var completed = false;

        using var sub = ((IReadOnlyList<IObservable<RxVoid>>)sources).RunAll()
            .Subscribe(_ => emitted++, () => completed = true);

        await Assert.That(runOrder).IsCollectionEqualTo(BuildIndexSequence(SourceCount));
        await Assert.That(emitted).IsEqualTo(1);
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that asynchronous sources complete in order before emitting.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRunAllAsyncSources_ThenWalksSequentially()
    {
        var subjectA = new Subject<RxVoid>();
        var subjectB = new Subject<RxVoid>();
        IObservable<RxVoid>[] sources = [subjectA, subjectB];
        var completed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        using var sub = ((IReadOnlyList<IObservable<RxVoid>>)sources).RunAll()
            .Subscribe(static _ => { }, () => completed.TrySetResult(true));

        await Assert.That(subjectA.HasObservers).IsTrue();
        await Assert.That(subjectB.HasObservers).IsFalse();

        subjectA.OnCompleted();

        await Assert.That(subjectB.HasObservers).IsTrue();
        subjectB.OnCompleted();

        var done = await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(done).IsTrue();
    }

    /// <summary>Verifies that an error in any source propagates and aborts the walk.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRunAllSourceErrors_ThenForwardsAndStops()
    {
        var subjectA = new Subject<RxVoid>();
        var subjectB = new Subject<RxVoid>();
        IObservable<RxVoid>[] sources = [subjectA, subjectB];
        Exception? caught = null;
        var expected = new InvalidOperationException(SourceErrorMessage);

        using var sub = ((IReadOnlyList<IObservable<RxVoid>>)sources).RunAll()
            .Subscribe(static _ => { }, ex => caught = ex);

        subjectA.OnError(expected);

        await Assert.That(caught).IsSameReferenceAs(expected);
        await Assert.That(subjectB.HasObservers).IsFalse();
    }

    /// <summary>Verifies that disposing mid-walk releases the active subscription.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRunAllDisposedMidWalk_ThenStops()
    {
        var subjectA = new Subject<RxVoid>();
        var subjectB = new Subject<RxVoid>();
        IObservable<RxVoid>[] sources = [subjectA, subjectB];
        var completed = false;

        var sub = ((IReadOnlyList<IObservable<RxVoid>>)sources).RunAll()
            .Subscribe(static _ => { }, () => completed = true);

        sub.Dispose();

        // Second dispose hits the Interlocked.Exchange null-loser branch in Sink.Dispose —
        // the first call swapped in null and disposed the previous subscription, so the
        // second call sees null and the `?.Dispose()` no-op fires.
        sub.Dispose();

        subjectA.OnCompleted();

        await Assert.That(completed).IsFalse();
        await Assert.That(subjectB.HasObservers).IsFalse();
    }

    /// <summary>Returns <c>[0, 1, …, count-1]</c> for collection-equality assertions.</summary>
    /// <summary>Verifies that <c>OnNext</c>, <c>OnError</c> and a duplicate <c>OnCompleted</c>
    /// arriving from a candidate after <c>RunAll</c> has already completed are silently dropped.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenEventsAfterCompleted_ThenDropped()
    {
        var first = new SyncDirectSource<RxVoid>();
        IObservable<RxVoid>[] sources = [first];
        var values = new List<RxVoid>();
        Exception? caught = null;
        var completedCount = 0;

        using var sub = ((IReadOnlyList<IObservable<RxVoid>>)sources).RunAll()
            .Subscribe(values.Add, ex => caught = ex, () => completedCount++);

        first.Observer.OnNext(RxVoid.Default);
        first.Observer.OnCompleted();
        first.Observer.OnNext(RxVoid.Default);
        first.Observer.OnError(new InvalidOperationException("late"));
        first.Observer.OnCompleted();

        await Assert.That(completedCount).IsEqualTo(1);
        await Assert.That(values).IsCollectionEqualTo([RxVoid.Default]);
        await Assert.That(caught).IsNull();
    }

    /// <summary>Exercises <c>RunAll.RunNext</c>'s post-loop <c>_done</c> guard — a source
    /// that synchronously errors during <c>Subscribe</c> sets <c>_done = true</c> inline,
    /// the <c>while (!_done ...)</c> loop bails, and the post-loop check returns without
    /// emitting <c>RxVoid.Default</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRunAllSourceSyncErrors_ThenPostLoopDoneGuardSuppressesFinalEmit()
    {
        IObservable<RxVoid>[] sources =
        [
            new SyncErroringObservable<RxVoid>(new InvalidOperationException(SourceErrorMessage)),
        ];
        Exception? caught = null;
        var emitted = 0;
        var completed = false;

        using var sub = ((IReadOnlyList<IObservable<RxVoid>>)sources).RunAll()
            .Subscribe(_ => emitted++, ex => caught = ex, () => completed = true);

        await Assert.That(caught).IsNotNull();
        await Assert.That(caught!.Message).IsEqualTo(SourceErrorMessage);
        await Assert.That(emitted).IsEqualTo(0);
        await Assert.That(completed).IsFalse();
    }

    /// <summary>Builds a zero-based index sequence of the given length.</summary>
    /// <param name="count">The exclusive upper bound.</param>
    /// <returns>A new array of zero-based indices.</returns>
    private static int[] BuildIndexSequence(int count)
    {
        var output = new int[count];
        for (var i = 0; i < count; i++)
        {
            output[i] = i;
        }

        return output;
    }

    /// <summary>Synchronously-erroring observable used to drive the sync-error path of
    /// <c>RunAll.RunNext</c>.</summary>
    /// <typeparam name="T">The element type.</typeparam>
    private sealed class SyncErroringObservable<T>(Exception error) : IObservable<T>
    {
        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            observer.OnError(error);
            return System.Reactive.Disposables.Disposable.Empty;
        }
    }
}
