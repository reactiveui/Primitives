// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;
using ReactiveUI.Primitives.Signals;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="ForkJoinWitness{TLeft, TRight, TResult}"/>.</summary>
public sealed partial class ForkJoinWitnessTests
{
    /// <summary>The integer constant one.</summary>
    private const int One = 1;

    /// <summary>The integer constant two.</summary>
    private const int Two = 2;

    /// <summary>Expected sum of One and Two.</summary>
    private const int Three = 3;

    /// <summary>Iteration count for concurrent completion stress loops.</summary>
    private const int RaceIterations = 150;

    /// <summary>Maximum wait in seconds for concurrent completion.</summary>
    private const int StressTimeoutSeconds = 5;

    /// <summary>Verifies both sources completing with values emits one result then completes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ForkJoinWitnessEmitsResultOnceWhenBothComplete()
    {
        RecordingWitness<int> observer = new();
        CapturingObservable<int> left = new();
        CapturingObservable<int> right = new();
        using var subscription = new ForkJoinWitness<int, int, int>(observer, static (a, b) => a + b).Run(left, right);

        left.Observer!.OnNext(One);
        right.Observer!.OnNext(Two);
        left.Observer.OnCompleted();
        right.Observer.OnCompleted();

        await Assert.That(observer.Values).HasSingleItem();
        await Assert.That(observer.Values[0]).IsEqualTo(One + Two);
        await Assert.That(observer.Completed).IsEqualTo(One);
        await Assert.That(observer.Errors).IsEmpty();
    }

    /// <summary>Verifies an error from one source is forwarded once and gates the other source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ForkJoinWitnessForwardsFirstErrorAndGatesAfterwards()
    {
        RecordingWitness<int> observer = new();
        CapturingObservable<int> left = new();
        CapturingObservable<int> right = new();
        InvalidOperationException error = new("left");
        using var subscription = new ForkJoinWitness<int, int, int>(observer, static (a, b) => a + b).Run(left, right);

        left.Observer!.OnError(error);

        await Assert.That(observer.Errors).HasSingleItem();
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);

        right.Observer!.OnNext(Two);
        right.Observer.OnError(new InvalidOperationException("right"));
        right.Observer.OnCompleted();
        left.Observer.OnCompleted();

        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(0);
        await Assert.That(observer.Errors).HasSingleItem();
    }

    /// <summary>Verifies two errors deliver <see cref="IObserver{T}.OnError"/> exactly once.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ForkJoinWitnessForwardsOnlyTheFirstOfTwoErrors()
    {
        RecordingWitness<int> observer = new();
        CapturingObservable<int> left = new();
        CapturingObservable<int> right = new();
        InvalidOperationException first = new("first");
        InvalidOperationException second = new("second");
        using var subscription = new ForkJoinWitness<int, int, int>(observer, static (a, b) => a + b).Run(left, right);

        left.Observer!.OnError(first);
        right.Observer!.OnError(second);

        await Assert.That(observer.Errors).HasSingleItem();
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(first);
    }

    /// <summary>Verifies an error after the result has been emitted produces no second terminal.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ForkJoinWitnessIgnoresErrorAfterCompletion()
    {
        RecordingWitness<int> observer = new();
        CapturingObservable<int> left = new();
        CapturingObservable<int> right = new();
        using var subscription = new ForkJoinWitness<int, int, int>(observer, static (a, b) => a + b).Run(left, right);

        left.Observer!.OnNext(One);
        right.Observer!.OnNext(Two);
        left.Observer.OnCompleted();
        right.Observer.OnCompleted();

        await Assert.That(observer.Completed).IsEqualTo(One);

        left.Observer.OnError(new InvalidOperationException("late"));
        right.Observer.OnError(new InvalidOperationException("late"));

        await Assert.That(observer.Errors).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(One);
    }

    /// <summary>Verifies completion with a side that produced no value completes without a result.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ForkJoinWitnessCompletesWithoutResultWhenASideIsEmpty()
    {
        RecordingWitness<int> observer = new();
        CapturingObservable<int> left = new();
        CapturingObservable<int> right = new();
        using var subscription = new ForkJoinWitness<int, int, int>(observer, static (a, b) => a + b).Run(left, right);

        left.Observer!.OnNext(One);
        left.Observer.OnCompleted();

        right.Observer!.OnCompleted();

        await Assert.That(observer.Values).IsEmpty();
        await Assert.That(observer.Completed).IsEqualTo(One);
    }

    /// <summary>Verifies a single source completing does not finish until both are done.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ForkJoinWitnessDefersCompletionUntilBothComplete()
    {
        RecordingWitness<int> observer = new();
        CapturingObservable<int> left = new();
        CapturingObservable<int> right = new();
        using var subscription = new ForkJoinWitness<int, int, int>(observer, static (a, b) => a + b).Run(left, right);

        left.Observer!.OnNext(One);
        right.Observer!.OnNext(Two);
        left.Observer.OnCompleted();

        await Assert.That(observer.Completed).IsEqualTo(0);

        right.Observer.OnCompleted();

        await Assert.That(observer.Values).HasSingleItem();
        await Assert.That(observer.Completed).IsEqualTo(One);
    }

    /// <summary>Concurrent emissions and completions on both ForkJoin arms always deliver the combined result and completion.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task Run_ConcurrentArmCompletion_AlwaysEmitsCombinedResultAndCompletes()
    {
        for (var i = 0; i < RaceIterations; i++)
        {
            using Signal<int> left = new();
            using Signal<int> right = new();
            TaskCompletionSource<int> done = new(TaskCreationOptions.RunContinuationsAsynchronously);
            var sum = 0;
            using var sub = new ForkJoinWitness<int, int, int>(
                new DelegateWitness<int>(x => sum = x, static _ => { }, () => done.TrySetResult(sum)),
                static (l, r) => l + r).Run(left, right);

            var t1 = Task.Run(() =>
            {
                left.OnNext(One);
                left.OnCompleted();
            });
            var t2 = Task.Run(() =>
            {
                right.OnNext(Two);
                right.OnCompleted();
            });

            await Task.WhenAll(t1, t2);
            using CancellationTokenSource timeout = new(TimeSpan.FromSeconds(StressTimeoutSeconds));
            await Assert.That(await done.Task.WaitAsync(timeout.Token)).IsEqualTo(Three);
        }
    }

    /// <summary>Verifies fork-join delivers completion and errors both during subscribe and after its subscriptions are stored.</summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Test]
    public async Task Terminal_DuringSubscribeAndAfterSubscribe_DeliversOnceAndReleases()
    {
        InvalidOperationException boom = new("fork-join");

        RecordingWitness<int> syncCompleted = new();
        using (Signal.Return(One).ForkJoin(Signal.Return(Two), static (a, b) => a + b).Subscribe(syncCompleted))
        {
            await Assert.That(syncCompleted.Values.SequenceEqual([Three])).IsTrue();
        }

        RecordingWitness<int> syncFailed = new();
        using (Signal.Throw<int>(boom).ForkJoin(Signal.Return(Two), static (a, b) => a + b).Subscribe(syncFailed))
        {
            await Assert.That(syncFailed.Errors.SequenceEqual([boom])).IsTrue();
        }

        using Signal<int> left = new();
        using Signal<int> right = new();
        RecordingWitness<int> liveCompleted = new();
        using (left.ForkJoin(right, static (a, b) => a + b).Subscribe(liveCompleted))
        {
            left.OnNext(One);
            left.OnCompleted();
            right.OnNext(Two);
            right.OnCompleted();
        }

        using Signal<int> failingLeft = new();
        using Signal<int> failingRight = new();
        RecordingWitness<int> liveFailed = new();
        using (failingLeft.ForkJoin(failingRight, static (a, b) => a + b).Subscribe(liveFailed))
        {
            failingLeft.OnError(boom);
        }

        await Assert.That(liveCompleted.Values.SequenceEqual([Three])).IsTrue();
        await Assert.That(liveCompleted.Completed).IsEqualTo(One);
        await Assert.That(liveFailed.Errors.SequenceEqual([boom])).IsTrue();
    }

    /// <summary>An observable that captures its observer for manual notification.</summary>
    /// <typeparam name="T">The value type.</typeparam>
    private sealed class CapturingObservable<T> : IObservable<T>
    {
        /// <summary>Gets the captured observer.</summary>
        public IObserver<T>? Observer { get; private set; }

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            Observer = observer;
            return EmptyDisposable.Instance;
        }
    }
}
