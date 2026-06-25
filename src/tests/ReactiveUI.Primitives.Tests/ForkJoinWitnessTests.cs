// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Advanced;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Tests;

/// <summary>Tests for <see cref="ForkJoinWitness{TLeft, TRight, TResult}"/>.</summary>
public sealed class ForkJoinWitnessTests
{
    /// <summary>The integer constant one.</summary>
    private const int One = 1;

    /// <summary>The integer constant two.</summary>
    private const int Two = 2;

    /// <summary>Verifies both sources completing with values emits one result then completes.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task ForkJoinWitnessEmitsResultOnceWhenBothComplete()
    {
        RecordingWitness<int> observer = new();
        CapturingObservable<int> left = new();
        CapturingObservable<int> right = new();
        using var subscription = new ForkJoinWitness<int, int, int>(observer, (a, b) => a + b).Run(left, right);

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
        using var subscription = new ForkJoinWitness<int, int, int>(observer, (a, b) => a + b).Run(left, right);

        left.Observer!.OnError(error);

        await Assert.That(observer.Errors).HasSingleItem();
        await Assert.That(observer.Errors[0]).IsSameReferenceAs(error);

        // Everything from either side is gated after the terminal error.
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
        using var subscription = new ForkJoinWitness<int, int, int>(observer, (a, b) => a + b).Run(left, right);

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
        using var subscription = new ForkJoinWitness<int, int, int>(observer, (a, b) => a + b).Run(left, right);

        left.Observer!.OnNext(One);
        right.Observer!.OnNext(Two);
        left.Observer.OnCompleted();
        right.Observer.OnCompleted();

        await Assert.That(observer.Completed).IsEqualTo(One);

        // A late error from either side must not deliver a second terminal.
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
        using var subscription = new ForkJoinWitness<int, int, int>(observer, (a, b) => a + b).Run(left, right);

        left.Observer!.OnNext(One);
        left.Observer.OnCompleted();

        // Right completes empty: no result, single completion.
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
        using var subscription = new ForkJoinWitness<int, int, int>(observer, (a, b) => a + b).Run(left, right);

        left.Observer!.OnNext(One);
        right.Observer!.OnNext(Two);
        left.Observer.OnCompleted();

        await Assert.That(observer.Completed).IsEqualTo(0);

        right.Observer.OnCompleted();

        await Assert.That(observer.Values).HasSingleItem();
        await Assert.That(observer.Completed).IsEqualTo(One);
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
