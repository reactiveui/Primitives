// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using System.Reactive.Subjects;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Tests.Internal;

/// <summary>Tests for <see cref = "FirstAsTaskHelper"/> covering the error and empty-completion paths that <c>ToHotTask</c> does not otherwise exercise.</summary>
public class FirstAsTaskHelperTests
{
    /// <summary>Value used by the latch-on-first-emission test.</summary>
    private const int FirstValue = 7;

    /// <summary>Value used by the latch-on-first-emission test to verify subsequent values are ignored.</summary>
    private const int SecondValue = 11;

    /// <summary>Verifies the helper faults the task when the source emits an error before any value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceErrors_ThenTaskFaults()
    {
        InvalidOperationException expected = new("boom");
        var task = FirstAsTaskHelper.FirstAsTask(Observable.Throw<int>(expected));
        var ex = await Assert.That(() => task).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies the helper faults the task when the source completes empty.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceCompletesEmpty_ThenTaskFaultsWithInvalidOperation()
    {
        var task = FirstAsTaskHelper.FirstAsTask(Observable.Empty<int>());
        await Assert.That(() => task).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies the task latches on the first emission and ignores subsequent values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceEmitsMultiple_ThenTaskCompletesWithFirst()
    {
        Subject<int> subject = new();
        var task = FirstAsTaskHelper.FirstAsTask(subject);
        subject.OnNext(FirstValue);
        subject.OnNext(SecondValue);
        subject.OnCompleted();
        await Assert.That(await task).IsEqualTo(FirstValue);
    }

    /// <summary>Verifies the helper throws when the source argument is null.</summary>
    [Test]
    public void WhenSourceNull_ThenThrowsArgumentNullException() =>
        Assert.Throws<ArgumentNullException>(static () => FirstAsTaskHelper.FirstAsTask<int>(null!));

    /// <summary>Exercises the <c>Subscription?.Dispose()</c> null-conditional branch on
    /// <c>FirstWitness.OnNext</c> — a source that synchronously emits during <c>Subscribe</c>
    /// (such as <see cref = "Observable.Return{T}(T)"/>) fires <c>OnNext</c> before
    /// <c>FirstAsTask</c> can assign the <c>Subscription</c> property, so the latch-and-cleanup
    /// path sees <c>Subscription == null</c> and the conditional dispose becomes a no-op.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncSourceEmits_ThenSubscriptionNullBranchSkipsDispose()
    {
        const int Sentinel = 17;
        var task = FirstAsTaskHelper.FirstAsTask(Observable.Return(Sentinel));
        await Assert.That(await task).IsEqualTo(Sentinel);
    }

    /// <summary>Verifies emissions arriving after the task has already settled are silently ignored.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSubjectErrorsThenLaterEvents_ThenLaterEventsIgnored()
    {
        Subject<int> subject = new();
        var task = FirstAsTaskHelper.FirstAsTask(subject);
        InvalidOperationException expected = new("first");
        subject.OnError(expected);
        subject.OnCompleted();
        subject.OnNext(FirstValue);
        var ex = await Assert.That(() => task).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that a second <c>OnNext</c> arriving via a non-cooperative source
    /// (one that does not stop emitting after the first value) is dropped by the latch.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSecondOnNextAfterFirstSettled_ThenIgnored()
    {
        InvasiveObservable<int> source = new();
        var task = FirstAsTaskHelper.FirstAsTask(source);
        source.Observer.OnNext(FirstValue);
        source.Observer.OnNext(SecondValue);
        source.Observer.OnError(new InvalidOperationException("ignored"));
        source.Observer.OnCompleted();
        await Assert.That(await task).IsEqualTo(FirstValue);
    }

    /// <summary>Verifies that a second <c>OnError</c> arriving after the first is dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSecondOnErrorAfterFirstSettled_ThenIgnored()
    {
        InvasiveObservable<int> source = new();
        var task = FirstAsTaskHelper.FirstAsTask(source);
        InvalidOperationException expected = new("first");
        source.Observer.OnError(expected);
        source.Observer.OnError(new InvalidOperationException("ignored"));
        source.Observer.OnCompleted();
        var caught = await Assert.That(() => task).ThrowsExactly<InvalidOperationException>();
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that a second <c>OnCompleted</c> arriving after the first is dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSecondOnCompletedAfterFirstSettled_ThenIgnored()
    {
        InvasiveObservable<int> source = new();
        var task = FirstAsTaskHelper.FirstAsTask(source);
        source.Observer.OnCompleted();
        source.Observer.OnCompleted();
        source.Observer.OnError(new InvalidOperationException("ignored"));
        await Assert.That(() => task).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Test observable that captures its subscriber so tests can directly invoke
    /// non-cooperative double-terminal sequences against <c>FirstAsTaskHelper</c>'s observer.</summary>
    /// <typeparam name = "T">The element type.</typeparam>
    private sealed class InvasiveObservable<T> : IObservable<T>
    {
        /// <summary>The captured observer from the most recent subscription.</summary>
        private IObserver<T>? _observer;

        /// <summary>Gets the captured observer.</summary>
        public IObserver<T> Observer => _observer ?? throw new InvalidOperationException("No subscriber yet.");

        /// <inheritdoc/>
        public IDisposable Subscribe(IObserver<T> observer)
        {
            _observer = observer;
            return EmptyDisposable.Instance;
        }
    }
}
