// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Linq;
using System.Reactive.Subjects;
using ReactiveUI.Primitives.Disposables;

namespace ReactiveUI.Primitives.Extensions.Tests.Internal;

/// <summary>Tests for <see cref = "FirstAsValueTaskHelper{T}"/> covering the value, error, and
/// empty-completion paths the pooled <c>ToHotValueTask</c> source exposes, plus the pool reuse and
/// post-settle drop branches.</summary>
public class FirstAsValueTaskHelperTests
{
    /// <summary>Message of an error the helper is expected to drop.</summary>
    private const string IgnoredErrorMessage = "ignored";

    /// <summary>Value used by the latch-on-first-emission tests.</summary>
    private const int FirstValue = 7;

    /// <summary>Value used to verify subsequent values are ignored.</summary>
    private const int SecondValue = 11;

    /// <summary>Guard timeout so a hung rendezvous fails this test rather than stalling the run.</summary>
    private static readonly TimeSpan GuardTimeout = TimeSpan.FromSeconds(5);

    /// <summary>Verifies the helper completes with the first value the source emits.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceEmits_ThenValueTaskCompletesWithFirst()
    {
        Subject<int> subject = new();
        var task = FirstAsValueTaskHelper<int>.FirstAsValueTask(subject);
        subject.OnNext(FirstValue);
        subject.OnNext(SecondValue);
        subject.OnCompleted();
        await Assert.That(await task).IsEqualTo(FirstValue);
    }

    /// <summary>Verifies the helper faults the value task when the source errors before any value.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceErrors_ThenValueTaskFaults()
    {
        InvalidOperationException expected = new("boom");
        var task = FirstAsValueTaskHelper<int>.FirstAsValueTask(Observable.Throw<int>(expected));
        var ex = await Assert.That(async () => await task).ThrowsExactly<InvalidOperationException>();
        await Assert.That(ex).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies the helper faults the value task when the source completes empty.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceCompletesEmpty_ThenValueTaskFaultsWithInvalidOperation()
    {
        var task = FirstAsValueTaskHelper<int>.FirstAsValueTask(Observable.Empty<int>());
        await Assert.That(async () => await task).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Verifies the helper throws when the source argument is null.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSourceNull_ThenThrowsArgumentNullException() => await Assert
        .That(static async () => await FirstAsValueTaskHelper<int>.FirstAsValueTask(null!))
        .ThrowsExactly<ArgumentNullException>();

    /// <summary>Exercises the <c>Subscription?.Dispose()</c> null-conditional branch when a source
    /// synchronously emits during <c>Subscribe</c> before the subscription field is assigned.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncSourceEmits_ThenSubscriptionNullBranchSkipsDispose()
    {
        const int Sentinel = 17;
        var value = await FirstAsValueTaskHelper<int>.FirstAsValueTask(Observable.Return(Sentinel));
        await Assert.That(value).IsEqualTo(Sentinel);
    }

    /// <summary>Verifies the pooled source is reused across sequential calls — a second call after the
    /// first has settled returns to the pool and resolves correctly.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCalledSequentially_ThenPooledSourceReused()
    {
        var first = await FirstAsValueTaskHelper<int>.FirstAsValueTask(Observable.Return(FirstValue));
        var second = await FirstAsValueTaskHelper<int>.FirstAsValueTask(Observable.Return(SecondValue));
        await Assert.That(first).IsEqualTo(FirstValue);
        await Assert.That(second).IsEqualTo(SecondValue);
    }

    /// <summary>Verifies awaiting the value task before the source emits registers a continuation on the
    /// pooled source (the incomplete-await path) and resolves once the value later arrives.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAwaitedBeforeEmission_ThenCompletesOnLaterValue()
    {
        Subject<int> subject = new();
        var pending = FirstAsValueTaskHelper<int>.FirstAsValueTask(subject).AsTask();
        subject.OnNext(FirstValue);
        var result = await pending.WaitAsync(GuardTimeout);
        await Assert.That(result).IsEqualTo(FirstValue);
    }

    /// <summary>Verifies emissions arriving after the value task has already settled are silently ignored.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSecondTerminalAfterSettled_ThenIgnored()
    {
        InvasiveObservable<int> source = new();
        var task = FirstAsValueTaskHelper<int>.FirstAsValueTask(source);
        source.Observer.OnNext(FirstValue);
        source.Observer.OnNext(SecondValue);
        source.Observer.OnError(new InvalidOperationException(IgnoredErrorMessage));
        source.Observer.OnCompleted();
        await Assert.That(await task).IsEqualTo(FirstValue);
    }

    /// <summary>Verifies a second <c>OnError</c> arriving after the first is dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSecondOnErrorAfterSettled_ThenIgnored()
    {
        InvasiveObservable<int> source = new();
        var task = FirstAsValueTaskHelper<int>.FirstAsValueTask(source);
        InvalidOperationException expected = new("first");
        source.Observer.OnError(expected);
        source.Observer.OnError(new InvalidOperationException(IgnoredErrorMessage));
        source.Observer.OnCompleted();
        var caught = await Assert.That(async () => await task).ThrowsExactly<InvalidOperationException>();
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies a second <c>OnCompleted</c> arriving after the first is dropped.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSecondOnCompletedAfterSettled_ThenIgnored()
    {
        InvasiveObservable<int> source = new();
        var task = FirstAsValueTaskHelper<int>.FirstAsValueTask(source);
        source.Observer.OnCompleted();
        source.Observer.OnCompleted();
        source.Observer.OnError(new InvalidOperationException(IgnoredErrorMessage));
        await Assert.That(async () => await task).ThrowsExactly<InvalidOperationException>();
    }

    /// <summary>Test observable that captures its subscriber so tests can directly invoke
    /// non-cooperative double-terminal sequences against the pooled first-value observer.</summary>
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
