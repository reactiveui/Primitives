// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.SystemReactiveBridge;
using System.Reactive;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.Collections.Immutable;
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Async.Internals;
using ReactiveUI.Primitives.Async.Subjects;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Coverage for the static fan-out helpers in
/// <see cref="Concurrent"/> — exercises empty / single / multi-observer paths and the
/// slow-path that uses <see cref="Task.WhenAll(System.Threading.Tasks.Task[])"/> when at
/// least one observer's <see cref="ValueTask"/> hasn't completed synchronously.</summary>
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "TUnit requires instance methods")]
public class ConcurrentSubjectBaseTests
{
    /// <summary>Value forwarded by the <c>OnNext</c> fan-out tests.</summary>
    private const int ForwardedValue = 42;

    /// <summary>Delay in milliseconds used to force the slow-path branch.</summary>
    private const int SlowPathDelayMilliseconds = 5;

    /// <summary>Verifies that <c>ForwardOnNextConcurrently</c> with an empty observer list returns immediately.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForwardOnNextEmpty_ThenCompletesImmediately()
    {
        var empty = ImmutableArray<IObserverAsync<int>>.Empty;

        await Concurrent.ForwardOnNextConcurrently(empty, ForwardedValue, default);

        // No observers → nothing to assert beyond reaching this line without throwing.
        await Assert.That(empty.Length).IsEqualTo(0);
    }

    /// <summary>Verifies that <c>ForwardOnNextConcurrently</c> with a single observer forwards once.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForwardOnNextSingleObserver_ThenForwardsOnce()
    {
        var capture = new IntCapture();
        var observers = ImmutableArray.Create<IObserverAsync<int>>(MakeSync(capture));

        await Concurrent.ForwardOnNextConcurrently(observers, ForwardedValue, default);

        await Assert.That(capture.Value).IsEqualTo(ForwardedValue);
    }

    /// <summary>Verifies that the synchronous-fast-path branch fires every observer when each
    /// returns a synchronously-completed <see cref="ValueTask"/>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForwardOnNextMultipleSync_ThenAllReceive()
    {
        var a = new IntCapture();
        var b = new IntCapture();
        var c = new IntCapture();

        var observers = ImmutableArray.Create<IObserverAsync<int>>(
            MakeSync(a),
            MakeSync(b),
            MakeSync(c));

        await Concurrent.ForwardOnNextConcurrently(observers, ForwardedValue, default);

        await Assert.That(a.Value).IsEqualTo(ForwardedValue);
        await Assert.That(b.Value).IsEqualTo(ForwardedValue);
        await Assert.That(c.Value).IsEqualTo(ForwardedValue);
    }

    /// <summary>Verifies that the slow-path branch (Task.WhenAll) runs every observer when
    /// any of them returns a non-completed <see cref="ValueTask"/>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForwardOnNextSlowPath_ThenWhenAllForwarded()
    {
        var a = new IntCapture();
        var b = new IntCapture();
        var c = new IntCapture();

        var observers = ImmutableArray.Create<IObserverAsync<int>>(
            MakeSlow(a),
            MakeSync(b),
            MakeSlow(c));

        await Concurrent.ForwardOnNextConcurrently(observers, ForwardedValue, default);

        await Assert.That(a.Value).IsEqualTo(ForwardedValue);
        await Assert.That(b.Value).IsEqualTo(ForwardedValue);
        await Assert.That(c.Value).IsEqualTo(ForwardedValue);
    }

    /// <summary>Verifies the empty / single / slow-path branches of <c>ForwardOnErrorResumeConcurrently</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForwardOnErrorResume_ThenAllBranchesForward()
    {
        var emptyObservers = ImmutableArray<IObserverAsync<int>>.Empty;
        await Concurrent.ForwardOnErrorResumeConcurrently(emptyObservers, new InvalidOperationException("empty"), default);

        var singleCaught = new ErrorCapture();
        var single = ImmutableArray.Create<IObserverAsync<int>>(
            new AnonymousObserverAsync<int>(static (_, _) => default, MakeErrorSync(singleCaught)));
        var singleError = new InvalidOperationException("single");
        await Concurrent.ForwardOnErrorResumeConcurrently(single, singleError, default);
        await Assert.That(singleCaught.Error).IsSameReferenceAs(singleError);

        var a = new ErrorCapture();
        var b = new ErrorCapture();
        var multi = ImmutableArray.Create<IObserverAsync<int>>(
            new AnonymousObserverAsync<int>(static (_, _) => default, MakeErrorSlow(a)),
            new AnonymousObserverAsync<int>(static (_, _) => default, MakeErrorSync(b)));
        var multiError = new InvalidOperationException("multi");
        await Concurrent.ForwardOnErrorResumeConcurrently(multi, multiError, default);
        await Assert.That(a.Error).IsSameReferenceAs(multiError);
        await Assert.That(b.Error).IsSameReferenceAs(multiError);
    }

    /// <summary>Verifies the empty / single / slow-path branches of <c>ForwardOnCompletedConcurrently</c>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenForwardOnCompleted_ThenAllBranchesForward()
    {
        var emptyObservers = ImmutableArray<IObserverAsync<int>>.Empty;
        await Concurrent.ForwardOnCompletedConcurrently(emptyObservers, Result.Success);

        var singleResult = new ResultCapture();
        var single = ImmutableArray.Create<IObserverAsync<int>>(
            new AnonymousObserverAsync<int>(static (_, _) => default, null, MakeCompletedSync(singleResult)));
        await Concurrent.ForwardOnCompletedConcurrently(single, Result.Success);
        await Assert.That(singleResult.Result).IsEqualTo(Result.Success);

        var a = new ResultCapture();
        var b = new ResultCapture();
        var multi = ImmutableArray.Create<IObserverAsync<int>>(
            new AnonymousObserverAsync<int>(static (_, _) => default, null, MakeCompletedSlow(a)),
            new AnonymousObserverAsync<int>(static (_, _) => default, null, MakeCompletedSync(b)));
        await Concurrent.ForwardOnCompletedConcurrently(multi, Result.Success);
        await Assert.That(a.Result).IsEqualTo(Result.Success);
        await Assert.That(b.Result).IsEqualTo(Result.Success);
    }

    /// <summary>Creates a synchronously-completing OnNext observer that captures the value.</summary>
    /// <param name="capture">The capture sink.</param>
    /// <returns>An observer whose <c>OnNextAsync</c> completes synchronously.</returns>
    private static AnonymousObserverAsync<int> MakeSync(IntCapture capture) =>
        new((x, _) =>
        {
            capture.Value = x;
            return default;
        });

    /// <summary>Creates an OnNext observer that delays before capturing — forces the slow path.</summary>
    /// <param name="capture">The capture sink.</param>
    /// <returns>An observer whose <c>OnNextAsync</c> completes asynchronously.</returns>
    private static AnonymousObserverAsync<int> MakeSlow(IntCapture capture) =>
        new(async (x, ct) =>
        {
            await Task.Delay(SlowPathDelayMilliseconds, ct).ConfigureAwait(false);
            capture.Value = x;
        });

    /// <summary>Synchronously-completing OnErrorResume handler that records the exception.</summary>
    /// <param name="capture">The capture sink.</param>
    /// <returns>An OnErrorResume delegate.</returns>
    private static Func<Exception, CancellationToken, ValueTask> MakeErrorSync(ErrorCapture capture) =>
        (ex, _) =>
        {
            capture.Error = ex;
            return default;
        };

    /// <summary>OnErrorResume handler that delays before recording — forces the slow path.</summary>
    /// <param name="capture">The capture sink.</param>
    /// <returns>An OnErrorResume delegate.</returns>
    private static Func<Exception, CancellationToken, ValueTask> MakeErrorSlow(ErrorCapture capture) =>
        async (ex, ct) =>
        {
            await Task.Delay(SlowPathDelayMilliseconds, ct).ConfigureAwait(false);
            capture.Error = ex;
        };

    /// <summary>Synchronously-completing OnCompleted handler that records the result.</summary>
    /// <param name="capture">The capture sink.</param>
    /// <returns>An OnCompleted delegate.</returns>
    private static Func<Result, ValueTask> MakeCompletedSync(ResultCapture capture) =>
        r =>
        {
            capture.Result = r;
            return default;
        };

    /// <summary>OnCompleted handler that delays before recording — forces the slow path.</summary>
    /// <param name="capture">The capture sink.</param>
    /// <returns>An OnCompleted delegate.</returns>
    private static Func<Result, ValueTask> MakeCompletedSlow(ResultCapture capture) =>
        async r =>
        {
            await Task.Delay(SlowPathDelayMilliseconds, CancellationToken.None).ConfigureAwait(false);
            capture.Result = r;
        };

    /// <summary>Mutable holder for an <see cref="int"/> captured by an observer delegate.</summary>
    private sealed class IntCapture
    {
        /// <summary>Gets or sets the captured value.</summary>
        public int Value { get; set; }
    }

    /// <summary>Mutable holder for an <see cref="Exception"/> captured by an observer delegate.</summary>
    private sealed class ErrorCapture
    {
        /// <summary>Gets or sets the captured exception.</summary>
        public Exception? Error { get; set; }
    }

    /// <summary>Mutable holder for a <see cref="Result"/> captured by an observer delegate.</summary>
    private sealed class ResultCapture
    {
        /// <summary>Gets or sets the captured result.</summary>
        public Result? Result { get; set; }
    }
}
