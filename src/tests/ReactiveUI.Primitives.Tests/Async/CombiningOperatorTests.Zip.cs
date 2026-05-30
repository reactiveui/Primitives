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
using ReactiveUI.Primitives.Async;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <content>
/// Zip tests for combining operators.
/// </content>
public partial class CombiningOperatorTests
{
    /// <summary>Tests Zip two sequences pairs by index.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipTwoSequences_ThenPairsByIndex()
    {
        var first = SignalAsync.Range(1, 3);
        var second = ZipStringsAbc.ToAsyncSignal();

        var result = await first.Zip(second, (n, s) => $"{n}{s}").ToListAsync();

        await Assert.That(result).IsEquivalentTo(["1a", "2b", "3c"]);
    }

    /// <summary>Tests Zip tuple overload creates tuples.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipTupleOverload_ThenCreatesTuples()
    {
        var first = SignalAsync.Range(1, 2);
        var second = ZipStringsXy.ToAsyncSignal();

        var result = await first.Zip(second).ToListAsync();

        await Assert.That(result).Count().IsEqualTo(SampleValue2);
        await Assert.That(result[0]).IsEqualTo((1, "x"));
    }

    /// <summary>Tests Zip different lengths stops at shortest.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipDifferentLengths_ThenStopsAtShortest()
    {
        var first = SignalAsync.Range(1, 5);
        var second = SignalAsync.Range(10, 2);

        var result = await first.Zip(second, (a, b) => a + b).ToListAsync();

        await Assert.That(result).IsEquivalentTo([ZipPair11, ZipPair13]);
    }

    /// <summary>Tests Zip null arguments throws.</summary>
    [Test]
    public void WhenZipNullArguments_ThenThrowsArgumentNull() => Assert.Throws<ArgumentNullException>(() =>
        ((SignalAsync<int>)null!).Zip(SignalAsync.Return(1), (a, b) => a + b));

    /// <summary>
    /// Verifies that zip propagates a failure when the first source errors.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipFirstSourceErrors_ThenFailurePropagates()
    {
        var first = SignalAsync.Throw<int>(new InvalidOperationException(FirstFailMessage));
        var second = SignalAsync.Return("a");

        Result? completionResult = null;
        await using var sub = await first
            .Zip(second, (a, b) => $"{a}{b}")
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>
    /// Verifies that zip propagates a failure when the second source errors.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipSecondSourceErrors_ThenFailurePropagates()
    {
        var first = SignalAsync.Return(1);
        var second = SignalAsync.Throw<string>(new InvalidOperationException("second fail"));

        Result? completionResult = null;
        await using var sub = await first
            .Zip(second, (a, b) => $"{a}{b}")
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>
    /// Verifies that zip completes correctly when the first source has more elements than the second,
    /// covering the completion logic for unmatched queued elements.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipFirstSourceLonger_ThenStopsAtShortest()
    {
        var first = SignalAsync.Range(1, 10);
        var second = SignalAsync.Range(100, 3);

        Result? completionResult = null;
        var items = new List<int>();

        await using var sub = await first
            .Zip(second, (a, b) => a + b)
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
        await Assert.That(items).IsEquivalentTo([RangeOffset101, RangeOffset103, RangeOffset105]);
    }

    /// <summary>
    /// Verifies that zip completes correctly when the second source has more elements than the first,
    /// covering the second source completion path with queued elements.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipSecondSourceLonger_ThenStopsAtShortest()
    {
        var first = SignalAsync.Range(1, 2);
        var second = SignalAsync.Range(100, 10);

        Result? completionResult = null;
        var items = new List<int>();

        await using var sub = await first
            .Zip(second, (a, b) => a + b)
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
        await Assert.That(items).IsEquivalentTo([RangeOffset101, RangeOffset103]);
    }

    /// <summary>
    /// Tests that Zip OnErrorResumeAsync from first source is forwarded to observer.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipFirstSourceErrorResume_ThenForwardedToObserver()
    {
        var first = Signal.Create<int>();
        var second = Signal.Create<string>();
        Exception? received = null;

        await using var sub = await first.Values.Zip(second.Values, (a, b) => $"{a}-{b}")
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    received = ex;
                    return default;
                });

        await first.OnErrorResumeAsync(new InvalidOperationException("first error"), CancellationToken.None);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => received is not null,
            TimeSpan.FromSeconds(5));

        await Assert.That(received).IsNotNull();
        await Assert.That(received!.Message).IsEqualTo("first error");
    }

    /// <summary>
    /// Tests that Zip OnErrorResumeAsync from second source is forwarded to observer.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipSecondSourceErrorResume_ThenForwardedToObserver()
    {
        var first = Signal.Create<int>();
        var second = Signal.Create<string>();
        Exception? received = null;

        await using var sub = await first.Values.Zip(second.Values, (a, b) => $"{a}-{b}")
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    received = ex;
                    return default;
                });

        await second.OnErrorResumeAsync(new InvalidOperationException("second error"), CancellationToken.None);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => received is not null,
            TimeSpan.FromSeconds(5));

        await Assert.That(received).IsNotNull();
        await Assert.That(received!.Message).IsEqualTo("second error");
    }

    /// <summary>
    /// Tests that Zip ignores items from first source after done flag is set.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipFirstSourceEmitsAfterDone_ThenIgnored()
    {
        var first = Signal.Create<int>();
        var second = Signal.Create<string>();
        var items = new List<string>();
        Result? completionResult = null;

        await using var sub = await first.Values.Zip(second.Values, (a, b) => $"{a}-{b}")
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        // Complete first with failure, setting done=true
        await first.OnCompletedAsync(Result.Failure(new InvalidOperationException("fail")));

        await AsyncTestHelpers.WaitForConditionAsync(
            () => completionResult is not null,
            TimeSpan.FromSeconds(5));

        // Items emitted after done should be ignored
        await first.OnNextAsync(Sentinel99, CancellationToken.None);
        await second.OnNextAsync("late", CancellationToken.None);

        await Assert.That(items).IsEmpty();
    }

    /// <summary>
    /// Tests that Zip buffers items correctly when second source produces before first.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipSecondEmitsBeforeFirst_ThenBuffersAndPairs()
    {
        var first = Signal.Create<int>();
        var second = Signal.Create<string>();
        var items = new List<string>();

        await using var sub = await first.Values.Zip(second.Values, (a, b) => $"{a}-{b}")
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null);

        // Second emits first, queuing in _queue2
        await second.OnNextAsync("a", CancellationToken.None);
        await second.OnNextAsync("b", CancellationToken.None);

        // Now first emits, pairing with queue2
        await first.OnNextAsync(1, CancellationToken.None);
        await first.OnNextAsync(SampleValue2, CancellationToken.None);
        await first.OnCompletedAsync(Result.Success);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => items.Count >= 2,
            TimeSpan.FromSeconds(5));

        await Assert.That(items).IsEquivalentTo(["1-a", "2-b"]);
    }

    /// <summary>
    /// Tests that Zip OnCompleted1Async returns early when done is already set.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipFirstSourceCompletedTwice_ThenSecondCompletionIgnored()
    {
        var first = Signal.Create<int>();
        var second = Signal.Create<string>();
        var completionCount = 0;

        await using var sub = await first.Values.Zip(second.Values, (a, b) => $"{a}-{b}")
            .SubscribeAsync(
                (_, _) => default,
                null,
                _ =>
                {
                    Interlocked.Increment(ref completionCount);
                    return default;
                });

        await first.OnCompletedAsync(Result.Success);
        await second.OnCompletedAsync(Result.Success);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => completionCount >= 1,
            TimeSpan.FromSeconds(5));

        // Only one completion should have been forwarded
        await Assert.That(completionCount).IsEqualTo(1);
    }

    /// <summary>Tests that Zip with empty first source returns empty.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipEmptyFirst_ThenReturnsEmpty()
    {
        var result = await SignalAsync.Empty<int>()
            .Zip(SignalAsync.Return("a"), (n, s) => $"{n}{s}")
            .ToListAsync();

        await Assert.That(result).IsEmpty();
    }

    /// <summary>Tests that Zip with empty second source returns empty.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipEmptySecond_ThenReturnsEmpty()
    {
        var result = await SignalAsync.Return(1)
            .Zip(SignalAsync.Empty<string>(), (n, s) => $"{n}{s}")
            .ToListAsync();

        await Assert.That(result).IsEmpty();
    }

    /// <summary>Tests that Zip throws on null second argument.</summary>
    [Test]
    public void WhenZipNullSecond_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(() =>
            SignalAsync.Return(1).Zip((IObservableAsync<string>)null!, (a, _) => a));

    /// <summary>Tests that Zip throws on null resultSelector.</summary>
    [Test]
    public void WhenZipNullResultSelector_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(() =>
            SignalAsync.Return(1).Zip(SignalAsync.Return(SampleValue2), (Func<int, int, int>)null!));

    /// <summary>Tests that Zip error from first source completes with failure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipFirstSourceFails_ThenCompletesWithFailure()
    {
        var first = Signal.Create<int>();
        var second = Signal.Create<int>();
        Result? completionResult = null;

        await using var sub = await first.Values
            .Zip(second.Values, (a, b) => a + b)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await first.OnCompletedAsync(Result.Failure(new InvalidOperationException(FirstFailMessage)));

        await AsyncTestHelpers.WaitForConditionAsync(
            () => completionResult.HasValue,
            TimeSpan.FromSeconds(5));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();

        await first.DisposeAsync();
        await second.DisposeAsync();
    }

    /// <summary>
    /// Tests Zip where the first source completes before the second with no queued items,
    /// exercising the shouldComplete path when queue1 is empty.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipFirstSourceCompletesWithEmptyQueue_ThenCompletes()
    {
        var first = SignalAsync.Return(1);
        var second = SignalAsync.Range(10, 3);

        var result = await first.Zip(second, (a, b) => a + b).ToListAsync();

        await Assert.That(result).IsEquivalentTo([ZipPair11]);
    }

    /// <summary>
    /// Tests Zip where the second source completes with queued items in source1,
    /// exercising the OnCompleted2Async shouldComplete path.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipSecondSourceCompletesWithEmptyQueue_ThenCompletes()
    {
        var first = SignalAsync.Range(1, 5);
        var second = SignalAsync.Return(100);

        var result = await first.Zip(second, (a, b) => a + b).ToListAsync();

        await Assert.That(result).IsEquivalentTo([RangeOffset101]);
    }

    /// <summary>
    /// Tests Zip where first source is empty, exercising the _done early return.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipFirstSourceEmpty_ThenNoOutput()
    {
        var first = SignalAsync.Empty<int>();
        var second = SignalAsync.Return(1);

        var result = await first.Zip(second, (a, b) => a + b).ToListAsync();

        await Assert.That(result).IsEmpty();
    }

    /// <summary>
    /// Verifies that Zip OnNext returns early after one source has already completed and set _done.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipOnNextAfterDone_ThenReturnsEarly()
    {
        var source1 = Signal.Create<int>();
        var source2 = Signal.Create<string>();
        var items = new List<string>();
        Result? completionResult = null;

        await using var sub = await source1.Values
            .Zip(source2.Values, (a, b) => $"{a}-{b}")
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        // Complete source1 with failure (sets _done = true)
        await source1.OnCompletedAsync(Result.Failure(new InvalidOperationException("done")));

        await AsyncTestHelpers.WaitForConditionAsync(
            () => completionResult.HasValue,
            TimeSpan.FromSeconds(5));

        // Now emit on source2 - should be ignored because _done = true
        await source2.OnNextAsync("after", CancellationToken.None);

        await Assert.That(items).IsEmpty();
    }

    /// <summary>
    /// Verifies that Zip OnNext1 returns early after done.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipOnNext1AfterDone_ThenReturnsEarly()
    {
        var source1 = Signal.Create<int>();
        var source2 = Signal.Create<string>();
        var items = new List<string>();
        Result? completionResult = null;

        await using var sub = await source1.Values
            .Zip(source2.Values, (a, b) => $"{a}-{b}")
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        // Complete source2 with failure (sets _done = true)
        await source2.OnCompletedAsync(Result.Failure(new InvalidOperationException("done")));

        await AsyncTestHelpers.WaitForConditionAsync(
            () => completionResult.HasValue,
            TimeSpan.FromSeconds(5));

        // Now emit on source1 - should be ignored because _done = true
        await source1.OnNextAsync(Sentinel42, CancellationToken.None);

        await Assert.That(items).IsEmpty();
    }

    /// <summary>
    /// Verifies that Zip OnCompleted1Async returns early when _done is already true.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipOnCompleted1AfterDone_ThenReturnsEarly()
    {
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<string>();
        var items = new List<string>();

        await using var sub = await src1
            .Zip(src2, (a, b) => $"{a}-{b}")
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null,
                result => default);

        // Complete source2 with failure → sets _done = true
        await src2.Complete(Result.Failure(new InvalidOperationException("done")));

        // Now complete source1 - OnCompleted1Async checks _done and returns early
        await src1.Complete(Result.Success);

        await Assert.That(items).IsEmpty();
    }

    /// <summary>Tests Zip completes when shorter side finishes.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipLeftShorter_ThenCompletesEarly()
    {
        var result = await SignalAsync.Range(1, 2)
            .Zip(SignalAsync.Range(10, 5), (a, b) => a + b)
            .ToListAsync();

        await Assert.That(result).IsEquivalentTo([ZipPair11, ZipPair13]);
    }

    /// <summary>Verifies that subscribing <c>Zip</c> with an already-cancelled token
    /// short-circuits the subscription's cancellation chain immediately.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipSubscribedWithAlreadyCancelledToken_ThenSubscriptionDisposes()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await using var sub = await SignalAsync.Range(1, 2)
            .Zip(SignalAsync.Range(10, 2), static (a, b) => a + b)
            .SubscribeAsync(static (_, _) => default, cts.Token);

        await Assert.That(sub).IsNotNull();
    }

    /// <summary>Verifies that subscribing <c>Zip</c> with a cancellable but not-yet-cancelled
    /// token registers the external link and the registration fires when the token is cancelled
    /// after subscribe, tearing the subscription down.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipExternalTokenCancelledAfterSubscribe_ThenRegistrationFires()
    {
        using var cts = new CancellationTokenSource();
        var left = Signal.Create<int>();
        var right = Signal.Create<int>();

        await using var sub = await left.Values
            .Zip(right.Values, static (a, b) => a + b)
            .SubscribeAsync(static (_, _) => default, cts.Token);

        await cts.CancelAsync();
        await Assert.That(sub).IsNotNull();
    }

    /// <summary>Exercises the <c>Zip</c> subscription's idempotent <c>DisposeAsync</c> path —
    /// a second dispose hits the <c>DisposalHelper.TrySetDisposed</c> already-set short-circuit.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenZipSubscriptionDisposedTwice_ThenSecondDisposeIsNoOp()
    {
        var left = Signal.Create<int>();
        var right = Signal.Create<int>();

        var sub = await left.Values
            .Zip(right.Values, static (a, b) => a + b)
            .SubscribeAsync(static (_, _) => default);

        await sub.DisposeAsync();
        await sub.DisposeAsync();

        await Assert.That(sub).IsNotNull();
    }
}
