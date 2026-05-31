// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;
using AsyncObs = ReactiveUI.Primitives.Async.SignalAsync;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for CombineLatestOperatorTests.</summary>
public partial class CombineLatestOperatorTests
{
    /// <summary>
    /// Tests CombineLatest enumerable returns early from SubscribeAsync when disposed during subscribe,
    /// exercising the early return path in CombineLatestEnumerable.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableDisposedDuringSubscribe_ThenReturnsEarly()
    {
        var signal = Signal.Create<int>();
        IObservableAsync<int>[] sources = [signal.Values, AsyncObs.Never<int>()];

        var sub = await sources.CombineLatest().SubscribeAsync(
            (_, _) => default,
            null);

        // Dispose immediately, before any values
        await sub.DisposeAsync();

        // Emit after dispose - should be ignored
        await signal.OnNextAsync(1, CancellationToken.None);
    }

    /// <summary>
    /// Tests CombineLatest enumerable OnNextAsync returns early when disposed,
    /// exercising the OnNextAsync disposed guard in CombineLatestEnumerable.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableOnNextAfterDispose_ThenIgnored()
    {
        const int SecondSignalValue = 2;
        var signal1 = Signal.Create<int>();
        var signal2 = Signal.Create<int>();
        var items = new List<IReadOnlyList<int>>();

        IObservableAsync<int>[] sources = [signal1.Values, signal2.Values];

        var sub = await sources.CombineLatest().SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        await sub.DisposeAsync();

        // These should be ignored
        await signal1.OnNextAsync(1, CancellationToken.None);
        await signal2.OnNextAsync(SecondSignalValue, CancellationToken.None);

        await Assert.That(items).IsEmpty();
    }

    /// <summary>
    /// Tests CombineLatest enumerable OnErrorResumeAsync returns early when disposed,
    /// exercising the OnErrorResumeAsync disposed guard in CombineLatestEnumerable.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableOnErrorResumeAfterDispose_ThenIgnored()
    {
        var signal = Signal.Create<int>();
        var errors = new List<Exception>();

        IObservableAsync<int>[] sources = [signal.Values];

        var sub = await sources.CombineLatest().SubscribeAsync(
            (_, _) => default,
            (ex, _) =>
            {
                errors.Add(ex);
                return default;
            });

        await sub.DisposeAsync();

        // Error after dispose should be ignored
        await signal.OnErrorResumeAsync(new InvalidOperationException("err"), CancellationToken.None);

        await Assert.That(errors).IsEmpty();
    }

    /// <summary>
    /// Tests CombineLatest enumerable OnCompletedAsync returns early when already completed for same index,
    /// exercising the already-completed guard in CombineLatestEnumerable.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableAlreadyDisposed_ThenOnCompletedIgnored()
    {
        var signal1 = Signal.Create<int>();
        var signal2 = Signal.Create<int>();

        IObservableAsync<int>[] sources = [signal1.Values, signal2.Values];
        Result? completion = null;

        var sub = await sources.CombineLatest().SubscribeAsync(
            (_, _) => default,
            null,
            r =>
            {
                completion = r;
                return default;
            });

        // Dispose first
        await sub.DisposeAsync();

        // Complete after dispose - should be ignored
        await signal1.OnCompletedAsync(Result.Success);

        // No extra completion should have been forwarded since we disposed
        await Assert.That(completion).IsNull();
    }

    /// <summary>
    /// Tests CombineLatest enumerable completes when a source completes without emitting a value,
    /// exercising the shouldComplete path in CombineLatestEnumerable.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableSourceCompletesWithoutValue_ThenCompletes()
    {
        var signal1 = Signal.Create<int>();
        var emptySource = AsyncObs.Empty<int>();

        IObservableAsync<int>[] sources = [signal1.Values, emptySource];
        Result? completion = null;

        await using var sub = await sources.CombineLatest().SubscribeAsync(
            (_, _) => default,
            null,
            r =>
            {
                completion = r;
                return default;
            });

        await AsyncTestHelpers.WaitForConditionAsync(
            () => completion is not null,
            TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(completion).IsNotNull();
        await Assert.That(completion!.Value.IsSuccess).IsTrue();
    }

    /// <summary>
    /// Verifies that CombineLatestEnumerable OnNextAsync returns early when disposed.
    /// Uses the blocking-OnCompletedAsync technique to keep the gate alive while _disposed is set.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableOnNextAfterDispose_ThenReturnsEarly()
    {
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        IObservableAsync<int>[] sources = [src1, src2];
        var items = new List<IReadOnlyList<int>>();
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await sources.CombineLatest()
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null,
                async _ =>
                {
                    completionBlocked.TrySetResult();
                    await allowCompletion.Task;
                });

        // Set up initial values
        await src1.EmitNext(1);
        await src2.EmitNext(Source1Value);

        // Trigger failure on src1 → CompleteAsync → _disposed=1 → blocks on OnCompletedAsync
        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        // _disposed is 1, gate still alive → OnNextAsync should hit the guard
        await src2.EmitNext(SentinelValue);

        await Assert.That(items).Count().IsEqualTo(1);

        allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that CombineLatestEnumerable OnErrorResumeAsync returns early when disposed.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableOnErrorResumeAfterDispose_ThenReturnsEarly()
    {
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        IObservableAsync<int>[] sources = [src1, src2];
        var errors = new List<Exception>();
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await sources.CombineLatest()
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                },
                async _ =>
                {
                    completionBlocked.TrySetResult();
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(Source1Value);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        // _disposed is 1, gate still alive → OnErrorResumeAsync should hit the guard
        await src2.EmitError(new InvalidOperationException("post-dispose error"));

        await Assert.That(errors).IsEmpty();

        allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that CombineLatestEnumerable OnCompleted returns early for an already-completed index.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableDoubleComplete_ThenSecondIsIgnored()
    {
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        IObservableAsync<int>[] sources = [src1, src2];
        var completionCount = 0;

        await using var sub = await sources.CombineLatest()
            .SubscribeAsync(
                (_, _) => default,
                null,
                _ =>
                {
                    Interlocked.Increment(ref completionCount);
                    return default;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(Source1Value);

        // Complete src1 (has emitted, src2 still active → no overall completion)
        await src1.Complete(Result.Success);

        // Complete src1 again - already completed[0] = true, returns early
        await src1.Complete(Result.Success);

        // Now complete src2 → overall completion
        await src2.Complete(Result.Success);

        await Assert.That(completionCount).IsEqualTo(1);
    }

    /// <summary>
    /// Verifies that CombineLatestEnumerable completes when a source completes without emitting.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableSourceCompletesWithoutValue_ThenCompletesImmediately()
    {
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        IObservableAsync<int>[] sources = [src1, src2];
        Result? completionResult = null;

        await using var sub = await sources.CombineLatest()
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        // src1 completes without ever emitting, so shouldComplete = !_values[0].HasValue = true
        await src1.Complete(Result.Success);

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>
    /// Verifies that CombineLatestEnumerable returns early when disposed during subscribe loop.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableDisposedDuringSubscribeLoop_ThenReturnsEarly()
    {
        // First source triggers disposal when subscribed
        var disposeTrigger =
            new TaskCompletionSource<IAsyncDisposable>(TaskCreationOptions.RunContinuationsAsynchronously);
        var slowSource = AsyncObs.Create<int>(async (_, ct) =>
        {
            var disp = await disposeTrigger.Task.WaitAsync(ct);
            await disp.DisposeAsync();
            return DisposableAsync.Empty;
        });

        var normalSource = new DirectSource<int>();
        IObservableAsync<int>[] sources = [slowSource, normalSource];

        // Cancel after 1s, not WaitTimeoutSeconds (5s): this test pure-waits for cancellation
        // by design (nothing ever sets disposeTrigger) — the cancellation is the only exit,
        // so we want the shortest window that reliably lets the subscribe loop start. 1s is
        // safe even on slow CI runners.
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(1));

        try
        {
            var sub = await sources.CombineLatest()
                .SubscribeAsync((_, _) => default, null, null, cts.Token);
            disposeTrigger.SetResult(sub);
            await sub.DisposeAsync();
        }
        catch (OperationCanceledException)
        {
            // Expected
        }
    }

    /// <summary>
    /// Verifies that SubscribeAsync bails out of the source subscription loop when an
    /// earlier source completes without emitting (triggering overall completion and
    /// cancelling the dispose CTS) before remaining sources are subscribed,
    /// covering the early-return guard in CombineLatestEnumerable.SubscribeAsync.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableFirstSourceCompletesImmediately_ThenSkipsRemainingSubscriptions()
    {
        var secondSourceSubscribed = false;

        // Second source records whether it was ever subscribed.
        var trackingSource = AsyncObs.Create<int>((_, _) =>
        {
            secondSourceSubscribed = true;
            return new(DisposableAsync.Empty);
        });

        // Empty completes immediately during subscribe, triggering overall completion
        // before the loop reaches the second source.
        IObservableAsync<int>[] sources = [AsyncObs.Empty<int>(), trackingSource];

        await using var sub = await sources.CombineLatest()
            .SubscribeAsync((_, _) => default, null);

        await Assert.That(secondSourceSubscribed).IsFalse();
    }

    /// <summary>
    /// Verifies that OnCompletedAsync returns early when the same source index completes
    /// a second time while the subscription is still active (the _completed[index] guard),
    /// covering line 268 in CombineLatestEnumerable.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableSameSourceCompletedTwice_ThenSecondCompletionIsIgnored()
    {
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        var src3 = new DirectSource<int>();
        IObservableAsync<int>[] sources = [src1, src2, src3];
        var completionCount = 0;

        await using var sub = await sources.CombineLatest()
            .SubscribeAsync(
                (_, _) => default,
                null,
                _ =>
                {
                    Interlocked.Increment(ref completionCount);
                    return default;
                });

        // All sources emit so _values[i].HasValue is true for all.
        await src1.EmitNext(1);
        await src2.EmitNext(Source1Value);
        await src3.EmitNext(Source2Value);

        // src1 completes: _completed[0]=true, completedCount=1 (not 3), shouldComplete=false.
        await src1.Complete(Result.Success);

        // src1 completes again: _completed[0] is already true, returns early (line 268).
        await src1.Complete(Result.Success);

        await Assert.That(completionCount).IsEqualTo(0);

        // Finish: complete remaining sources so the subscription terminates cleanly.
        await src2.Complete(Result.Success);
        await src3.Complete(Result.Success);

        await Assert.That(completionCount).IsEqualTo(1);
    }

    /// <summary>
    /// Verifies that OnCompletedAsync takes the non-completing path (shouldComplete=false)
    /// when a source that has already emitted a value completes while other sources remain active,
    /// covering line 277 in CombineLatestEnumerable.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableSourceWithValueCompletes_ThenDoesNotCompleteUntilAllDone()
    {
        const int Src1FirstValue = 10;
        const int Src2FirstValue = 20;
        const int Src2SecondValue = 30;
        const int ExpectedEmissions = 2;
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        IObservableAsync<int>[] sources = [src1, src2];
        Result? completionResult = null;
        var emissions = new List<IReadOnlyList<int>>();

        await using var sub = await sources.CombineLatest()
            .SubscribeAsync(
                (snapshot, _) =>
                {
                    emissions.Add(snapshot);
                    return default;
                },
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        // Both emit so _values[i].HasValue is true for both.
        await src1.EmitNext(Src1FirstValue);
        await src2.EmitNext(Src2FirstValue);

        await Assert.That(emissions).Count().IsEqualTo(1);

        // src1 completes: _values[0].HasValue=true, completedCount=1 (not 2) → shouldComplete=false (line 277).
        await src1.Complete(Result.Success);

        // Subscription is still active because shouldComplete was false.
        await Assert.That(completionResult).IsNull();

        // src2 can still emit and combine with src1's last value.
        await src2.EmitNext(Src2SecondValue);

        await Assert.That(emissions).Count().IsEqualTo(ExpectedEmissions);
        await Assert.That(emissions[1][0]).IsEqualTo(Src1FirstValue);
        await Assert.That(emissions[1][1]).IsEqualTo(Src2SecondValue);

        // Complete src2 → all sources completed, shouldComplete=true.
        await src2.Complete(Result.Success);

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>
    /// Verifies that OnCompletedAsync returns early without incrementing the completed count
    /// when the same source index has already completed, exercising the _completed[index] guard
    /// on line 268 of CombineLatestEnumerable with a single additional source still active.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task
        WhenCombineLatestEnumerableDuplicateCompletionForSameIndex_ThenIgnoredAndRemainingSourceStillEmits()
    {
        const int Src2LateValue = 5;
        const int ExpectedEmissions = 2;
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        IObservableAsync<int>[] sources = [src1, src2];
        var emissions = new List<IReadOnlyList<int>>();
        Result? completionResult = null;

        await using var sub = await sources.CombineLatest()
            .SubscribeAsync(
                (snapshot, _) =>
                {
                    emissions.Add(snapshot);
                    return default;
                },
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        // Both sources emit so all _values have values.
        await src1.EmitNext(1);
        await src2.EmitNext(Source1Value);

        await Assert.That(emissions).Count().IsEqualTo(1);

        // src1 completes: _completed[0]=true, completedCount=1, shouldComplete=false.
        await src1.Complete(Result.Success);

        await Assert.That(completionResult).IsNull();

        // Duplicate completion for index 0: _completed[0] is already true, returns default (line 268).
        await src1.Complete(Result.Success);

        // Still no overall completion because src2 hasn't completed.
        await Assert.That(completionResult).IsNull();

        // src2 can still emit; the duplicate completion did not corrupt state.
        await src2.EmitNext(Src2LateValue);

        await Assert.That(emissions).Count().IsEqualTo(ExpectedEmissions);
        await Assert.That(emissions[1][0]).IsEqualTo(1);
        await Assert.That(emissions[1][1]).IsEqualTo(Src2LateValue);

        // Clean termination.
        await src2.Complete(Result.Success);

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>
    /// Verifies that when one source in a three-source CombineLatest completes without ever
    /// emitting a value, the shouldComplete path triggers immediate completion and the remaining
    /// sources are torn down, exercising line 277 of CombineLatestEnumerable.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableMiddleSourceCompletesWithoutEmitting_ThenCompletesImmediately()
    {
        const int Src1Value = 10;
        const int Src3Value = 30;
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        var src3 = new DirectSource<int>();
        IObservableAsync<int>[] sources = [src1, src2, src3];
        var emissions = new List<IReadOnlyList<int>>();
        Result? completionResult = null;

        await using var sub = await sources.CombineLatest()
            .SubscribeAsync(
                (snapshot, _) =>
                {
                    emissions.Add(snapshot);
                    return default;
                },
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        // src1 and src3 emit, but src2 never emits.
        await src1.EmitNext(Src1Value);
        await src3.EmitNext(Src3Value);

        // No snapshot yet because src2 has not emitted.
        await Assert.That(emissions).IsEmpty();

        // src2 completes without emitting: !_values[1].HasValue is true → shouldComplete=true (line 277).
        await src2.Complete(Result.Success);

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();

        // No snapshots were ever emitted because not all sources had values.
        await Assert.That(emissions).IsEmpty();
    }
}
