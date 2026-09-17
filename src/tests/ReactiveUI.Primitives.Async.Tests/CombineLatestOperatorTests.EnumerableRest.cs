// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;
using AsyncObs = ReactiveUI.Primitives.Async.SignalAsync;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests the enumerable <c>CombineLatest</c> overload's disposal and completion guards.</summary>
public partial class CombineLatestOperatorTests
{
    /// <summary>Verifies that emitting after an immediate dispose completes without error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableDisposedDuringSubscribe_ThenReturnsEarly()
    {
        var signal = Signal.Create<int>();
        IObservableAsync<int>[] sources = [signal.Values, AsyncObs.Never<int>()];

        var sub = await sources.CombineLatest().SubscribeAsync(
            static (_, _) => default,
            null);

        await sub.DisposeAsync();

        await signal.OnNextAsync(1, CancellationToken.None);
    }

    /// <summary>Verifies that values pushed by either source after disposal are not forwarded.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableOnNextAfterDispose_ThenIgnored()
    {
        const int SecondSignalValue = 2;
        var signal1 = Signal.Create<int>();
        var signal2 = Signal.Create<int>();
        List<IReadOnlyList<int>> items = [];

        IObservableAsync<int>[] sources = [signal1.Values, signal2.Values];

        var sub = await sources.CombineLatest().SubscribeAsync(
            (x, _) =>
            {
                items.Add(x);
                return default;
            },
            null);

        await sub.DisposeAsync();

        await signal1.OnNextAsync(1, CancellationToken.None);
        await signal2.OnNextAsync(SecondSignalValue, CancellationToken.None);

        await Assert.That(items).IsEmpty();
    }

    /// <summary>Verifies that an error resumed after disposal is not forwarded.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableOnErrorResumeAfterDispose_ThenIgnored()
    {
        var signal = Signal.Create<int>();
        List<Exception> errors = [];

        IObservableAsync<int>[] sources = [signal.Values];

        var sub = await sources.CombineLatest().SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                errors.Add(ex);
                return default;
            });

        await sub.DisposeAsync();

        await signal.OnErrorResumeAsync(new InvalidOperationException("err"), CancellationToken.None);

        await Assert.That(errors).IsEmpty();
    }

    /// <summary>Verifies that a source completing after disposal forwards no completion.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableAlreadyDisposed_ThenOnCompletedIgnored()
    {
        var signal1 = Signal.Create<int>();
        var signal2 = Signal.Create<int>();

        IObservableAsync<int>[] sources = [signal1.Values, signal2.Values];
        Result? completion = null;

        var sub = await sources.CombineLatest().SubscribeAsync(
            static (_, _) => default,
            null,
            r =>
            {
                completion = r;
                return default;
            });

        await sub.DisposeAsync();

        await signal1.OnCompletedAsync(Result.Success);

        await Assert.That(completion).IsNull();
    }

    /// <summary>Verifies that an empty source completes the combined sequence during subscription.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableSourceCompletesWithoutValue_ThenCompletes()
    {
        var signal1 = Signal.Create<int>();
        var emptySource = AsyncObs.Empty<int>();

        IObservableAsync<int>[] sources = [signal1.Values, emptySource];
        Result? completion = null;

        await using var sub = await sources.CombineLatest().SubscribeAsync(
            static (_, _) => default,
            null,
            r =>
            {
                completion = r;
                return default;
            });

        await Assert.That(completion is not null).IsTrue();

        await Assert.That(completion).IsNotNull();
        await Assert.That(completion!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that a value arriving while the subscription tears down is dropped.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableOnNextAfterDispose_ThenReturnsEarly()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        IObservableAsync<int>[] sources = [src1, src2];
        List<IReadOnlyList<int>> items = [];
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

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
                    IgnoredResult.Of(completionBlocked.TrySetResult());
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(Source1Value);

        // Blocking inside the completion handler parks the subscription mid-teardown.
        var failTask = src1.Complete(Result.Failure(new InvalidOperationException("test")));
        await completionBlocked.Task;

        await src2.EmitNext(SentinelValue);

        await Assert.That(items).Count().IsEqualTo(1);

        _ = allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>Verifies that an error arriving while the subscription tears down is dropped.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableOnErrorResumeAfterDispose_ThenReturnsEarly()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        IObservableAsync<int>[] sources = [src1, src2];
        List<Exception> errors = [];
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await sources.CombineLatest()
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                },
                async _ =>
                {
                    IgnoredResult.Of(completionBlocked.TrySetResult());
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(Source1Value);

        // Blocking inside the completion handler parks the subscription mid-teardown.
        var failTask = src1.Complete(Result.Failure(new InvalidOperationException("test")));
        await completionBlocked.Task;

        await src2.EmitError(new InvalidOperationException("post-dispose error"));

        await Assert.That(errors).IsEmpty();

        _ = allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>Verifies that completing a source twice yields a single overall completion.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableDoubleComplete_ThenSecondIsIgnored()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        IObservableAsync<int>[] sources = [src1, src2];
        var completionCount = 0;

        await using var sub = await sources.CombineLatest()
            .SubscribeAsync(
                static (_, _) => default,
                null,
                _ =>
                {
                    IgnoredResult.Of(Interlocked.Increment(ref completionCount));
                    return default;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(Source1Value);

        await src1.Complete(Result.Success);
        await src1.Complete(Result.Success);
        await src2.Complete(Result.Success);

        await Assert.That(completionCount).IsEqualTo(1);
    }

    /// <summary>Verifies that a source completing without a value completes the sequence at once.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableSourceCompletesWithoutValue_ThenCompletesImmediately()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        IObservableAsync<int>[] sources = [src1, src2];
        Result? completionResult = null;

        await using var sub = await sources.CombineLatest()
            .SubscribeAsync(
                static (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await src1.Complete(Result.Success);

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that cancelling mid-subscribe throws and leaves later sources unsubscribed.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableDisposedDuringSubscribeLoop_ThenReturnsEarly()
    {
        TaskCompletionSource entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var slowSource = AsyncObs.Create<int>(async (_, ct) =>
        {
            IgnoredResult.Of(entered.TrySetResult());
            await release.Task.WaitAsync(ct);
            return DisposableAsync.Empty;
        });

        var normalSubscribed = false;
        var normalSource = AsyncObs.Create<int>((_, _) =>
        {
            normalSubscribed = true;
            return new(DisposableAsync.Empty);
        });
        IObservableAsync<int>[] sources = [slowSource, normalSource];
        using CancellationTokenSource cts = new();
        var pending = sources.CombineLatest()
            .SubscribeAsync(static (_, _) => default, null, null, cts.Token);
        await entered.Task;
        await cts.CancelAsync();
        await Assert.That(async () => await pending).Throws<OperationCanceledException>();
        await Assert.That(normalSubscribed).IsFalse();
    }

    /// <summary>Verifies that a first source completing during subscribe skips subscribing the rest.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableFirstSourceCompletesImmediately_ThenSkipsRemainingSubscriptions()
    {
        var secondSourceSubscribed = false;

        var trackingSource = AsyncObs.Create<int>((_, _) =>
        {
            secondSourceSubscribed = true;
            return new(DisposableAsync.Empty);
        });

        IObservableAsync<int>[] sources = [AsyncObs.Empty<int>(), trackingSource];

        await using var sub = await sources.CombineLatest()
            .SubscribeAsync(static (_, _) => default, null);

        await Assert.That(secondSourceSubscribed).IsFalse();
    }

    /// <summary>Verifies that a repeated completion from one of three sources is not counted twice.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableSameSourceCompletedTwice_ThenSecondCompletionIsIgnored()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        DirectSource<int> src3 = new();
        IObservableAsync<int>[] sources = [src1, src2, src3];
        var completionCount = 0;

        await using var sub = await sources.CombineLatest()
            .SubscribeAsync(
                static (_, _) => default,
                null,
                _ =>
                {
                    IgnoredResult.Of(Interlocked.Increment(ref completionCount));
                    return default;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(Source1Value);
        await src3.EmitNext(Source2Value);

        await src1.Complete(Result.Success);
        await src1.Complete(Result.Success);

        await Assert.That(completionCount).IsEqualTo(0);

        await src2.Complete(Result.Success);
        await src3.Complete(Result.Success);

        await Assert.That(completionCount).IsEqualTo(1);
    }

    /// <summary>Verifies that a completed source that had emitted keeps the sequence open while others run.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableSourceWithValueCompletes_ThenDoesNotCompleteUntilAllDone()
    {
        const int Src1FirstValue = 10;
        const int Src2FirstValue = 20;
        const int Src2SecondValue = 30;
        const int ExpectedEmissions = 2;
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        IObservableAsync<int>[] sources = [src1, src2];
        Result? completionResult = null;
        List<IReadOnlyList<int>> emissions = [];

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

        await src1.EmitNext(Src1FirstValue);
        await src2.EmitNext(Src2FirstValue);

        await Assert.That(emissions).Count().IsEqualTo(1);

        await src1.Complete(Result.Success);

        await Assert.That(completionResult).IsNull();

        // src2 combines against the last value of the completed src1.
        await src2.EmitNext(Src2SecondValue);

        await Assert.That(emissions).Count().IsEqualTo(ExpectedEmissions);
        await Assert.That(emissions[1][0]).IsEqualTo(Src1FirstValue);
        await Assert.That(emissions[1][1]).IsEqualTo(Src2SecondValue);

        await src2.Complete(Result.Success);

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that a repeated completion leaves the remaining source free to emit.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task
        WhenCombineLatestEnumerableDuplicateCompletionForSameIndex_ThenIgnoredAndRemainingSourceStillEmits()
    {
        const int Src2LateValue = 5;
        const int ExpectedEmissions = 2;
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        IObservableAsync<int>[] sources = [src1, src2];
        List<IReadOnlyList<int>> emissions = [];
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

        await src1.EmitNext(1);
        await src2.EmitNext(Source1Value);

        await Assert.That(emissions).Count().IsEqualTo(1);

        await src1.Complete(Result.Success);

        await Assert.That(completionResult).IsNull();

        await src1.Complete(Result.Success);

        await Assert.That(completionResult).IsNull();

        await src2.EmitNext(Src2LateValue);

        await Assert.That(emissions).Count().IsEqualTo(ExpectedEmissions);
        await Assert.That(emissions[1][0]).IsEqualTo(1);
        await Assert.That(emissions[1][1]).IsEqualTo(Src2LateValue);

        await src2.Complete(Result.Success);

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that a middle source completing without a value completes the sequence with no snapshot.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEnumerableMiddleSourceCompletesWithoutEmitting_ThenCompletesImmediately()
    {
        const int Src1Value = 10;
        const int Src3Value = 30;
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        DirectSource<int> src3 = new();
        IObservableAsync<int>[] sources = [src1, src2, src3];
        List<IReadOnlyList<int>> emissions = [];
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

        await src1.EmitNext(Src1Value);
        await src3.EmitNext(Src3Value);

        await Assert.That(emissions).IsEmpty();

        await src2.Complete(Result.Success);

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();

        await Assert.That(emissions).IsEmpty();
    }
}
