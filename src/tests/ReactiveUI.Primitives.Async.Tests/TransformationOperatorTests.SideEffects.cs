// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests Do and Tap notification side effects.</summary>
public partial class TransformationOperatorTests
{
    /// <summary>Tests Do wraps the source when only a later callback is supplied, and returns the source for a null callback.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoHasOnlyLaterCallbacks_ThenWrapsSource()
    {
        var source = SignalAsync.Return(1);
        Func<Exception, CancellationToken, ValueTask> onErrorResume = static (_, _) => default;
        Func<Result, ValueTask> onCompleted = static _ => default;

        Func<int, CancellationToken, ValueTask>? noNext = null;
        Func<Exception, CancellationToken, ValueTask>? noErrorResume = null;
        Func<Result, ValueTask>? noCompleted = null;

        await Assert.That(source.Do(noNext, onErrorResume, noCompleted)).IsTypeOf<SignalAsyncExtensions.TapAsyncSignal<int>>();
        await Assert.That(source.Do(noNext, noErrorResume, onCompleted)).IsTypeOf<SignalAsyncExtensions.TapAsyncSignal<int>>();
        await Assert.That(source.Do(noNext, noErrorResume, noCompleted)).IsSameReferenceAs(source);
        await Assert.That(source.Do((Func<int, CancellationToken, ValueTask>?)null)).IsSameReferenceAs(source);
    }

    /// <summary>Tests Tap and Do null callback overloads return the original source.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTapAndDoCallbacksAreNull_ThenReturnSource()
    {
        var source = SignalAsync.Return(1);
        await Assert.That(source.Tap((Func<int, CancellationToken, ValueTask>?)null, null, null))
            .IsSameReferenceAs(source);
        await Assert.That(source.Tap((Action<int>)null!)).IsSameReferenceAs(source);
        await Assert.That(source.Do((Action<int>?)null, null, null)).IsSameReferenceAs(source);
    }

    /// <summary>Tests synchronous Do callbacks receive values, resumable errors and completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoSyncWithAllCallbacks_ThenInvokesAndForwards()
    {
        const int ExpectedFirst = 7;
        const int ExpectedSecond = 8;
        List<int> nextValues = [];
        List<Exception> errors = [];
        List<Result> completions = [];
        TaskCompletionSource errored = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnNextAsync(ExpectedFirst, ct);
            await observer.OnErrorResumeAsync(new InvalidOperationException("resume"), ct);
            await observer.OnNextAsync(ExpectedSecond, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        await using var sub = await source.Do(
            nextValues.Add,
            exception =>
            {
                errors.Add(exception);
                _ = errored.TrySetResult();
            },
            result =>
            {
                completions.Add(result);
                _ = completed.TrySetResult();
            }).SubscribeAsync(static (_, _) => default);
        await Task.WhenAll(errored.Task, completed.Task);
        await Assert.That(nextValues).IsCollectionEqualTo([ExpectedFirst, ExpectedSecond]);
        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(completions).Count().IsEqualTo(1);
    }

    /// <summary>Tests Do with only a completion callback forwards resumable errors.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoSyncWithOnlyCompletionCallbackAndSourceEmitsErrorResume_ThenForwardsError()
    {
        List<Exception> caughtErrors = [];
        List<Result> completions = [];
        TaskCompletionSource done = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException("sync completion-only"), ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        await using var sub = await source.Do((Action<int>?)null, null, completions.Add)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caughtErrors.Add(ex);
                    return default;
                },
                _ =>
                {
                    IgnoredResult.Of(done.TrySetResult());
                    return default;
                });
        await done.Task;
        await Assert.That(caughtErrors).Count().IsEqualTo(1);
        await Assert.That(completions).Count().IsEqualTo(1);
    }

    /// <summary>Tests Tap with only an asynchronous completion callback invokes it.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTapAsyncWithOnlyCompletionCallback_ThenInvokesCallback()
    {
        Result? completion = null;
        await SignalAsync.Return(1).Tap((Func<int, CancellationToken, ValueTask>?)null, null, result =>
        {
            completion = result;
            return default;
        }).WaitCompletionAsync();
        await Assert.That(completion).IsNotNull();
        await Assert.That(completion!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Tests Do without callbacks forwards resumable errors and completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoWithNoCallbacksAndSourceEmitsErrorResume_ThenForwardsBoth()
    {
        Exception? caught = null;
        var completed = false;
        TaskCompletionSource errorTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource completionTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        var source = SignalAsync.Create<int>(static async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException("resume"), ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });
        await using var sub = await source.Do().SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                caught = ex;
                IgnoredResult.Of(errorTcs.TrySetResult());
                return default;
            },
            _ =>
            {
                completed = true;
                IgnoredResult.Of(completionTcs.TrySetResult());
                return default;
            });
        await Task.WhenAll(errorTcs.Task, completionTcs.Task);
        await Assert.That(caught).IsNotNull();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Tests Do without callbacks forwards source values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoWithNoCallbacks_ThenPassesThroughValues()
    {
        const int ExpectedSecond = 2;
        const int ExpectedThird = 3;
        const int SourceValueCount = 3;

        var result = await SignalAsync.Range(1, SourceValueCount).Do().ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([1, ExpectedSecond, ExpectedThird]);
    }

    /// <summary>Tests sync Do invokes side effects.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoSync_ThenInvokesSideEffects()
    {
        const int ExpectedSecond = 2;
        const int ExpectedThird = 3;
        const int SourceValueCount = 3;

        List<int> sideEffects = [];
        var result = await SignalAsync.Range(1, SourceValueCount).Do((x, _) =>
        {
            sideEffects.Add(x);
            return default;
        }).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([1, ExpectedSecond, ExpectedThird]);
        await Assert.That(sideEffects).IsCollectionEqualTo([1, ExpectedSecond, ExpectedThird]);
    }

    /// <summary>Tests async Do invokes side effects.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoAsync_ThenInvokesSideEffects()
    {
        const int ExpectedSecond = 2;
        const int ExpectedThird = 3;
        const int SourceValueCount = 3;

        List<int> sideEffects = [];
        var result = await SignalAsync.Range(1, SourceValueCount).Do(async (x, _) =>
        {
            await Task.Yield();
            sideEffects.Add(x);
        }).ToListAsync();
        await Assert.That(result).IsCollectionEqualTo([1, ExpectedSecond, ExpectedThird]);
        await Assert.That(sideEffects).IsCollectionEqualTo([1, ExpectedSecond, ExpectedThird]);
    }

    /// <summary>Tests Do with completion handler invokes on completed.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDoWithCompletionHandler_ThenInvokesOnCompleted()
    {
        Result? completion = null;
        await SignalAsync.Empty<int>().Do((Action<int>?)null, (Action<Exception>?)null, r => completion = r)
            .WaitCompletionAsync();
        await Assert.That(completion).IsNotNull();
        await Assert.That(completion!.Value.IsSuccess).IsTrue();
    }
}
