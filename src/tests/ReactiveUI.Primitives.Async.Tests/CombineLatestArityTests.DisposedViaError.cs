// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for CombineLatestArityTests.</summary>
public partial class CombineLatestArityTests
{
    /// <summary>String literal "ignored" used by multiple tests.</summary>
    private const string IgnoredMessage = "ignored";

    /// <summary>
    /// Verifies that CombineLatest2 OnNextCombined guard returns early when disposal is triggered
    /// by an error on source 1 while the downstream OnCompletedAsync is still running.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest2DisposedViaError_ThenOnNextCombinedGuardHits()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        List<int> results = [];
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, static (a, b) => a + b)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null,
                async _ =>
                {
                    IgnoredResult.Of(completionBlocked.TrySetResult());
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(SeedValue2);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        await src2.EmitNext(PostDisposeValue);

        await Assert.That(results).Count().IsEqualTo(1);

        _ = allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that CombineLatest2 OnErrorResume guard returns early when disposal is triggered
    /// by an error on source 1 while the downstream OnCompletedAsync is still running.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest2DisposedViaError_ThenOnErrorResumeGuardHits()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        Exception? receivedError = null;
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, static (a, b) => a + b)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    receivedError = ex;
                    return default;
                },
                async _ =>
                {
                    IgnoredResult.Of(completionBlocked.TrySetResult());
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(SeedValue2);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        await src2.EmitError(new InvalidOperationException(IgnoredMessage));

        await Assert.That(receivedError).IsNull();

        _ = allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that CombineLatest3 OnNextCombined guard returns early when disposal is triggered
    /// by an error on source 1 while the downstream OnCompletedAsync is still running.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest3DisposedViaError_ThenOnNextCombinedGuardHits()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        DirectSource<int> src3 = new();
        List<int> results = [];
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, static (a, b, c) => a + b + c)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null,
                async _ =>
                {
                    IgnoredResult.Of(completionBlocked.TrySetResult());
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(SeedValue2);
        await src3.EmitNext(SeedValue3);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        await src3.EmitNext(PostDisposeValue);

        await Assert.That(results).Count().IsEqualTo(1);

        _ = allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that CombineLatest3 OnErrorResume guard returns early when disposal is triggered
    /// by an error on source 1 while the downstream OnCompletedAsync is still running.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest3DisposedViaError_ThenOnErrorResumeGuardHits()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        DirectSource<int> src3 = new();
        Exception? receivedError = null;
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, static (a, b, c) => a + b + c)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    receivedError = ex;
                    return default;
                },
                async _ =>
                {
                    IgnoredResult.Of(completionBlocked.TrySetResult());
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(SeedValue2);
        await src3.EmitNext(SeedValue3);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        await src3.EmitError(new InvalidOperationException(IgnoredMessage));

        await Assert.That(receivedError).IsNull();

        _ = allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that CombineLatest4 OnNextCombined guard returns early when disposal is triggered
    /// by an error on source 1 while the downstream OnCompletedAsync is still running.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest4DisposedViaError_ThenOnNextCombinedGuardHits()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        DirectSource<int> src3 = new();
        DirectSource<int> src4 = new();
        List<int> results = [];
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, src4, static (a, b, c, d) => a + b + c + d)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null,
                async _ =>
                {
                    IgnoredResult.Of(completionBlocked.TrySetResult());
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(SeedValue2);
        await src3.EmitNext(SeedValue3);
        await src4.EmitNext(SeedValue4);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        await src4.EmitNext(PostDisposeValue);

        await Assert.That(results).Count().IsEqualTo(1);

        _ = allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that CombineLatest4 OnErrorResume guard returns early when disposal is triggered
    /// by an error on source 1 while the downstream OnCompletedAsync is still running.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest4DisposedViaError_ThenOnErrorResumeGuardHits()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        DirectSource<int> src3 = new();
        DirectSource<int> src4 = new();
        Exception? receivedError = null;
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, src4, static (a, b, c, d) => a + b + c + d)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    receivedError = ex;
                    return default;
                },
                async _ =>
                {
                    IgnoredResult.Of(completionBlocked.TrySetResult());
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(SeedValue2);
        await src3.EmitNext(SeedValue3);
        await src4.EmitNext(SeedValue4);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        await src4.EmitError(new InvalidOperationException(IgnoredMessage));

        await Assert.That(receivedError).IsNull();

        _ = allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that CombineLatest5 OnNextCombined guard returns early when disposal is triggered
    /// by an error on source 1 while the downstream OnCompletedAsync is still running.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest5DisposedViaError_ThenOnNextCombinedGuardHits()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        DirectSource<int> src3 = new();
        DirectSource<int> src4 = new();
        DirectSource<int> src5 = new();
        List<int> results = [];
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, src4, src5, static (a, b, c, d, e) => a + b + c + d + e)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null,
                async _ =>
                {
                    IgnoredResult.Of(completionBlocked.TrySetResult());
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(SeedValue2);
        await src3.EmitNext(SeedValue3);
        await src4.EmitNext(SeedValue4);
        await src5.EmitNext(SeedValue5);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        await src5.EmitNext(PostDisposeValue);

        await Assert.That(results).Count().IsEqualTo(1);

        _ = allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that CombineLatest5 OnErrorResume guard returns early when disposal is triggered
    /// by an error on source 1 while the downstream OnCompletedAsync is still running.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest5DisposedViaError_ThenOnErrorResumeGuardHits()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        DirectSource<int> src3 = new();
        DirectSource<int> src4 = new();
        DirectSource<int> src5 = new();
        Exception? receivedError = null;
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, src4, src5, static (a, b, c, d, e) => a + b + c + d + e)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    receivedError = ex;
                    return default;
                },
                async _ =>
                {
                    IgnoredResult.Of(completionBlocked.TrySetResult());
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(SeedValue2);
        await src3.EmitNext(SeedValue3);
        await src4.EmitNext(SeedValue4);
        await src5.EmitNext(SeedValue5);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        await src5.EmitError(new InvalidOperationException(IgnoredMessage));

        await Assert.That(receivedError).IsNull();

        _ = allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that CombineLatest6 OnNextCombined guard returns early when disposal is triggered
    /// by an error on source 1 while the downstream OnCompletedAsync is still running.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest6DisposedViaError_ThenOnNextCombinedGuardHits()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        DirectSource<int> src3 = new();
        DirectSource<int> src4 = new();
        DirectSource<int> src5 = new();
        DirectSource<int> src6 = new();
        List<int> results = [];
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, src4, src5, src6, static (a, b, c, d, e, f) => a + b + c + d + e + f)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null,
                async _ =>
                {
                    IgnoredResult.Of(completionBlocked.TrySetResult());
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(SeedValue2);
        await src3.EmitNext(SeedValue3);
        await src4.EmitNext(SeedValue4);
        await src5.EmitNext(SeedValue5);
        await src6.EmitNext(SeedValue6);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        await src6.EmitNext(PostDisposeValue);

        await Assert.That(results).Count().IsEqualTo(1);

        _ = allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that CombineLatest6 OnErrorResume guard returns early when disposal is triggered
    /// by an error on source 1 while the downstream OnCompletedAsync is still running.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest6DisposedViaError_ThenOnErrorResumeGuardHits()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        DirectSource<int> src3 = new();
        DirectSource<int> src4 = new();
        DirectSource<int> src5 = new();
        DirectSource<int> src6 = new();
        Exception? receivedError = null;
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, src4, src5, src6, static (a, b, c, d, e, f) => a + b + c + d + e + f)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    receivedError = ex;
                    return default;
                },
                async _ =>
                {
                    IgnoredResult.Of(completionBlocked.TrySetResult());
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(SeedValue2);
        await src3.EmitNext(SeedValue3);
        await src4.EmitNext(SeedValue4);
        await src5.EmitNext(SeedValue5);
        await src6.EmitNext(SeedValue6);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        await src6.EmitError(new InvalidOperationException(IgnoredMessage));

        await Assert.That(receivedError).IsNull();

        _ = allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that CombineLatest7 OnNextCombined guard returns early when disposal is triggered
    /// by an error on source 1 while the downstream OnCompletedAsync is still running.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest7DisposedViaError_ThenOnNextCombinedGuardHits()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        DirectSource<int> src3 = new();
        DirectSource<int> src4 = new();
        DirectSource<int> src5 = new();
        DirectSource<int> src6 = new();
        DirectSource<int> src7 = new();
        List<int> results = [];
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, src4, src5, src6, src7, static (a, b, c, d, e, f, g) => a + b + c + d + e + f + g)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null,
                async _ =>
                {
                    IgnoredResult.Of(completionBlocked.TrySetResult());
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(SeedValue2);
        await src3.EmitNext(SeedValue3);
        await src4.EmitNext(SeedValue4);
        await src5.EmitNext(SeedValue5);
        await src6.EmitNext(SeedValue6);
        await src7.EmitNext(SeedValue7);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        await src7.EmitNext(PostDisposeValue);

        await Assert.That(results).Count().IsEqualTo(1);

        _ = allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that CombineLatest7 OnErrorResume guard returns early when disposal is triggered
    /// by an error on source 1 while the downstream OnCompletedAsync is still running.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest7DisposedViaError_ThenOnErrorResumeGuardHits()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        DirectSource<int> src3 = new();
        DirectSource<int> src4 = new();
        DirectSource<int> src5 = new();
        DirectSource<int> src6 = new();
        DirectSource<int> src7 = new();
        Exception? receivedError = null;
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, src4, src5, src6, src7, static (a, b, c, d, e, f, g) => a + b + c + d + e + f + g)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    receivedError = ex;
                    return default;
                },
                async _ =>
                {
                    IgnoredResult.Of(completionBlocked.TrySetResult());
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(SeedValue2);
        await src3.EmitNext(SeedValue3);
        await src4.EmitNext(SeedValue4);
        await src5.EmitNext(SeedValue5);
        await src6.EmitNext(SeedValue6);
        await src7.EmitNext(SeedValue7);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        await src7.EmitError(new InvalidOperationException(IgnoredMessage));

        await Assert.That(receivedError).IsNull();

        _ = allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that CombineLatest8 OnNextCombined guard returns early when disposal is triggered
    /// by an error on source 1 while the downstream OnCompletedAsync is still running.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest8DisposedViaError_ThenOnNextCombinedGuardHits()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        DirectSource<int> src3 = new();
        DirectSource<int> src4 = new();
        DirectSource<int> src5 = new();
        DirectSource<int> src6 = new();
        DirectSource<int> src7 = new();
        DirectSource<int> src8 = new();
        List<int> results = [];
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(
                src2,
                src3,
                src4,
                src5,
                src6,
                src7,
                src8,
                static (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null,
                async _ =>
                {
                    IgnoredResult.Of(completionBlocked.TrySetResult());
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(SeedValue2);
        await src3.EmitNext(SeedValue3);
        await src4.EmitNext(SeedValue4);
        await src5.EmitNext(SeedValue5);
        await src6.EmitNext(SeedValue6);
        await src7.EmitNext(SeedValue7);
        await src8.EmitNext(SeedValue8);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        await src8.EmitNext(PostDisposeValue);

        await Assert.That(results).Count().IsEqualTo(1);

        _ = allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that CombineLatest8 OnErrorResume guard returns early when disposal is triggered
    /// by an error on source 1 while the downstream OnCompletedAsync is still running.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest8DisposedViaError_ThenOnErrorResumeGuardHits()
    {
        DirectSource<int> src1 = new();
        DirectSource<int> src2 = new();
        DirectSource<int> src3 = new();
        DirectSource<int> src4 = new();
        DirectSource<int> src5 = new();
        DirectSource<int> src6 = new();
        DirectSource<int> src7 = new();
        DirectSource<int> src8 = new();
        Exception? receivedError = null;
        TaskCompletionSource completionBlocked = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource allowCompletion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(
                src2,
                src3,
                src4,
                src5,
                src6,
                src7,
                src8,
                static (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    receivedError = ex;
                    return default;
                },
                async _ =>
                {
                    IgnoredResult.Of(completionBlocked.TrySetResult());
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(SeedValue2);
        await src3.EmitNext(SeedValue3);
        await src4.EmitNext(SeedValue4);
        await src5.EmitNext(SeedValue5);
        await src6.EmitNext(SeedValue6);
        await src7.EmitNext(SeedValue7);
        await src8.EmitNext(SeedValue8);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        await src8.EmitError(new InvalidOperationException(IgnoredMessage));

        await Assert.That(receivedError).IsNull();

        _ = allowCompletion.TrySetResult();
        await failTask;
    }
}
