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
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        var results = new List<int>();
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, (a, b) => a + b)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null,
                async _ =>
                {
                    completionBlocked.TrySetResult();
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(SeedValue2);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        await src2.EmitNext(PostDisposeValue);

        await Assert.That(results).Count().IsEqualTo(1);

        allowCompletion.TrySetResult();
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
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        Exception? receivedError = null;
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, (a, b) => a + b)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    receivedError = ex;
                    return default;
                },
                async _ =>
                {
                    completionBlocked.TrySetResult();
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(SeedValue2);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        await src2.EmitError(new InvalidOperationException(IgnoredMessage));

        await Assert.That(receivedError).IsNull();

        allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that CombineLatest3 OnNextCombined guard returns early when disposal is triggered
    /// by an error on source 1 while the downstream OnCompletedAsync is still running.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107:Methods should not have too many parameters", Justification = "Test Reasons")]
    public async Task WhenCombineLatest3DisposedViaError_ThenOnNextCombinedGuardHits()
    {
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        var src3 = new DirectSource<int>();
        var results = new List<int>();
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, (a, b, c) => a + b + c)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null,
                async _ =>
                {
                    completionBlocked.TrySetResult();
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(SeedValue2);
        await src3.EmitNext(SeedValue3);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        await src3.EmitNext(PostDisposeValue);

        await Assert.That(results).Count().IsEqualTo(1);

        allowCompletion.TrySetResult();
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
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        var src3 = new DirectSource<int>();
        Exception? receivedError = null;
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, (a, b, c) => a + b + c)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    receivedError = ex;
                    return default;
                },
                async _ =>
                {
                    completionBlocked.TrySetResult();
                    await allowCompletion.Task;
                });

        await src1.EmitNext(1);
        await src2.EmitNext(SeedValue2);
        await src3.EmitNext(SeedValue3);

        var failTask = Task.Run(() => src1.Complete(Result.Failure(new InvalidOperationException("test"))));
        await completionBlocked.Task;

        await src3.EmitError(new InvalidOperationException(IgnoredMessage));

        await Assert.That(receivedError).IsNull();

        allowCompletion.TrySetResult();
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
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        var src3 = new DirectSource<int>();
        var src4 = new DirectSource<int>();
        var results = new List<int>();
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, src4, (a, b, c, d) => a + b + c + d)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null,
                async _ =>
                {
                    completionBlocked.TrySetResult();
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

        allowCompletion.TrySetResult();
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
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        var src3 = new DirectSource<int>();
        var src4 = new DirectSource<int>();
        Exception? receivedError = null;
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, src4, (a, b, c, d) => a + b + c + d)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    receivedError = ex;
                    return default;
                },
                async _ =>
                {
                    completionBlocked.TrySetResult();
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

        allowCompletion.TrySetResult();
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
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        var src3 = new DirectSource<int>();
        var src4 = new DirectSource<int>();
        var src5 = new DirectSource<int>();
        var results = new List<int>();
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, src4, src5, (a, b, c, d, e) => a + b + c + d + e)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null,
                async _ =>
                {
                    completionBlocked.TrySetResult();
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

        allowCompletion.TrySetResult();
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
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        var src3 = new DirectSource<int>();
        var src4 = new DirectSource<int>();
        var src5 = new DirectSource<int>();
        Exception? receivedError = null;
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, src4, src5, (a, b, c, d, e) => a + b + c + d + e)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    receivedError = ex;
                    return default;
                },
                async _ =>
                {
                    completionBlocked.TrySetResult();
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

        allowCompletion.TrySetResult();
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
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        var src3 = new DirectSource<int>();
        var src4 = new DirectSource<int>();
        var src5 = new DirectSource<int>();
        var src6 = new DirectSource<int>();
        var results = new List<int>();
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, src4, src5, src6, (a, b, c, d, e, f) => a + b + c + d + e + f)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null,
                async _ =>
                {
                    completionBlocked.TrySetResult();
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

        allowCompletion.TrySetResult();
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
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        var src3 = new DirectSource<int>();
        var src4 = new DirectSource<int>();
        var src5 = new DirectSource<int>();
        var src6 = new DirectSource<int>();
        Exception? receivedError = null;
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, src4, src5, src6, (a, b, c, d, e, f) => a + b + c + d + e + f)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    receivedError = ex;
                    return default;
                },
                async _ =>
                {
                    completionBlocked.TrySetResult();
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

        allowCompletion.TrySetResult();
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
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        var src3 = new DirectSource<int>();
        var src4 = new DirectSource<int>();
        var src5 = new DirectSource<int>();
        var src6 = new DirectSource<int>();
        var src7 = new DirectSource<int>();
        var results = new List<int>();
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, src4, src5, src6, src7, (a, b, c, d, e, f, g) => a + b + c + d + e + f + g)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null,
                async _ =>
                {
                    completionBlocked.TrySetResult();
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

        allowCompletion.TrySetResult();
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
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        var src3 = new DirectSource<int>();
        var src4 = new DirectSource<int>();
        var src5 = new DirectSource<int>();
        var src6 = new DirectSource<int>();
        var src7 = new DirectSource<int>();
        Exception? receivedError = null;
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(src2, src3, src4, src5, src6, src7, (a, b, c, d, e, f, g) => a + b + c + d + e + f + g)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    receivedError = ex;
                    return default;
                },
                async _ =>
                {
                    completionBlocked.TrySetResult();
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

        allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that CombineLatest8 OnNextCombined guard returns early when disposal is triggered
    /// by an error on source 1 while the downstream OnCompletedAsync is still running.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107:Methods should not have too many parameters", Justification = "Test Reasons")]
    public async Task WhenCombineLatest8DisposedViaError_ThenOnNextCombinedGuardHits()
    {
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        var src3 = new DirectSource<int>();
        var src4 = new DirectSource<int>();
        var src5 = new DirectSource<int>();
        var src6 = new DirectSource<int>();
        var src7 = new DirectSource<int>();
        var src8 = new DirectSource<int>();
        var results = new List<int>();
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(
                src2,
                src3,
                src4,
                src5,
                src6,
                src7,
                src8,
                (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null,
                async _ =>
                {
                    completionBlocked.TrySetResult();
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

        allowCompletion.TrySetResult();
        await failTask;
    }

    /// <summary>
    /// Verifies that CombineLatest8 OnErrorResume guard returns early when disposal is triggered
    /// by an error on source 1 while the downstream OnCompletedAsync is still running.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107:Methods should not have too many parameters", Justification = "Test Reasons")]
    public async Task WhenCombineLatest8DisposedViaError_ThenOnErrorResumeGuardHits()
    {
        var src1 = new DirectSource<int>();
        var src2 = new DirectSource<int>();
        var src3 = new DirectSource<int>();
        var src4 = new DirectSource<int>();
        var src5 = new DirectSource<int>();
        var src6 = new DirectSource<int>();
        var src7 = new DirectSource<int>();
        var src8 = new DirectSource<int>();
        Exception? receivedError = null;
        var completionBlocked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var allowCompletion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await src1
            .CombineLatest(
                src2,
                src3,
                src4,
                src5,
                src6,
                src7,
                src8,
                (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    receivedError = ex;
                    return default;
                },
                async _ =>
                {
                    completionBlocked.TrySetResult();
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

        allowCompletion.TrySetResult();
        await failTask;
    }
}
