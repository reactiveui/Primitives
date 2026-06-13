// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for CombineLatestOperatorTests.</summary>
public partial class CombineLatestOperatorTests
{
    /// <summary>Verifies that CombineLatest with 2 sources propagates failure when source 1 errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest2Sources_Source1Errors_ThenFailurePropagates()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();

        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(s2.Values, (a, b) => a + b)
            .SubscribeAsync(
                (_, _) => default,
                null,
                r =>
                {
                    completed.TrySetResult(r);
                    return default;
                });

        await s1.OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));
        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(result.IsFailure).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 2 sources propagates failure when source 2 errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest2Sources_Source2Errors_ThenFailurePropagates()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();

        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(s2.Values, (a, b) => a + b)
            .SubscribeAsync(
                (_, _) => default,
                null,
                r =>
                {
                    completed.TrySetResult(r);
                    return default;
                });

        await s2.OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));
        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(result.IsFailure).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 2 sources completes when source 1 is the last to complete.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest2Sources_Source1CompletesLast_ThenCombinedCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();

        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(s2.Values, (a, b) => a + b)
            .SubscribeAsync(
                (_, _) => default,
                null,
                r =>
                {
                    completed.TrySetResult(r);
                    return default;
                });

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Source1Value, CancellationToken.None);
        await s2.OnCompletedAsync(Result.Success);
        await s1.OnCompletedAsync(Result.Success);

        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));
        await Assert.That(result.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 2 sources completes when source 2 is the last to complete.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest2Sources_Source2CompletesLast_ThenCombinedCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();

        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(s2.Values, (a, b) => a + b)
            .SubscribeAsync(
                (_, _) => default,
                null,
                r =>
                {
                    completed.TrySetResult(r);
                    return default;
                });

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Source1Value, CancellationToken.None);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);

        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));
        await Assert.That(result.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 2 sources handles disposal during active emission gracefully.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest2Sources_DisposedDuringEmission_ThenNoError()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();

        List<int> results = [];
        var sub = await s1.Values.CombineLatest(s2.Values, (a, b) => a + b)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null,
                _ => default);

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Source1Value, CancellationToken.None);
        await sub.DisposeAsync();

        // Emit after disposal - should be ignored
        await s1.OnNextAsync(Step1, CancellationToken.None);
        await s2.OnNextAsync(Step2, CancellationToken.None);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
    }

    /// <summary>Verifies that CombineLatest with 2 sources ignores error resume after disposal.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest2Sources_ErrorResumeAfterDisposal_ThenIgnored()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();

        var sub = await s1.Values.CombineLatest(s2.Values, (a, b) => a + b)
            .SubscribeAsync(
                (_, _) => default,
                null,
                _ => default);

        await sub.DisposeAsync();

        // Error resume after disposal should not throw
        await s1.OnErrorResumeAsync(new InvalidOperationException(), CancellationToken.None);
        await s2.OnErrorResumeAsync(new InvalidOperationException(), CancellationToken.None);
    }

    /// <summary>Verifies that CombineLatest with 3 sources propagates failure when source 1 errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest3Sources_Source1Errors_ThenFailurePropagates()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();

        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, (a, b, c) => a + b + c)
            .SubscribeAsync(
                (_, _) => default,
                null,
                r =>
                {
                    completed.TrySetResult(r);
                    return default;
                });

        await s1.OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));
        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(result.IsFailure).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 3 sources propagates failure when source 2 errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest3Sources_Source2Errors_ThenFailurePropagates()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();

        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, (a, b, c) => a + b + c)
            .SubscribeAsync(
                (_, _) => default,
                null,
                r =>
                {
                    completed.TrySetResult(r);
                    return default;
                });

        await s2.OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));
        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(result.IsFailure).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 3 sources propagates failure when source 3 errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest3Sources_Source3Errors_ThenFailurePropagates()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();

        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, (a, b, c) => a + b + c)
            .SubscribeAsync(
                (_, _) => default,
                null,
                r =>
                {
                    completed.TrySetResult(r);
                    return default;
                });

        await s3.OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));
        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(result.IsFailure).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 3 sources completes when source 1 is the last to complete.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest3Sources_Source1CompletesLast_ThenCombinedCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();

        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, (a, b, c) => a + b + c)
            .SubscribeAsync(
                (_, _) => default,
                null,
                r =>
                {
                    completed.TrySetResult(r);
                    return default;
                });

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Source1Value, CancellationToken.None);
        await s3.OnNextAsync(Source2Value, CancellationToken.None);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s1.OnCompletedAsync(Result.Success);

        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));
        await Assert.That(result.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 3 sources completes when source 2 is the last to complete.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest3Sources_Source2CompletesLast_ThenCombinedCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();

        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, (a, b, c) => a + b + c)
            .SubscribeAsync(
                (_, _) => default,
                null,
                r =>
                {
                    completed.TrySetResult(r);
                    return default;
                });

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Source1Value, CancellationToken.None);
        await s3.OnNextAsync(Source2Value, CancellationToken.None);
        await s1.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);

        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));
        await Assert.That(result.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 3 sources completes when source 3 is the last to complete.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest3Sources_Source3CompletesLast_ThenCombinedCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();

        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, (a, b, c) => a + b + c)
            .SubscribeAsync(
                (_, _) => default,
                null,
                r =>
                {
                    completed.TrySetResult(r);
                    return default;
                });

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Source1Value, CancellationToken.None);
        await s3.OnNextAsync(Source2Value, CancellationToken.None);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);

        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));
        await Assert.That(result.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 3 sources handles disposal during active emission gracefully.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest3Sources_DisposedDuringEmission_ThenNoError()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();

        List<int> results = [];
        var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, (a, b, c) => a + b + c)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null,
                _ => default);

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Source1Value, CancellationToken.None);
        await s3.OnNextAsync(Source2Value, CancellationToken.None);
        await sub.DisposeAsync();

        // Emit after disposal - should be ignored
        await s1.OnNextAsync(Step1, CancellationToken.None);
        await s2.OnNextAsync(Step2, CancellationToken.None);
        await s3.OnNextAsync(Step3, CancellationToken.None);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
    }

    /// <summary>Verifies that CombineLatest with 3 sources ignores error resume after disposal.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatest3Sources_ErrorResumeAfterDisposal_ThenIgnored()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();

        var sub = await s1.Values.CombineLatest(s2.Values, s3.Values, (a, b, c) => a + b + c)
            .SubscribeAsync(
                (_, _) => default,
                null,
                _ => default);

        await sub.DisposeAsync();

        // Error resume after disposal should not throw
        await s1.OnErrorResumeAsync(new InvalidOperationException(), CancellationToken.None);
        await s2.OnErrorResumeAsync(new InvalidOperationException(), CancellationToken.None);
        await s3.OnErrorResumeAsync(new InvalidOperationException(), CancellationToken.None);
    }
}
