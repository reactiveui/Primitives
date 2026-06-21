// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for CombineLatestOperatorTests.</summary>
public partial class CombineLatestOperatorTests
{
    /// <summary>Verifies that CombineLatest with 8 sources propagates failure when source 1 errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S107",
        Justification =
            "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatest8Sources_Source1Errors_ThenFailurePropagates()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();

        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(
            s2.Values,
            s3.Values,
            s4.Values,
            s5.Values,
            s6.Values,
            s7.Values,
            s8.Values,
            (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
            (_, _) => default,
            null,
            r =>
            {
                _ = completed.TrySetResult(r);
                return default;
            });

        await s1.OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));
        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(result.IsFailure).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 8 sources propagates failure when source 2 errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S107",
        Justification =
            "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatest8Sources_Source2Errors_ThenFailurePropagates()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();

        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(
            s2.Values,
            s3.Values,
            s4.Values,
            s5.Values,
            s6.Values,
            s7.Values,
            s8.Values,
            (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
            (_, _) => default,
            null,
            r =>
            {
                _ = completed.TrySetResult(r);
                return default;
            });

        await s2.OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));
        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(result.IsFailure).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 8 sources propagates failure when source 3 errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S107",
        Justification =
            "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatest8Sources_Source3Errors_ThenFailurePropagates()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();

        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(
            s2.Values,
            s3.Values,
            s4.Values,
            s5.Values,
            s6.Values,
            s7.Values,
            s8.Values,
            (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
            (_, _) => default,
            null,
            r =>
            {
                _ = completed.TrySetResult(r);
                return default;
            });

        await s3.OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));
        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(result.IsFailure).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 8 sources propagates failure when source 4 errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S107",
        Justification =
            "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatest8Sources_Source4Errors_ThenFailurePropagates()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();

        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(
            s2.Values,
            s3.Values,
            s4.Values,
            s5.Values,
            s6.Values,
            s7.Values,
            s8.Values,
            (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
            (_, _) => default,
            null,
            r =>
            {
                _ = completed.TrySetResult(r);
                return default;
            });

        await s4.OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));
        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(result.IsFailure).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 8 sources propagates failure when source 5 errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S107",
        Justification =
            "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatest8Sources_Source5Errors_ThenFailurePropagates()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();

        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(
            s2.Values,
            s3.Values,
            s4.Values,
            s5.Values,
            s6.Values,
            s7.Values,
            s8.Values,
            (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
            (_, _) => default,
            null,
            r =>
            {
                _ = completed.TrySetResult(r);
                return default;
            });

        await s5.OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));
        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(result.IsFailure).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 8 sources propagates failure when source 6 errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S107",
        Justification =
            "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatest8Sources_Source6Errors_ThenFailurePropagates()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();

        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(
            s2.Values,
            s3.Values,
            s4.Values,
            s5.Values,
            s6.Values,
            s7.Values,
            s8.Values,
            (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
            (_, _) => default,
            null,
            r =>
            {
                _ = completed.TrySetResult(r);
                return default;
            });

        await s6.OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));
        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(result.IsFailure).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 8 sources propagates failure when source 7 errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S107",
        Justification =
            "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatest8Sources_Source7Errors_ThenFailurePropagates()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();

        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(
            s2.Values,
            s3.Values,
            s4.Values,
            s5.Values,
            s6.Values,
            s7.Values,
            s8.Values,
            (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
            (_, _) => default,
            null,
            r =>
            {
                _ = completed.TrySetResult(r);
                return default;
            });

        await s7.OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));
        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(result.IsFailure).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 8 sources propagates failure when source 8 errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S107",
        Justification =
            "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatest8Sources_Source8Errors_ThenFailurePropagates()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();

        TaskCompletionSource<Result> completed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var sub = await s1.Values.CombineLatest(
            s2.Values,
            s3.Values,
            s4.Values,
            s5.Values,
            s6.Values,
            s7.Values,
            s8.Values,
            (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
            (_, _) => default,
            null,
            r =>
            {
                _ = completed.TrySetResult(r);
                return default;
            });

        await s8.OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));
        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));

        await Assert.That(result.IsFailure).IsTrue();
    }
}
