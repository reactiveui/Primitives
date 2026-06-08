// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for CombineLatestOperatorTests.</summary>
public partial class CombineLatestOperatorTests
{
    /// <summary>Verifies that CombineLatest with 8 sources completes when source 1 is the last to complete.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107", Justification = "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatest8Sources_Source1CompletesLast_ThenCombinedCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();

        var completed = new TaskCompletionSource<Result>(TaskCreationOptions.RunContinuationsAsynchronously);
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
                completed.TrySetResult(r);
                return default;
            });

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Source1Value, CancellationToken.None);
        await s3.OnNextAsync(Source2Value, CancellationToken.None);
        await s4.OnNextAsync(Source3Value, CancellationToken.None);
        await s5.OnNextAsync(Source4Value, CancellationToken.None);
        await s6.OnNextAsync(Source5Value, CancellationToken.None);
        await s7.OnNextAsync(Source6Value, CancellationToken.None);
        await s8.OnNextAsync(Source7Value, CancellationToken.None);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
        await s8.OnCompletedAsync(Result.Success);
        await s1.OnCompletedAsync(Result.Success);

        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));
        await Assert.That(result.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 8 sources completes when source 2 is the last to complete.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107", Justification = "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatest8Sources_Source2CompletesLast_ThenCombinedCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();

        var completed = new TaskCompletionSource<Result>(TaskCreationOptions.RunContinuationsAsynchronously);
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
                completed.TrySetResult(r);
                return default;
            });

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Source1Value, CancellationToken.None);
        await s3.OnNextAsync(Source2Value, CancellationToken.None);
        await s4.OnNextAsync(Source3Value, CancellationToken.None);
        await s5.OnNextAsync(Source4Value, CancellationToken.None);
        await s6.OnNextAsync(Source5Value, CancellationToken.None);
        await s7.OnNextAsync(Source6Value, CancellationToken.None);
        await s8.OnNextAsync(Source7Value, CancellationToken.None);
        await s1.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
        await s8.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);

        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));
        await Assert.That(result.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 8 sources completes when source 3 is the last to complete.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107", Justification = "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatest8Sources_Source3CompletesLast_ThenCombinedCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();

        var completed = new TaskCompletionSource<Result>(TaskCreationOptions.RunContinuationsAsynchronously);
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
                completed.TrySetResult(r);
                return default;
            });

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Source1Value, CancellationToken.None);
        await s3.OnNextAsync(Source2Value, CancellationToken.None);
        await s4.OnNextAsync(Source3Value, CancellationToken.None);
        await s5.OnNextAsync(Source4Value, CancellationToken.None);
        await s6.OnNextAsync(Source5Value, CancellationToken.None);
        await s7.OnNextAsync(Source6Value, CancellationToken.None);
        await s8.OnNextAsync(Source7Value, CancellationToken.None);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
        await s8.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);

        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));
        await Assert.That(result.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 8 sources completes when source 4 is the last to complete.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107", Justification = "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatest8Sources_Source4CompletesLast_ThenCombinedCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();

        var completed = new TaskCompletionSource<Result>(TaskCreationOptions.RunContinuationsAsynchronously);
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
                completed.TrySetResult(r);
                return default;
            });

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Source1Value, CancellationToken.None);
        await s3.OnNextAsync(Source2Value, CancellationToken.None);
        await s4.OnNextAsync(Source3Value, CancellationToken.None);
        await s5.OnNextAsync(Source4Value, CancellationToken.None);
        await s6.OnNextAsync(Source5Value, CancellationToken.None);
        await s7.OnNextAsync(Source6Value, CancellationToken.None);
        await s8.OnNextAsync(Source7Value, CancellationToken.None);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
        await s8.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);

        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));
        await Assert.That(result.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 8 sources completes when source 5 is the last to complete.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107", Justification = "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatest8Sources_Source5CompletesLast_ThenCombinedCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();

        var completed = new TaskCompletionSource<Result>(TaskCreationOptions.RunContinuationsAsynchronously);
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
                completed.TrySetResult(r);
                return default;
            });

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Source1Value, CancellationToken.None);
        await s3.OnNextAsync(Source2Value, CancellationToken.None);
        await s4.OnNextAsync(Source3Value, CancellationToken.None);
        await s5.OnNextAsync(Source4Value, CancellationToken.None);
        await s6.OnNextAsync(Source5Value, CancellationToken.None);
        await s7.OnNextAsync(Source6Value, CancellationToken.None);
        await s8.OnNextAsync(Source7Value, CancellationToken.None);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
        await s8.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);

        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));
        await Assert.That(result.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 8 sources completes when source 6 is the last to complete.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107", Justification = "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatest8Sources_Source6CompletesLast_ThenCombinedCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();

        var completed = new TaskCompletionSource<Result>(TaskCreationOptions.RunContinuationsAsynchronously);
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
                completed.TrySetResult(r);
                return default;
            });

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Source1Value, CancellationToken.None);
        await s3.OnNextAsync(Source2Value, CancellationToken.None);
        await s4.OnNextAsync(Source3Value, CancellationToken.None);
        await s5.OnNextAsync(Source4Value, CancellationToken.None);
        await s6.OnNextAsync(Source5Value, CancellationToken.None);
        await s7.OnNextAsync(Source6Value, CancellationToken.None);
        await s8.OnNextAsync(Source7Value, CancellationToken.None);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
        await s8.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);

        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));
        await Assert.That(result.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 8 sources completes when source 7 is the last to complete.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107", Justification = "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatest8Sources_Source7CompletesLast_ThenCombinedCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();

        var completed = new TaskCompletionSource<Result>(TaskCreationOptions.RunContinuationsAsynchronously);
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
                completed.TrySetResult(r);
                return default;
            });

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Source1Value, CancellationToken.None);
        await s3.OnNextAsync(Source2Value, CancellationToken.None);
        await s4.OnNextAsync(Source3Value, CancellationToken.None);
        await s5.OnNextAsync(Source4Value, CancellationToken.None);
        await s6.OnNextAsync(Source5Value, CancellationToken.None);
        await s7.OnNextAsync(Source6Value, CancellationToken.None);
        await s8.OnNextAsync(Source7Value, CancellationToken.None);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s8.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);

        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));
        await Assert.That(result.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 8 sources completes when source 8 is the last to complete.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107", Justification = "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatest8Sources_Source8CompletesLast_ThenCombinedCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();

        var completed = new TaskCompletionSource<Result>(TaskCreationOptions.RunContinuationsAsynchronously);
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
                completed.TrySetResult(r);
                return default;
            });

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Source1Value, CancellationToken.None);
        await s3.OnNextAsync(Source2Value, CancellationToken.None);
        await s4.OnNextAsync(Source3Value, CancellationToken.None);
        await s5.OnNextAsync(Source4Value, CancellationToken.None);
        await s6.OnNextAsync(Source5Value, CancellationToken.None);
        await s7.OnNextAsync(Source6Value, CancellationToken.None);
        await s8.OnNextAsync(Source7Value, CancellationToken.None);
        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await s3.OnCompletedAsync(Result.Success);
        await s4.OnCompletedAsync(Result.Success);
        await s5.OnCompletedAsync(Result.Success);
        await s6.OnCompletedAsync(Result.Success);
        await s7.OnCompletedAsync(Result.Success);
        await s8.OnCompletedAsync(Result.Success);

        var result = await completed.Task.WaitAsync(TimeSpan.FromSeconds(WaitTimeoutSeconds));
        await Assert.That(result.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that CombineLatest with 8 sources handles disposal during active emission gracefully.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107", Justification = "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatest8Sources_DisposedDuringEmission_ThenNoError()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();

        var results = new List<int>();
        var sub = await s1.Values.CombineLatest(
            s2.Values,
            s3.Values,
            s4.Values,
            s5.Values,
            s6.Values,
            s7.Values,
            s8.Values,
            (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h).SubscribeAsync(
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
        await s4.OnNextAsync(Source3Value, CancellationToken.None);
        await s5.OnNextAsync(Source4Value, CancellationToken.None);
        await s6.OnNextAsync(Source5Value, CancellationToken.None);
        await s7.OnNextAsync(Source6Value, CancellationToken.None);
        await s8.OnNextAsync(Source7Value, CancellationToken.None);
        await sub.DisposeAsync();

        // Emit after disposal - should be ignored
        await s1.OnNextAsync(Step1, CancellationToken.None);
        await s2.OnNextAsync(Step2, CancellationToken.None);
        await s3.OnNextAsync(Step3, CancellationToken.None);
        await s4.OnNextAsync(Step4, CancellationToken.None);
        await s5.OnNextAsync(Step5, CancellationToken.None);
        await s6.OnNextAsync(Step6, CancellationToken.None);
        await s7.OnNextAsync(Step7, CancellationToken.None);
        await s8.OnNextAsync(Step8, CancellationToken.None);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
    }

    /// <summary>Verifies that CombineLatest with 8 sources ignores error resume after disposal.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107", Justification = "Arity-N CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatest8Sources_ErrorResumeAfterDisposal_ThenIgnored()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();
        var s4 = Signal.Create<int>();
        var s5 = Signal.Create<int>();
        var s6 = Signal.Create<int>();
        var s7 = Signal.Create<int>();
        var s8 = Signal.Create<int>();

        var sub = await s1.Values.CombineLatest(
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
            _ => default);

        await sub.DisposeAsync();

        // Error resume after disposal should not throw
        await s1.OnErrorResumeAsync(new InvalidOperationException(), CancellationToken.None);
        await s2.OnErrorResumeAsync(new InvalidOperationException(), CancellationToken.None);
        await s3.OnErrorResumeAsync(new InvalidOperationException(), CancellationToken.None);
        await s4.OnErrorResumeAsync(new InvalidOperationException(), CancellationToken.None);
        await s5.OnErrorResumeAsync(new InvalidOperationException(), CancellationToken.None);
        await s6.OnErrorResumeAsync(new InvalidOperationException(), CancellationToken.None);
        await s7.OnErrorResumeAsync(new InvalidOperationException(), CancellationToken.None);
        await s8.OnErrorResumeAsync(new InvalidOperationException(), CancellationToken.None);
    }
}
