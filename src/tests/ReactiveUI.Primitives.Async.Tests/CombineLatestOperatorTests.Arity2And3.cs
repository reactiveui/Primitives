// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for CombineLatestOperatorTests.</summary>
public partial class CombineLatestOperatorTests
{
    /// <summary>No emission until both sources have produced a value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestTwoSources_NoEmissionUntilBothHaveValues()
    {
        const int ExpectedSum = 11;
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();

        List<int> results = [];
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, static (a, b) => a + b)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        await s1.OnNextAsync(1, CancellationToken.None);

        await Assert.That(results).IsEmpty();

        await s2.OnNextAsync(Step1, CancellationToken.None);

        await Assert.That(results).Count().IsEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ExpectedSum);
    }

    /// <summary>Multiple emissions use the latest value from each source.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestTwoSources_ThenEmitsOnEachNewValue()
    {
        const int MinExpectedEmissions = 3;
        const int Sum1 = 11;
        const int Sum2 = 12;
        const int Sum3 = 22;
        const int Sum3Index = 2;
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();

        List<int> results = [];
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, static (a, b) => a + b)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Step1, CancellationToken.None);
        await s1.OnNextAsync(Source1Value, CancellationToken.None);
        await s2.OnNextAsync(Step2, CancellationToken.None);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(MinExpectedEmissions);
        await Assert.That(results[0]).IsEqualTo(Sum1);
        await Assert.That(results[1]).IsEqualTo(Sum2);
        await Assert.That(results[Sum3Index]).IsEqualTo(Sum3);
    }

    /// <summary>Error from source 1 completes the combined sequence with failure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestTwoSources_ErrorFromSrc1_ThenCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();

        Result? completionResult = null;
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, static (a, b) => a + b)
            .SubscribeAsync(
                static (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await s1.OnCompletedAsync(Result.Failure(new InvalidOperationException("src1 error")));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
        await Assert.That(completionResult.Value.Exception is InvalidOperationException).IsTrue();
    }

    /// <summary>Error from source 2 completes the combined sequence with failure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestTwoSources_ErrorFromSrc2_ThenCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();

        Result? completionResult = null;
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, static (a, b) => a + b)
            .SubscribeAsync(
                static (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await s2.OnCompletedAsync(Result.Failure(new InvalidOperationException("src2 error")));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Combined sequence completes only when both sources complete successfully.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestTwoSources_BothComplete_ThenCombinedCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();

        Result? completionResult = null;
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, static (a, b) => a + b)
            .SubscribeAsync(
                static (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await s1.OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNull();

        await s2.OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Partial completion does not complete the combined sequence.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestTwoSources_OnlyOneCompletes_ThenNotComplete()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();

        Result? completionResult = null;
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, static (a, b) => a + b)
            .SubscribeAsync(
                static (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await s1.OnCompletedAsync(Result.Success);

        await Assert.That(completionResult).IsNull();
    }

    /// <summary>Disposal stops further emissions.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestTwoSources_Disposed_ThenNoMoreEmissions()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();

        List<int> results = [];
        var sub = await s1.Values
            .CombineLatest(s2.Values, static (a, b) => a + b)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Step1, CancellationToken.None);
        var countBefore = results.Count;

        await sub.DisposeAsync();

        await s1.OnNextAsync(Source1Value, CancellationToken.None);

        await Assert.That(results).Count().IsEqualTo(countBefore);
    }

    /// <summary>Double disposal is safe and does not throw.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestTwoSources_DoubleDispose_ThenSafe()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();

        var sub = await s1.Values
            .CombineLatest(s2.Values, static (a, b) => a + b)
            .SubscribeAsync(
                static (_, _) => default,
                null);

        await sub.DisposeAsync();
        await sub.DisposeAsync();
    }

    /// <summary>Error resume from source 1 is forwarded to the observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestTwoSources_ErrorResume_ThenForwarded()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();

        List<Exception> errors = [];
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, static (a, b) => a + b)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        InvalidOperationException expected = new("resume error");
        await s1.OnErrorResumeAsync(expected, CancellationToken.None);

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>Error resume from source 2 is also forwarded.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestTwoSources_ErrorResumeFromSrc2_ThenForwarded()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();

        List<Exception> errors = [];
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, static (a, b) => a + b)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        InvalidOperationException expected = new("src2 resume");
        await s2.OnErrorResumeAsync(expected, CancellationToken.None);

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>Tests CombineLatest with 3 sources combines correctly.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_ThenCombinesAll()
    {
        const int ExpectedSum = 111;
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();

        List<int> results = [];
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, static (a, b, c) => a + b + c)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Step1, CancellationToken.None);
        await s3.OnNextAsync(LargeStep1, CancellationToken.None);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ExpectedSum);
    }

    /// <summary>No emission until all three sources have values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_NoEmissionUntilAllHaveValues()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();

        List<int> results = [];
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, static (a, b, c) => a + b + c)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Step1, CancellationToken.None);

        await Assert.That(results).IsEmpty();
    }

    /// <summary>Error from any source propagates in 3-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_Error_ThenCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();

        Result? completionResult = null;
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, static (a, b, c) => a + b + c)
            .SubscribeAsync(
                static (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await s2.OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>All three sources complete successfully.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_AllComplete_ThenCombinedCompletes()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();

        Result? completionResult = null;
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, static (a, b, c) => a + b + c)
            .SubscribeAsync(
                static (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNull();

        await s3.OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Error resume forwarded in 3-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_ErrorResume_ThenForwarded()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();

        List<Exception> errors = [];
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, static (a, b, c) => a + b + c)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        InvalidOperationException expected = new("resume");
        await s3.OnErrorResumeAsync(expected, CancellationToken.None);

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0]).IsSameReferenceAs(expected);
    }
}
