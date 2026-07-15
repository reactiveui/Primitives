// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for CombineLatestOperatorTests.</summary>
public partial class CombineLatestOperatorTests
{
    /// <summary>String literal "after dispose" used by multiple tests.</summary>
    private const string AfterDisposeMessage = "after dispose";

    /// <summary>Error resume after disposal is ignored.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestTwoSources_ErrorResumeAfterDispose_ThenIgnored()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();

        List<Exception> errors = [];
        var sub = await s1.Values
            .CombineLatest(s2.Values, static (a, b) => a + b)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        await sub.DisposeAsync();

        await s1.OnErrorResumeAsync(new InvalidOperationException(AfterDisposeMessage), CancellationToken.None);

        await Assert.That(errors).IsEmpty();
    }

    /// <summary>Error from source before any values still propagates.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_ErrorBeforeAnyValues_ThenCompletes()
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

        await s1.OnCompletedAsync(Result.Failure(new InvalidOperationException("early error")));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Emission continues after error resume (non-terminal).</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestTwoSources_EmissionContinuesAfterErrorResume()
    {
        const int MinResultCount = 2;
        const int ExpectedFinalSum = 12;
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();

        List<int> results = [];
        List<Exception> errors = [];
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, static (a, b) => a + b)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Step1, CancellationToken.None);

        await s1.OnErrorResumeAsync(new InvalidOperationException("non-terminal"), CancellationToken.None);

        await s1.OnNextAsync(Source1Value, CancellationToken.None);

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(results).Count().IsGreaterThanOrEqualTo(MinResultCount);
        await Assert.That(results[^1]).IsEqualTo(ExpectedFinalSum);
    }

    /// <summary>No emission until all N sources have values for 4-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestFourSources_NoEmissionUntilAllHaveValues()
    {
        const int SourcesToEmitFirst = 3;
        const int FinalSourceValue = 4;
        const int ExpectedSum = 10;
        var signals = Enumerable.Range(0, FourSources).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                static (a, b, c, d) => a + b + c + d)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var i = 0; i < SourcesToEmitFirst; i++)
        {
            await signals[i].OnNextAsync(i + 1, CancellationToken.None);
        }

        await Assert.That(results).IsEmpty();

        await signals[Source3Index].OnNextAsync(FinalSourceValue, CancellationToken.None);
        await Assert.That(results).Count().IsEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ExpectedSum);
    }

    /// <summary>Disposal of 3-source variant stops emissions.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_Disposed_ThenNoMoreEmissions()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();

        List<int> results = [];
        var sub = await s1.Values
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
        var countBefore = results.Count;

        await sub.DisposeAsync();

        await s1.OnNextAsync(Source1Value, CancellationToken.None);

        await Assert.That(results).Count().IsEqualTo(countBefore);
    }

    /// <summary>OnNextCombined returns early when subscription is disposed mid-stream (2-source).</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestTwoSources_DisposedDuringEmission_ThenOnNextCombinedReturnsEarly()
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

        // Establish initial combined values so future OnNext triggers OnNextCombined.
        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Step1, CancellationToken.None);
        var countBefore = results.Count;

        // Dispose, then emit — the OnNextCombined path should hit _disposed == 1 and return.
        await sub.DisposeAsync();

        await s1.OnNextAsync(SentinelValue, CancellationToken.None);
        await s2.OnNextAsync(SentinelValue, CancellationToken.None);

        await Assert.That(results).Count().IsEqualTo(countBefore);
    }

    /// <summary>OnNextCombined returns early when subscription is disposed mid-stream (3-source).</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_DisposedDuringEmission_ThenOnNextCombinedReturnsEarly()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();

        List<int> results = [];
        var sub = await s1.Values
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
        var countBefore = results.Count;

        await sub.DisposeAsync();

        await s1.OnNextAsync(SentinelValue, CancellationToken.None);
        await s2.OnNextAsync(SentinelValue, CancellationToken.None);
        await s3.OnNextAsync(SentinelValue, CancellationToken.None);

        await Assert.That(results).Count().IsEqualTo(countBefore);
    }

    /// <summary>OnErrorResume returns early when subscription is disposed (3-source).</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_ErrorResumeAfterDispose_ThenIgnored()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();

        List<Exception> errors = [];
        var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, static (a, b, c) => a + b + c)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        await sub.DisposeAsync();

        await s1.OnErrorResumeAsync(new InvalidOperationException(AfterDisposeMessage), CancellationToken.None);
        await s2.OnErrorResumeAsync(new InvalidOperationException(AfterDisposeMessage), CancellationToken.None);
        await s3.OnErrorResumeAsync(new InvalidOperationException(AfterDisposeMessage), CancellationToken.None);

        await Assert.That(errors).IsEmpty();
    }

    /// <summary>Error from source 1 in 3-source variant triggers completion with failure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_ErrorFromSrc1_ThenCompletes()
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

        await s1.OnCompletedAsync(Result.Failure(new InvalidOperationException("src1 error")));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
        await Assert.That(completionResult.Value.Exception is InvalidOperationException).IsTrue();
    }

    /// <summary>Error from source 3 in 3-source variant triggers completion with failure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_ErrorFromSrc3_ThenCompletes()
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

        await s3.OnCompletedAsync(Result.Failure(new InvalidOperationException("src3 error")));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
        await Assert.That(completionResult.Value.Exception is InvalidOperationException).IsTrue();
    }

    /// <summary>Two-source: source 2 completes first, then source 1 completes the combined sequence.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestTwoSources_Src2CompletesThenSrc1Completes_ThenCombinedCompletes()
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

        await s2.OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNull();

        await s1.OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Three-source: sources complete in reverse order (3, 2, 1) with combined completing on last.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_SourcesCompleteInReverseOrder_ThenCombinedCompletesOnLast()
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

        await s3.OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNull();

        await s2.OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNull();

        await s1.OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Three-source: source 2 completes last, triggering combined completion.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_Src2CompletesLast_ThenCombinedCompletes()
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
        await s3.OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNull();

        await s2.OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Three-source: source 3 completes last, triggering combined completion.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_Src3CompletesLast_ThenCombinedCompletes()
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

    /// <summary>Error resume from source 1 is forwarded in 3-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_ErrorResumeFromSrc1_ThenForwarded()
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

        InvalidOperationException expected = new("src1 resume");
        await s1.OnErrorResumeAsync(expected, CancellationToken.None);

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>Error resume from source 2 is forwarded in 3-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_ErrorResumeFromSrc2_ThenForwarded()
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

        InvalidOperationException expected = new("src2 resume");
        await s2.OnErrorResumeAsync(expected, CancellationToken.None);

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>Double disposal is safe and does not throw for 3-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_DoubleDispose_ThenSafe()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();

        var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, static (a, b, c) => a + b + c)
            .SubscribeAsync(
                static (_, _) => default,
                null);

        await sub.DisposeAsync();
        await sub.DisposeAsync();
    }

    /// <summary>Error from source 1 in 2-source variant while other source already completed triggers failure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestTwoSources_Src2CompletedThenSrc1Errors_ThenFailure()
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

        await s2.OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNull();

        await s1.OnCompletedAsync(Result.Failure(new InvalidOperationException("late error")));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Error from source 2 in 3-source variant while others already completed triggers failure.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_TwoCompletedThenThirdErrors_ThenFailure()
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
        await s3.OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNull();

        await s2.OnCompletedAsync(Result.Failure(new InvalidOperationException("late error")));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Two-source: emissions continue after one source completes successfully.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestTwoSources_OneSrcCompleted_ThenOtherSrcStillEmits()
    {
        const int ExpectedFinalSum = 21;
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();

        List<int> results = [];
        Result? completionResult = null;
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, static (a, b) => a + b)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Step1, CancellationToken.None);
        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);

        // Complete source 1, but source 2 can still emit combined values.
        await s1.OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNull();

        await s2.OnNextAsync(Step2, CancellationToken.None);

        // The latest s1 value (1) is still held, so 1 + 20 = 21.
        await Assert.That(results[^1]).IsEqualTo(ExpectedFinalSum);
    }

    /// <summary>Three-source: emissions continue after two sources complete successfully.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_TwoSrcsCompleted_ThenRemainingSrcStillEmits()
    {
        const int ExpectedFinalSum = 211;
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();

        List<int> results = [];
        Result? completionResult = null;
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, static (a, b, c) => a + b + c)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(Step1, CancellationToken.None);
        await s3.OnNextAsync(LargeStep1, CancellationToken.None);
        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);

        await s1.OnCompletedAsync(Result.Success);
        await s2.OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNull();

        await s3.OnNextAsync(LargeStep2, CancellationToken.None);

        // Latest held values: s1=1, s2=10, so 1 + 10 + 200 = 211.
        await Assert.That(results[^1]).IsEqualTo(ExpectedFinalSum);
    }

    /// <summary>Multiple emissions with updated latest values for 5-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestFiveSources_MultipleEmissions_ThenUsesLatestValues()
    {
        const int InitialSum = 5;
        const int FinalSum = 14;
        var signals = Enumerable.Range(0, FiveSources).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                signals[Source4Index].Values,
                static (a, b, c, d, e) => a + b + c + d + e)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var i = 0; i < FiveSources; i++)
        {
            await signals[i].OnNextAsync(1, CancellationToken.None);
        }

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(InitialSum);

        await signals[0].OnNextAsync(Step1, CancellationToken.None);
        await Assert.That(results[^1]).IsEqualTo(FinalSum);
    }
}
