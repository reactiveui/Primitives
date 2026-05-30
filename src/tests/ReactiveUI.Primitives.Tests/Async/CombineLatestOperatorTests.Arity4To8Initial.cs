// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for CombineLatestOperatorTests.</summary>
public partial class CombineLatestOperatorTests
{
    /// <summary>String literal "resume" used by multiple tests.</summary>
    private const string ResumeMessage = "resume";

    /// <summary>Error propagation in 4-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestFourSources_Error_ThenCompletes()
    {
        var signals = Enumerable.Range(0, FourSources).Select(_ => Signal.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                (a, b, c, d) => a + b + c + d)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await signals[Source3Index].OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>All four sources complete successfully.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestFourSources_AllComplete_ThenCombinedCompletes()
    {
        const int SourcesToCompleteFirst = 3;
        var signals = Enumerable.Range(0, FourSources).Select(_ => Signal.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                (a, b, c, d) => a + b + c + d)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        for (var i = 0; i < SourcesToCompleteFirst; i++)
        {
            await signals[i].OnCompletedAsync(Result.Success);
        }

        await Assert.That(completionResult).IsNull();

        await signals[Source3Index].OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Error resume forwarded in 4-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestFourSources_ErrorResume_ThenForwarded()
    {
        var signals = Enumerable.Range(0, FourSources).Select(_ => Signal.Create<int>()).ToList();

        var errors = new List<Exception>();
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                (a, b, c, d) => a + b + c + d)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        var expected = new InvalidOperationException(ResumeMessage);
        await signals[Source2Index].OnErrorResumeAsync(expected, CancellationToken.None);

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>Error propagation in 5-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestFiveSources_Error_ThenCompletes()
    {
        var signals = Enumerable.Range(0, FiveSources).Select(_ => Signal.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                signals[Source4Index].Values,
                (a, b, c, d, e) => a + b + c + d + e)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await signals[0].OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>All five sources complete successfully.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestFiveSources_AllComplete_ThenCombinedCompletes()
    {
        const int SourcesToCompleteFirst = 4;
        var signals = Enumerable.Range(0, FiveSources).Select(_ => Signal.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                signals[Source4Index].Values,
                (a, b, c, d, e) => a + b + c + d + e)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        for (var i = 0; i < SourcesToCompleteFirst; i++)
        {
            await signals[i].OnCompletedAsync(Result.Success);
        }

        await Assert.That(completionResult).IsNull();

        await signals[Source4Index].OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Error resume forwarded in 5-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestFiveSources_ErrorResume_ThenForwarded()
    {
        var signals = Enumerable.Range(0, FiveSources).Select(_ => Signal.Create<int>()).ToList();

        var errors = new List<Exception>();
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                signals[Source4Index].Values,
                (a, b, c, d, e) => a + b + c + d + e)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        var expected = new InvalidOperationException(ResumeMessage);
        await signals[Source4Index].OnErrorResumeAsync(expected, CancellationToken.None);

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>Error propagation in 6-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestSixSources_Error_ThenCompletes()
    {
        var signals = Enumerable.Range(0, SixSources).Select(_ => Signal.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                signals[Source4Index].Values,
                signals[Source5Index].Values,
                (a, b, c, d, e, f) => a + b + c + d + e + f)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await signals[Source5Index].OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>All six sources complete successfully.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestSixSources_AllComplete_ThenCombinedCompletes()
    {
        const int SourcesToCompleteFirst = 5;
        var signals = Enumerable.Range(0, SixSources).Select(_ => Signal.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                signals[Source4Index].Values,
                signals[Source5Index].Values,
                (a, b, c, d, e, f) => a + b + c + d + e + f)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        for (var i = 0; i < SourcesToCompleteFirst; i++)
        {
            await signals[i].OnCompletedAsync(Result.Success);
        }

        await Assert.That(completionResult).IsNull();

        await signals[Source5Index].OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Error resume forwarded in 6-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestSixSources_ErrorResume_ThenForwarded()
    {
        var signals = Enumerable.Range(0, SixSources).Select(_ => Signal.Create<int>()).ToList();

        var errors = new List<Exception>();
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                signals[Source4Index].Values,
                signals[Source5Index].Values,
                (a, b, c, d, e, f) => a + b + c + d + e + f)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        var expected = new InvalidOperationException(ResumeMessage);
        await signals[Source3Index].OnErrorResumeAsync(expected, CancellationToken.None);

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>Error propagation in 7-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestSevenSources_Error_ThenCompletes()
    {
        var signals = Enumerable.Range(0, SevenSources).Select(_ => Signal.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                signals[Source4Index].Values,
                signals[Source5Index].Values,
                signals[Source6Index].Values,
                (a, b, c, d, e, f, g) => a + b + c + d + e + f + g)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await signals[Source6Index].OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>All seven sources complete successfully.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestSevenSources_AllComplete_ThenCombinedCompletes()
    {
        const int SourcesToCompleteFirst = 6;
        var signals = Enumerable.Range(0, SevenSources).Select(_ => Signal.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                signals[Source4Index].Values,
                signals[Source5Index].Values,
                signals[Source6Index].Values,
                (a, b, c, d, e, f, g) => a + b + c + d + e + f + g)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        for (var i = 0; i < SourcesToCompleteFirst; i++)
        {
            await signals[i].OnCompletedAsync(Result.Success);
        }

        await Assert.That(completionResult).IsNull();

        await signals[Source6Index].OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Error resume forwarded in 7-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestSevenSources_ErrorResume_ThenForwarded()
    {
        var signals = Enumerable.Range(0, SevenSources).Select(_ => Signal.Create<int>()).ToList();

        var errors = new List<Exception>();
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                signals[Source4Index].Values,
                signals[Source5Index].Values,
                signals[Source6Index].Values,
                (a, b, c, d, e, f, g) => a + b + c + d + e + f + g)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        var expected = new InvalidOperationException(ResumeMessage);
        await signals[0].OnErrorResumeAsync(expected, CancellationToken.None);

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>Error propagation in 8-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107:Methods should not have too many parameters", Justification = "Test Purposes")]
    public async Task WhenCombineLatestEightSources_Error_ThenCompletes()
    {
        var signals = Enumerable.Range(0, EightSources).Select(_ => Signal.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                signals[Source4Index].Values,
                signals[Source5Index].Values,
                signals[Source6Index].Values,
                signals[Source7Index].Values,
                (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await signals[Source7Index].OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>All eight sources complete successfully.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107:Methods should not have too many parameters", Justification = "Test Purposes")]
    public async Task WhenCombineLatestEightSources_AllComplete_ThenCombinedCompletes()
    {
        const int SourcesToCompleteFirst = 7;
        var signals = Enumerable.Range(0, EightSources).Select(_ => Signal.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                signals[Source4Index].Values,
                signals[Source5Index].Values,
                signals[Source6Index].Values,
                signals[Source7Index].Values,
                (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        for (var i = 0; i < SourcesToCompleteFirst; i++)
        {
            await signals[i].OnCompletedAsync(Result.Success);
        }

        await Assert.That(completionResult).IsNull();

        await signals[Source7Index].OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Error resume forwarded in 8-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107", Justification = "Arity-8 CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatestEightSources_ErrorResume_ThenForwarded()
    {
        var signals = Enumerable.Range(0, EightSources).Select(_ => Signal.Create<int>()).ToList();

        var errors = new List<Exception>();
        await using var sub = await signals[0].Values
            .CombineLatest(
                signals[1].Values,
                signals[Source2Index].Values,
                signals[Source3Index].Values,
                signals[Source4Index].Values,
                signals[Source5Index].Values,
                signals[Source6Index].Values,
                signals[Source7Index].Values,
                (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        var expected = new InvalidOperationException(ResumeMessage);
        await signals[Source5Index].OnErrorResumeAsync(expected, CancellationToken.None);

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0]).IsSameReferenceAs(expected);
    }
}
