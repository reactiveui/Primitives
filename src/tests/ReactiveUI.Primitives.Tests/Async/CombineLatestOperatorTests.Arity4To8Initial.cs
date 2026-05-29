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
using ReactiveUI.Primitives.Async.Subjects;

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
        var subjects = Enumerable.Range(0, FourSources).Select(_ => SubjectAsync.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await subjects[0].Values
            .CombineLatest(
                subjects[1].Values,
                subjects[Source2Index].Values,
                subjects[Source3Index].Values,
                (a, b, c, d) => a + b + c + d)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await subjects[Source3Index].OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>All four sources complete successfully.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestFourSources_AllComplete_ThenCombinedCompletes()
    {
        const int SourcesToCompleteFirst = 3;
        var subjects = Enumerable.Range(0, FourSources).Select(_ => SubjectAsync.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await subjects[0].Values
            .CombineLatest(
                subjects[1].Values,
                subjects[Source2Index].Values,
                subjects[Source3Index].Values,
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
            await subjects[i].OnCompletedAsync(Result.Success);
        }

        await Assert.That(completionResult).IsNull();

        await subjects[Source3Index].OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Error resume forwarded in 4-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestFourSources_ErrorResume_ThenForwarded()
    {
        var subjects = Enumerable.Range(0, FourSources).Select(_ => SubjectAsync.Create<int>()).ToList();

        var errors = new List<Exception>();
        await using var sub = await subjects[0].Values
            .CombineLatest(
                subjects[1].Values,
                subjects[Source2Index].Values,
                subjects[Source3Index].Values,
                (a, b, c, d) => a + b + c + d)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        var expected = new InvalidOperationException(ResumeMessage);
        await subjects[Source2Index].OnErrorResumeAsync(expected, CancellationToken.None);

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>Error propagation in 5-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestFiveSources_Error_ThenCompletes()
    {
        var subjects = Enumerable.Range(0, FiveSources).Select(_ => SubjectAsync.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await subjects[0].Values
            .CombineLatest(
                subjects[1].Values,
                subjects[Source2Index].Values,
                subjects[Source3Index].Values,
                subjects[Source4Index].Values,
                (a, b, c, d, e) => a + b + c + d + e)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await subjects[0].OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>All five sources complete successfully.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestFiveSources_AllComplete_ThenCombinedCompletes()
    {
        const int SourcesToCompleteFirst = 4;
        var subjects = Enumerable.Range(0, FiveSources).Select(_ => SubjectAsync.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await subjects[0].Values
            .CombineLatest(
                subjects[1].Values,
                subjects[Source2Index].Values,
                subjects[Source3Index].Values,
                subjects[Source4Index].Values,
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
            await subjects[i].OnCompletedAsync(Result.Success);
        }

        await Assert.That(completionResult).IsNull();

        await subjects[Source4Index].OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Error resume forwarded in 5-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestFiveSources_ErrorResume_ThenForwarded()
    {
        var subjects = Enumerable.Range(0, FiveSources).Select(_ => SubjectAsync.Create<int>()).ToList();

        var errors = new List<Exception>();
        await using var sub = await subjects[0].Values
            .CombineLatest(
                subjects[1].Values,
                subjects[Source2Index].Values,
                subjects[Source3Index].Values,
                subjects[Source4Index].Values,
                (a, b, c, d, e) => a + b + c + d + e)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        var expected = new InvalidOperationException(ResumeMessage);
        await subjects[Source4Index].OnErrorResumeAsync(expected, CancellationToken.None);

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>Error propagation in 6-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestSixSources_Error_ThenCompletes()
    {
        var subjects = Enumerable.Range(0, SixSources).Select(_ => SubjectAsync.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await subjects[0].Values
            .CombineLatest(
                subjects[1].Values,
                subjects[Source2Index].Values,
                subjects[Source3Index].Values,
                subjects[Source4Index].Values,
                subjects[Source5Index].Values,
                (a, b, c, d, e, f) => a + b + c + d + e + f)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await subjects[Source5Index].OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>All six sources complete successfully.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestSixSources_AllComplete_ThenCombinedCompletes()
    {
        const int SourcesToCompleteFirst = 5;
        var subjects = Enumerable.Range(0, SixSources).Select(_ => SubjectAsync.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await subjects[0].Values
            .CombineLatest(
                subjects[1].Values,
                subjects[Source2Index].Values,
                subjects[Source3Index].Values,
                subjects[Source4Index].Values,
                subjects[Source5Index].Values,
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
            await subjects[i].OnCompletedAsync(Result.Success);
        }

        await Assert.That(completionResult).IsNull();

        await subjects[Source5Index].OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Error resume forwarded in 6-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestSixSources_ErrorResume_ThenForwarded()
    {
        var subjects = Enumerable.Range(0, SixSources).Select(_ => SubjectAsync.Create<int>()).ToList();

        var errors = new List<Exception>();
        await using var sub = await subjects[0].Values
            .CombineLatest(
                subjects[1].Values,
                subjects[Source2Index].Values,
                subjects[Source3Index].Values,
                subjects[Source4Index].Values,
                subjects[Source5Index].Values,
                (a, b, c, d, e, f) => a + b + c + d + e + f)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        var expected = new InvalidOperationException(ResumeMessage);
        await subjects[Source3Index].OnErrorResumeAsync(expected, CancellationToken.None);

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>Error propagation in 7-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestSevenSources_Error_ThenCompletes()
    {
        var subjects = Enumerable.Range(0, SevenSources).Select(_ => SubjectAsync.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await subjects[0].Values
            .CombineLatest(
                subjects[1].Values,
                subjects[Source2Index].Values,
                subjects[Source3Index].Values,
                subjects[Source4Index].Values,
                subjects[Source5Index].Values,
                subjects[Source6Index].Values,
                (a, b, c, d, e, f, g) => a + b + c + d + e + f + g)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await subjects[Source6Index].OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>All seven sources complete successfully.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestSevenSources_AllComplete_ThenCombinedCompletes()
    {
        const int SourcesToCompleteFirst = 6;
        var subjects = Enumerable.Range(0, SevenSources).Select(_ => SubjectAsync.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await subjects[0].Values
            .CombineLatest(
                subjects[1].Values,
                subjects[Source2Index].Values,
                subjects[Source3Index].Values,
                subjects[Source4Index].Values,
                subjects[Source5Index].Values,
                subjects[Source6Index].Values,
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
            await subjects[i].OnCompletedAsync(Result.Success);
        }

        await Assert.That(completionResult).IsNull();

        await subjects[Source6Index].OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Error resume forwarded in 7-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestSevenSources_ErrorResume_ThenForwarded()
    {
        var subjects = Enumerable.Range(0, SevenSources).Select(_ => SubjectAsync.Create<int>()).ToList();

        var errors = new List<Exception>();
        await using var sub = await subjects[0].Values
            .CombineLatest(
                subjects[1].Values,
                subjects[Source2Index].Values,
                subjects[Source3Index].Values,
                subjects[Source4Index].Values,
                subjects[Source5Index].Values,
                subjects[Source6Index].Values,
                (a, b, c, d, e, f, g) => a + b + c + d + e + f + g)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        var expected = new InvalidOperationException(ResumeMessage);
        await subjects[0].OnErrorResumeAsync(expected, CancellationToken.None);

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0]).IsSameReferenceAs(expected);
    }

    /// <summary>Error propagation in 8-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107:Methods should not have too many parameters", Justification = "Test Purposes")]
    public async Task WhenCombineLatestEightSources_Error_ThenCompletes()
    {
        var subjects = Enumerable.Range(0, EightSources).Select(_ => SubjectAsync.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await subjects[0].Values
            .CombineLatest(
                subjects[1].Values,
                subjects[Source2Index].Values,
                subjects[Source3Index].Values,
                subjects[Source4Index].Values,
                subjects[Source5Index].Values,
                subjects[Source6Index].Values,
                subjects[Source7Index].Values,
                (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h)
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await subjects[Source7Index].OnCompletedAsync(Result.Failure(new InvalidOperationException("err")));

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
        var subjects = Enumerable.Range(0, EightSources).Select(_ => SubjectAsync.Create<int>()).ToList();

        Result? completionResult = null;
        await using var sub = await subjects[0].Values
            .CombineLatest(
                subjects[1].Values,
                subjects[Source2Index].Values,
                subjects[Source3Index].Values,
                subjects[Source4Index].Values,
                subjects[Source5Index].Values,
                subjects[Source6Index].Values,
                subjects[Source7Index].Values,
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
            await subjects[i].OnCompletedAsync(Result.Success);
        }

        await Assert.That(completionResult).IsNull();

        await subjects[Source7Index].OnCompletedAsync(Result.Success);
        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Error resume forwarded in 8-source variant.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage("Major Code Smell", "S107", Justification = "Arity-8 CombineLatest selector lambda parameter count mirrors the operator signature under test.")]
    public async Task WhenCombineLatestEightSources_ErrorResume_ThenForwarded()
    {
        var subjects = Enumerable.Range(0, EightSources).Select(_ => SubjectAsync.Create<int>()).ToList();

        var errors = new List<Exception>();
        await using var sub = await subjects[0].Values
            .CombineLatest(
                subjects[1].Values,
                subjects[Source2Index].Values,
                subjects[Source3Index].Values,
                subjects[Source4Index].Values,
                subjects[Source5Index].Values,
                subjects[Source6Index].Values,
                subjects[Source7Index].Values,
                (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h)
            .SubscribeAsync(
                (_, _) => default,
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        var expected = new InvalidOperationException(ResumeMessage);
        await subjects[Source5Index].OnErrorResumeAsync(expected, CancellationToken.None);

        await Assert.That(errors).Count().IsEqualTo(1);
        await Assert.That(errors[0]).IsSameReferenceAs(expected);
    }
}
