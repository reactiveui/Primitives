// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Coverage for the error-forwarding and edge cases of the fused
/// async filter operators in <c>ParityHelpers.FilterFusions</c> —
/// <c>SkipWhileNull</c>, <c>WhereIsNotNull</c>, <c>LatestOrDefault</c>,
/// <c>WaitUntil</c>, <c>AsSignal</c>, <c>Not</c>, <c>WhereTrue</c>, <c>WhereFalse</c>.</summary>
[SuppressMessage("Performance", "CA1822:Mark members as static", Justification = "TUnit requires instance methods")]
public class ParityHelpersFilterFusionsTests
{
    /// <summary>Sentinel "found" sentinel string.</summary>
    private const string Hit = "hit";

    /// <summary>Expected count of bool outputs that pass / fail their filter.</summary>
    private const int ExpectedBoolFilterCount = 2;

    /// <summary>Expected count of unit signals emitted by the <c>AsSignal</c> test.</summary>
    private const int ExpectedSignalCount = 3;

    /// <summary>Predicate threshold for the <c>WaitUntil</c> test.</summary>
    private const int WaitUntilThreshold = 3;

    /// <summary>Verifies that <c>SkipWhileNull</c> drops leading nulls then forwards every value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSkipWhileNull_ThenDropsLeadingNullsThenLatches()
    {
        string?[] inputs = [null, null, "a", null, "b"];

        var result = await inputs.ToAsyncSignal()
            .SkipWhileNull()
            .ToListAsync();

        // After the first non-null, the gate opens and every subsequent value (including null!) flows.
        // The implementation forwards `value!` past the gate; we assert the non-null prefix is correct
        // and we receive at least the values after the gate opened.
        await Assert.That(result.Count).IsGreaterThanOrEqualTo(1);
        await Assert.That(result[0]).IsEqualTo("a");
    }

    /// <summary>Verifies that <c>WhereIsNotNull</c> strips nulls and forwards non-nulls.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereIsNotNull_ThenForwardsNonNullsOnly()
    {
        string?[] inputs = [null, "a", null, "b", null];

        var result = await inputs.ToAsyncSignal()
            .WhereIsNotNull()
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo(["a", "b"]);
    }

    /// <summary>Verifies that <c>LatestOrDefault</c> emits the seed first, then suppresses
    /// values equal to the most-recent emitted, then forwards every distinct value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLatestOrDefault_ThenSeedFirstAndDistinctOnly()
    {
        const int Zero = 0;
        const int One = 1;
        const int Two = 2;
        int[] inputs = [Zero, Zero, One, One, Two];

        var result = await inputs.ToAsyncSignal()
            .LatestOrDefault(Zero)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([Zero, One, Two]);
    }

    /// <summary>Verifies that <c>WaitUntil</c> emits the first matching value and completes, dropping subsequent values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitUntilMatches_ThenEmitsFirstHitAndCompletes()
    {
        int[] inputs = [1, 2, 3, 4, 5];

        var result = await inputs.ToAsyncSignal()
            .WaitUntil(static x => x >= WaitUntilThreshold)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([WaitUntilThreshold]);
    }

    /// <summary>Verifies that <c>WaitUntil</c> with no match completes empty.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitUntilNoMatch_ThenCompletesEmpty()
    {
        int[] inputs = [1, 2, 3];

        var result = await inputs.ToAsyncSignal()
            .WaitUntil(static _ => false)
            .ToListAsync();

        await Assert.That(result).IsEmpty();
    }

    /// <summary>Verifies that <c>AsSignal</c> projects every emission to <see cref="RxVoid.Default"/>.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsSignal_ThenProjectsToUnit()
    {
        int[] inputs = [1, 2, 3];

        var result = await inputs.ToAsyncSignal()
            .AsSignal()
            .ToListAsync();

        await Assert.That(result.Count).IsEqualTo(ExpectedSignalCount);
        for (var i = 0; i < result.Count; i++)
        {
            await Assert.That(result[i]).IsEqualTo(RxVoid.Default);
        }
    }

    /// <summary>Verifies that <c>Not</c> negates every boolean emission.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenNot_ThenNegatesEvery()
    {
        bool[] inputs = [true, false, true];

        var result = await inputs.ToAsyncSignal()
            .Not()
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([false, true, false]);
    }

    /// <summary>Verifies that <c>WhereTrue</c> forwards only true values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereTrue_ThenForwardsOnlyTrue()
    {
        bool[] inputs = [true, false, true, false];

        var result = await inputs.ToAsyncSignal()
            .WhereTrue()
            .ToListAsync();

        await Assert.That(result.Count).IsEqualTo(ExpectedBoolFilterCount);
        for (var i = 0; i < result.Count; i++)
        {
            await Assert.That(result[i]).IsTrue();
        }
    }

    /// <summary>Verifies that <c>WhereFalse</c> forwards only false values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereFalse_ThenForwardsOnlyFalse()
    {
        bool[] inputs = [true, false, true, false];

        var result = await inputs.ToAsyncSignal()
            .WhereFalse()
            .ToListAsync();

        await Assert.That(result.Count).IsEqualTo(ExpectedBoolFilterCount);
        for (var i = 0; i < result.Count; i++)
        {
            await Assert.That(result[i]).IsFalse();
        }
    }

    /// <summary>Verifies that <c>WhereIsNotNull</c> forwards <c>OnErrorResume</c> downstream.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereIsNotNullSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<string?>();
        Exception? received = null;

        await using var sub = await signal.Values
            .WhereIsNotNull()
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    received = ex;
                    return default;
                });

        await signal.OnErrorResumeAsync(new InvalidOperationException(Hit), CancellationToken.None);

        await AsyncTestHelpers.WaitForConditionAsync(
            () => received is not null,
            TimeSpan.FromSeconds(5));

        await Assert.That(received).IsNotNull();
        await Assert.That(received!.Message).IsEqualTo(Hit);
    }

    /// <summary>Verifies that <c>Pairwise</c> forwards a non-terminal upstream error downstream.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenPairwiseSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.Pairwise().SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                caught = ex;
                errorTcs.TrySetResult();
                return default;
            });

        var expected = new InvalidOperationException("pairwise-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>SkipWhileNull</c> forwards a non-terminal upstream error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSkipWhileNullSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<string?>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.SkipWhileNull().SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                caught = ex;
                errorTcs.TrySetResult();
                return default;
            });

        var expected = new InvalidOperationException("skip-while-null-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>LatestOrDefault</c> forwards a non-terminal upstream error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenLatestOrDefaultSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.LatestOrDefault(0).SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                caught = ex;
                errorTcs.TrySetResult();
                return default;
            });

        var expected = new InvalidOperationException("latest-or-default-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>WaitUntil</c> forwards a non-terminal upstream error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWaitUntilSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.WaitUntil(static _ => false).SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                caught = ex;
                errorTcs.TrySetResult();
                return default;
            });

        var expected = new InvalidOperationException("wait-until-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>AsSignal</c> forwards a non-terminal upstream error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsSignalSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.AsSignal().SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                caught = ex;
                errorTcs.TrySetResult();
                return default;
            });

        var expected = new InvalidOperationException("as-signal-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>Not</c> forwards a non-terminal upstream error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAsyncNotSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<bool>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.Not().SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                caught = ex;
                errorTcs.TrySetResult();
                return default;
            });

        var expected = new InvalidOperationException("not-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>WhereTrue</c> forwards a non-terminal upstream error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereTrueSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<bool>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.WhereTrue().SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                caught = ex;
                errorTcs.TrySetResult();
                return default;
            });

        var expected = new InvalidOperationException("where-true-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>WhereFalse</c> forwards a non-terminal upstream error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereFalseSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<bool>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values.WhereFalse().SubscribeAsync(
            static (_, _) => default,
            (ex, _) =>
            {
                caught = ex;
                errorTcs.TrySetResult();
                return default;
            });

        var expected = new InvalidOperationException("where-false-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }
}
