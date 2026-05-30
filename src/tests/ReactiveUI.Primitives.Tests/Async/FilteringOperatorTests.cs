// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Tests for filtering operators: Where, Take, Skip, TakeWhile, SkipWhile, Distinct, DistinctUntilChanged.
/// </summary>
public class FilteringOperatorTests
{
    /// <summary>Second element (2).</summary>
    private const int SecondElement = 2;

    /// <summary>Third element (3).</summary>
    private const int ThirdElement = 3;

    /// <summary>Fourth element (4).</summary>
    private const int FourthElement = 4;

    /// <summary>Fifth element (5).</summary>
    private const int FifthElement = 5;

    /// <summary>Sixth element (6).</summary>
    private const int SixthElement = 6;

    /// <summary>Hoisted source array used by tests (was inline literal).</summary>
    private static readonly int[] Sequence112231 = [1, 1, 2, 2, 3, 1];

    /// <summary>Hoisted source array used by tests (was inline literal).</summary>
    private static readonly int[] Sequence122313 = [1, 2, 2, 3, 1, 3];

    /// <summary>Hoisted source array used by tests (was inline literal).</summary>
    private static readonly string[] SequenceAABB = ["a", "A", "b", "B"];

    /// <summary>Hoisted source array used by tests (was inline literal).</summary>
    private static readonly string[] SequenceAABBB = ["a", "A", "b", "B", "b"];

    /// <summary>Hoisted source array used by tests (was inline literal).</summary>
    private static readonly string[] SequenceAaAbBaBb = ["aa", "ab", "ba", "bb"];

    /// <summary>Hoisted source array used by tests (was inline literal).</summary>
    private static readonly string[] SequenceAbcAbADefDe = ["abc", "ab", "a", "def", "de"];

    /// <summary>Tests sync Where filters elements.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereSync_ThenFiltersElements()
    {
        var result = await SignalAsync.Range(1, 6)
            .Where(x => x % 2 == 0)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([SecondElement, FourthElement, SixthElement]);
    }

    /// <summary>Tests async Where filters elements.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereAsync_ThenFiltersElements()
    {
        var result = await SignalAsync.Range(1, 5)
            .Where(async (x, _) =>
            {
                await Task.Yield();
                return x > 3;
            })
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([FourthElement, FifthElement]);
    }

    /// <summary>Tests Where filtering all emits nothing.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereFilterAll_ThenEmitsNothing()
    {
        var result = await SignalAsync.Range(1, 3)
            .Where(_ => false)
            .ToListAsync();

        await Assert.That(result).IsEmpty();
    }

    /// <summary>Tests Take emits only first N.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTake_ThenEmitsOnlyFirstN()
    {
        var result = await SignalAsync.Range(1, 10)
            .Take(3)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([1, SecondElement, ThirdElement]);
    }

    /// <summary>Tests Take zero emits nothing.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeZero_ThenEmitsNothing()
    {
        var result = await SignalAsync.Range(1, 10)
            .Take(0)
            .ToListAsync();

        await Assert.That(result).IsEmpty();
    }

    /// <summary>Tests Take more than available emits all.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeMoreThanAvailable_ThenEmitsAll()
    {
        var result = await SignalAsync.Range(1, 3)
            .Take(100)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([1, SecondElement, ThirdElement]);
    }

    /// <summary>Tests Take negative throws.</summary>
    [Test]
    public void WhenTakeNegative_ThenThrowsArgumentOutOfRange() =>
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            SignalAsync.Return(1).Take(-1));

    /// <summary>Tests Skip skips first N.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSkip_ThenSkipsFirstN()
    {
        var result = await SignalAsync.Range(1, 5)
            .Skip(2)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([ThirdElement, FourthElement, FifthElement]);
    }

    /// <summary>Tests Skip zero emits all.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSkipZero_ThenEmitsAll()
    {
        var result = await SignalAsync.Range(1, 3)
            .Skip(0)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([1, SecondElement, ThirdElement]);
    }

    /// <summary>Tests Skip more than available emits nothing.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSkipMoreThanAvailable_ThenEmitsNothing()
    {
        var result = await SignalAsync.Range(1, 3)
            .Skip(100)
            .ToListAsync();

        await Assert.That(result).IsEmpty();
    }

    /// <summary>Tests Skip negative throws.</summary>
    [Test]
    public void WhenSkipNegative_ThenThrowsArgumentOutOfRange() =>
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            SignalAsync.Return(1).Skip(-1));

    /// <summary>Tests sync TakeWhile emits while true.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeWhileSync_ThenEmitsWhileTrue()
    {
        var result = await SignalAsync.Range(1, 10)
            .TakeWhile(x => x < 4)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([1, SecondElement, ThirdElement]);
    }

    /// <summary>Tests async TakeWhile emits while true.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeWhileAsync_ThenEmitsWhileTrue()
    {
        var result = await SignalAsync.Range(1, 10)
            .TakeWhile(async (x, _) =>
            {
                await Task.Yield();
                return x <= 2;
            })
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([1, SecondElement]);
    }

    /// <summary>Tests TakeWhile all true emits all.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeWhileAllTrue_ThenEmitsAll()
    {
        var result = await SignalAsync.Range(1, 3)
            .TakeWhile(_ => true)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([1, SecondElement, ThirdElement]);
    }

    /// <summary>Tests TakeWhile all false emits nothing.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeWhileAllFalse_ThenEmitsNothing()
    {
        var result = await SignalAsync.Range(1, 3)
            .TakeWhile(_ => false)
            .ToListAsync();

        await Assert.That(result).IsEmpty();
    }

    /// <summary>Tests TakeWhile null predicate throws.</summary>
    [Test]
    public void WhenTakeWhileNullPredicate_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(() =>
            SignalAsync.Return(1).TakeWhile((Func<int, bool>)null!));

    /// <summary>Tests sync SkipWhile skips while true.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSkipWhileSync_ThenSkipsWhileTrue()
    {
        var result = await SignalAsync.Range(1, 6)
            .SkipWhile(x => x < 4)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([FourthElement, FifthElement, SixthElement]);
    }

    /// <summary>Tests async SkipWhile skips while true.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSkipWhileAsync_ThenSkipsWhileTrue()
    {
        var result = await SignalAsync.Range(1, 5)
            .SkipWhile(async (x, _) =>
            {
                await Task.Yield();
                return x < 3;
            })
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([ThirdElement, FourthElement, FifthElement]);
    }

    /// <summary>Tests SkipWhile always true emits nothing.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSkipWhileAlwaysTrue_ThenEmitsNothing()
    {
        var result = await SignalAsync.Range(1, 3)
            .SkipWhile(_ => true)
            .ToListAsync();

        await Assert.That(result).IsEmpty();
    }

    /// <summary>Tests SkipWhile always false emits all.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSkipWhileAlwaysFalse_ThenEmitsAll()
    {
        var result = await SignalAsync.Range(1, 3)
            .SkipWhile(_ => false)
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([1, SecondElement, ThirdElement]);
    }

    /// <summary>Tests SkipWhile null predicate throws.</summary>
    [Test]
    public void WhenSkipWhileNullPredicate_ThenThrowsArgumentNull() =>
        Assert.Throws<ArgumentNullException>(() =>
            SignalAsync.Return(1).SkipWhile((Func<int, bool>)null!));

    /// <summary>Tests Distinct removes duplicates.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDistinct_ThenRemovesDuplicates()
    {
        var source = Sequence122313.ToAsyncSignal();

        var result = await source.Distinct().ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([1, SecondElement, ThirdElement]);
    }

    /// <summary>Tests Distinct with comparer uses case insensitive.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDistinctWithComparer_ThenUsesCaseInsensitive()
    {
        var source = SequenceAABB.ToAsyncSignal();

        var result = await source.Distinct(StringComparer.OrdinalIgnoreCase).ToListAsync();

        await Assert.That(result).IsCollectionEqualTo(["a", "b"]);
    }

    /// <summary>Tests DistinctBy distinguishes by key.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDistinctBy_ThenDistinguishesByKey()
    {
        var source = SequenceAbcAbADefDe.ToAsyncSignal();

        var result = await source.DistinctBy(s => s.Length).ToListAsync();

        await Assert.That(result).IsCollectionEqualTo(["abc", "ab", "a"]);
    }

    /// <summary>Tests DistinctUntilChanged suppresses consecutive duplicates.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDistinctUntilChanged_ThenSuppressesConsecutiveDuplicates()
    {
        var source = Sequence112231.ToAsyncSignal();

        var result = await source.DistinctUntilChanged().ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([1, SecondElement, ThirdElement, 1]);
    }

    /// <summary>Tests DistinctUntilChanged with comparer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDistinctUntilChangedWithComparer_ThenUsesComparer()
    {
        var source = SequenceAABBB.ToAsyncSignal();

        var result = await source.DistinctUntilChanged(StringComparer.OrdinalIgnoreCase).ToListAsync();

        await Assert.That(result).IsCollectionEqualTo(["a", "b"]);
    }

    /// <summary>Tests DistinctUntilChangedBy distinguishes by key.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDistinctUntilChangedBy_ThenDistinguishesByKey()
    {
        var source = SequenceAaAbBaBb.ToAsyncSignal();

        var result = await source.DistinctUntilChangedBy(s => s[0]).ToListAsync();

        await Assert.That(result).IsCollectionEqualTo(["aa", "ba"]);
    }

    /// <summary>Verifies that sync-predicate <c>SkipWhile</c> forwards a non-terminal upstream error
    /// through its <c>OnErrorResume</c> path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSkipWhileSyncSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .SkipWhile(static x => x < 2)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("skip-while-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that sync-predicate <c>TakeWhile</c> forwards a non-terminal upstream error
    /// through its <c>OnErrorResume</c> path.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeWhileSyncSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .TakeWhile(static x => x < 10)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("take-while-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies the async-predicate <c>SkipWhile</c> sync-completed predicate path —
    /// returning <see langword="true"/> drops the value, returning <see langword="false"/> latches
    /// the gate and forwards.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSkipWhileAsyncWithSyncPredicate_ThenLatchesOnFalse()
    {
        var result = await SignalAsync.Range(1, 5)
            .SkipWhile(static (x, _) => new ValueTask<bool>(x < 3))
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([ThirdElement, FourthElement, FifthElement]);
    }

    /// <summary>Verifies the async-predicate <c>TakeWhile</c> sync-completed predicate path —
    /// returning <see langword="true"/> forwards, returning <see langword="false"/> terminates.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeWhileAsyncWithSyncPredicate_ThenTerminatesOnFalse()
    {
        var result = await SignalAsync.Range(1, 5)
            .TakeWhile(static (x, _) => new ValueTask<bool>(x < 3))
            .ToListAsync();

        await Assert.That(result).IsCollectionEqualTo([1, SecondElement]);
    }

    /// <summary>Verifies that async-predicate <c>SkipWhile</c> forwards a non-terminal upstream error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSkipWhileAsyncSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .SkipWhile(static (_, _) => new ValueTask<bool>(true))
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("skip-while-async-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that async-predicate <c>TakeWhile</c> forwards a non-terminal upstream error.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeWhileAsyncSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .TakeWhile(static (_, _) => new ValueTask<bool>(true))
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("take-while-async-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Exercises the <c>DistinctObserver.OnErrorResumeAsyncCore</c> forwarding —
    /// upstream resumable errors propagate verbatim to the downstream observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDistinctSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .Distinct()
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("distinct-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Exercises the <c>DistinctByObserver.OnErrorResumeAsyncCore</c> forwarding.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDistinctBySourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .DistinctBy(static x => x)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("distinct-by-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Exercises the <c>DistinctUntilChangedObserver.OnErrorResumeAsyncCore</c> forwarding.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDistinctUntilChangedSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .DistinctUntilChanged()
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("distinct-until-changed-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Exercises the <c>DistinctUntilChangedByObserver.OnErrorResumeAsyncCore</c> forwarding.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenDistinctUntilChangedBySourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .DistinctUntilChangedBy(static x => x)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("distinct-until-changed-by-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Exercises the <c>WhereSyncObserver.OnErrorResumeAsyncCore</c> forwarding —
    /// the synchronous-predicate overload forwards upstream resumable errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereSyncSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .Where(static _ => true)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("where-sync-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Exercises the <c>SkipObserver.OnErrorResumeAsyncCore</c> forwarding —
    /// upstream resumable errors propagate verbatim through the Skip observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSkipSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .Skip(1)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("skip-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Exercises the <c>TakeObserver.OnErrorResumeAsyncCore</c> forwarding.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTakeSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .Take(10)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("take-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Exercises the <c>CastObserver.OnErrorResumeAsyncCore</c> forwarding.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCastSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<object>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .Cast<object, int>()
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("cast-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Exercises the <c>OfTypeObserver.OnErrorResumeAsyncCore</c> forwarding.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenOfTypeSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<object>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .OfType<object, string>()
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("of-type-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Exercises the <c>SelectSyncObserver.OnErrorResumeAsyncCore</c> forwarding —
    /// upstream resumable errors propagate verbatim through the synchronous Select observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectSyncSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .Select(static x => x + 1)
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("select-sync-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Exercises the <c>SelectAsyncObserver.OnErrorResumeAsyncCore</c> forwarding —
    /// the async-selector overload forwards upstream resumable errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .Select(static (x, _) => new ValueTask<int>(x + 1))
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("select-async-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Exercises the <c>WhereAsyncObserver.OnErrorResumeAsyncCore</c> forwarding —
    /// the async-predicate overload forwards upstream resumable errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereAsyncSourceErrorResume_ThenForwarded()
    {
        var signal = Signal.Create<int>();
        Exception? caught = null;
        var errorTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var sub = await signal.Values
            .Where(static (_, _) => new ValueTask<bool>(true))
            .SubscribeAsync(
                static (_, _) => default,
                (ex, _) =>
                {
                    caught = ex;
                    errorTcs.TrySetResult();
                    return default;
                });

        var expected = new InvalidOperationException("where-async-error");
        await signal.OnErrorResumeAsync(expected, CancellationToken.None);

        await errorTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(caught).IsSameReferenceAs(expected);
    }
}
