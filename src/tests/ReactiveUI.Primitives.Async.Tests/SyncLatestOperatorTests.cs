// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>
/// Tests for the <c>SyncLatest</c> spelling of the combine-latest operator: the enumerable overloads
/// (snapshot and projecting) and the arity-8 through arity-16 overloads. Each source contributes a value
/// of one, so the projected result equals the arity once every source has produced a value.
/// </summary>
public partial class SyncLatestOperatorTests
{
    /// <summary>Seconds a test waits for a combined emission before giving up.</summary>
    private const int WaitTimeoutSeconds = 5;

    /// <summary>Index of source 3 in the per-test source list.</summary>
    private const int SourceIndex2 = 2;

    /// <summary>Index of source 4 in the per-test source list.</summary>
    private const int SourceIndex3 = 3;

    /// <summary>Index of source 5 in the per-test source list.</summary>
    private const int SourceIndex4 = 4;

    /// <summary>Index of source 6 in the per-test source list.</summary>
    private const int SourceIndex5 = 5;

    /// <summary>Index of source 7 in the per-test source list.</summary>
    private const int SourceIndex6 = 6;

    /// <summary>Index of source 8 in the per-test source list.</summary>
    private const int SourceIndex7 = 7;

    /// <summary>Index of source 9 in the per-test source list.</summary>
    private const int SourceIndex8 = 8;

    /// <summary>Index of source 10 in the per-test source list.</summary>
    private const int SourceIndex9 = 9;

    /// <summary>Index of source 11 in the per-test source list.</summary>
    private const int SourceIndex10 = 10;

    /// <summary>Index of source 12 in the per-test source list.</summary>
    private const int SourceIndex11 = 11;

    /// <summary>Index of source 13 in the per-test source list.</summary>
    private const int SourceIndex12 = 12;

    /// <summary>Index of source 14 in the per-test source list.</summary>
    private const int SourceIndex13 = 13;

    /// <summary>Index of source 15 in the per-test source list.</summary>
    private const int SourceIndex14 = 14;

    /// <summary>Index of source 16 in the per-test source list.</summary>
    private const int SourceIndex15 = 15;

    /// <summary>Number of sources combined by the arity-8 overload.</summary>
    private const int ArityEight = 8;

    /// <summary>Number of sources combined by the arity-9 overload.</summary>
    private const int ArityNine = 9;

    /// <summary>Number of sources combined by the arity-10 overload.</summary>
    private const int ArityTen = 10;

    /// <summary>Number of sources combined by the arity-11 overload.</summary>
    private const int ArityEleven = 11;

    /// <summary>Number of sources combined by the arity-12 overload.</summary>
    private const int ArityTwelve = 12;

    /// <summary>Number of sources combined by the arity-13 overload.</summary>
    private const int ArityThirteen = 13;

    /// <summary>Number of sources combined by the arity-14 overload.</summary>
    private const int ArityFourteen = 14;

    /// <summary>Number of sources combined by the arity-15 overload.</summary>
    private const int ArityFifteen = 15;

    /// <summary>Number of sources combined by the arity-16 overload.</summary>
    private const int AritySixteen = 16;

    /// <summary>Value emitted by the second source in the enumerable tests.</summary>
    private const int SecondValue = 2;

    /// <summary>Maximum time a test waits for a combined emission to arrive.</summary>
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(WaitTimeoutSeconds);

    /// <summary>Verifies the enumerable <c>SyncLatest</c> emits a snapshot of the latest value of every source
    /// once all of them have produced one.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncLatestOverEnumerable_ThenEmitsSnapshotOfLatestValues()
    {
        var first = Signal.Create<int>();
        var second = Signal.Create<int>();
        IObservableAsync<int>[] sources = [first.Values, second.Values];

        List<int[]> snapshots = [];
        await using var sub = await sources.SyncLatest().SubscribeAsync(
            (snapshot, _) =>
            {
                // The operator reuses one buffer per subscription, so the snapshot must be copied here.
                snapshots.Add([.. snapshot]);
                return default;
            },
            null);

        await first.OnNextAsync(1, CancellationToken.None);
        await second.OnNextAsync(SecondValue, CancellationToken.None);

        await AsyncTestHelpers.WaitForConditionAsync(() => snapshots.Count >= 1, WaitTimeout);

        await Assert.That(snapshots).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(snapshots[0]).IsCollectionEqualTo([1, SecondValue]);
    }

    /// <summary>Verifies the projecting enumerable <c>SyncLatest</c> runs the snapshot through the result selector.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSyncLatestOverEnumerableWithSelector_ThenProjectsSnapshot()
    {
        var first = Signal.Create<int>();
        var second = Signal.Create<int>();
        IObservableAsync<int>[] sources = [first.Values, second.Values];

        List<int> results = [];
        await using var sub = await sources.SyncLatest(static snapshot => snapshot[0] + snapshot[1]).SubscribeAsync(
            (x, _) =>
            {
                results.Add(x);
                return default;
            },
            null);

        await first.OnNextAsync(1, CancellationToken.None);
        await second.OnNextAsync(SecondValue, CancellationToken.None);

        await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 1, WaitTimeout);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(1 + SecondValue);
    }

    /// <summary>Verifies the arity-8 <c>SyncLatest</c> projects the latest value of all eight sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S107:Methods should not have too many parameters",
        Justification = "An arity-N combinator's selector takes N values; the lambda mirrors the operator signature.")]
    public async Task WhenSyncLatestEightSources_ThenCombinesAll()
    {
        var signals = Enumerable.Range(0, ArityEight).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .SyncLatest(
                signals[1].Values,
                signals[SourceIndex2].Values,
                signals[SourceIndex3].Values,
                signals[SourceIndex4].Values,
                signals[SourceIndex5].Values,
                signals[SourceIndex6].Values,
                signals[SourceIndex7].Values,
                static (a, b, c, d, e, f, g, h) => a + b + c + d + e + f + g + h)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var index = 0; index < ArityEight; index++)
        {
            await signals[index].OnNextAsync(1, CancellationToken.None);
        }

        await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 1, WaitTimeout);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ArityEight);
    }

    /// <summary>Verifies the arity-9 <c>SyncLatest</c> projects the latest value of all nine sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S107:Methods should not have too many parameters",
        Justification = "An arity-N combinator's selector takes N values; the lambda mirrors the operator signature.")]
    public async Task WhenSyncLatestNineSources_ThenCombinesAll()
    {
        var signals = Enumerable.Range(0, ArityNine).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .SyncLatest(
                signals[1].Values,
                signals[SourceIndex2].Values,
                signals[SourceIndex3].Values,
                signals[SourceIndex4].Values,
                signals[SourceIndex5].Values,
                signals[SourceIndex6].Values,
                signals[SourceIndex7].Values,
                signals[SourceIndex8].Values,
                static (a, b, c, d, e, f, g, h, i) => a + b + c + d + e + f + g + h + i)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var index = 0; index < ArityNine; index++)
        {
            await signals[index].OnNextAsync(1, CancellationToken.None);
        }

        await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 1, WaitTimeout);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ArityNine);
    }

    /// <summary>Verifies the arity-10 <c>SyncLatest</c> projects the latest value of all ten sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S107:Methods should not have too many parameters",
        Justification = "An arity-N combinator's selector takes N values; the lambda mirrors the operator signature.")]
    public async Task WhenSyncLatestTenSources_ThenCombinesAll()
    {
        var signals = Enumerable.Range(0, ArityTen).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .SyncLatest(
                signals[1].Values,
                signals[SourceIndex2].Values,
                signals[SourceIndex3].Values,
                signals[SourceIndex4].Values,
                signals[SourceIndex5].Values,
                signals[SourceIndex6].Values,
                signals[SourceIndex7].Values,
                signals[SourceIndex8].Values,
                signals[SourceIndex9].Values,
                static (a, b, c, d, e, f, g, h, i, j) => a + b + c + d + e + f + g + h + i + j)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var index = 0; index < ArityTen; index++)
        {
            await signals[index].OnNextAsync(1, CancellationToken.None);
        }

        await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 1, WaitTimeout);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ArityTen);
    }

    /// <summary>Verifies the arity-11 <c>SyncLatest</c> projects the latest value of all eleven sources.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    [SuppressMessage(
        "Major Code Smell",
        "S107:Methods should not have too many parameters",
        Justification = "An arity-N combinator's selector takes N values; the lambda mirrors the operator signature.")]
    public async Task WhenSyncLatestElevenSources_ThenCombinesAll()
    {
        var signals = Enumerable.Range(0, ArityEleven).Select(static _ => Signal.Create<int>()).ToList();

        List<int> results = [];
        await using var sub = await signals[0].Values
            .SyncLatest(
                signals[1].Values,
                signals[SourceIndex2].Values,
                signals[SourceIndex3].Values,
                signals[SourceIndex4].Values,
                signals[SourceIndex5].Values,
                signals[SourceIndex6].Values,
                signals[SourceIndex7].Values,
                signals[SourceIndex8].Values,
                signals[SourceIndex9].Values,
                signals[SourceIndex10].Values,
                static (a, b, c, d, e, f, g, h, i, j, k) => a + b + c + d + e + f + g + h + i + j + k)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        for (var index = 0; index < ArityEleven; index++)
        {
            await signals[index].OnNextAsync(1, CancellationToken.None);
        }

        await AsyncTestHelpers.WaitForConditionAsync(() => results.Count >= 1, WaitTimeout);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(ArityEleven);
    }
}
