// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Tests;

/// <summary>
/// Verifies the wide-arity <c>SyncLatest</c> combinators for ten through sixteen sources: a result is
/// withheld until every source has produced a value, each source lands in its own selector parameter, and a
/// later value from any source replaces only that source's contribution.
/// </summary>
public partial class SyncLatestTests
{
    /// <summary>The number of sources combined by the ten-source overload.</summary>
    private const int TenSources = 10;

    /// <summary>The number of sources combined by the eleven-source overload.</summary>
    private const int ElevenSources = 11;

    /// <summary>The number of sources combined by the twelve-source overload.</summary>
    private const int TwelveSources = 12;

    /// <summary>The number of sources combined by the thirteen-source overload.</summary>
    private const int ThirteenSources = 13;

    /// <summary>The number of sources combined by the fourteen-source overload.</summary>
    private const int FourteenSources = 14;

    /// <summary>The number of sources combined by the fifteen-source overload.</summary>
    private const int FifteenSources = 15;

    /// <summary>The number of sources combined by the sixteen-source overload.</summary>
    private const int SixteenSources = 16;

    /// <summary>Verifies the ten-source overload combines the latest value of every source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SyncLatestOverTenSourcesCombinesTheLatestValueOfEverySource()
    {
        var sources = CreateSources(TenSources);
        List<int[]> results = [];
        using var subscription = sources[0]
            .SyncLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                sources[9],
                static (a, b, c, d, e, f, g, h, i, j) => new[] { a, b, c, d, e, f, g, h, i, j })
            .Subscribe(results.Add);
        await AssertCombinesLatestOfEverySource(sources, results).ConfigureAwait(false);
    }

    /// <summary>Verifies the eleven-source overload combines the latest value of every source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SyncLatestOverElevenSourcesCombinesTheLatestValueOfEverySource()
    {
        var sources = CreateSources(ElevenSources);
        List<int[]> results = [];
        using var subscription = sources[0]
            .SyncLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                sources[9],
                sources[10],
                static (a, b, c, d, e, f, g, h, i, j, k) => new[] { a, b, c, d, e, f, g, h, i, j, k })
            .Subscribe(results.Add);
        await AssertCombinesLatestOfEverySource(sources, results).ConfigureAwait(false);
    }

    /// <summary>Verifies the twelve-source overload combines the latest value of every source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SyncLatestOverTwelveSourcesCombinesTheLatestValueOfEverySource()
    {
        var sources = CreateSources(TwelveSources);
        List<int[]> results = [];
        using var subscription = sources[0]
            .SyncLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                sources[9],
                sources[10],
                sources[11],
                static (a, b, c, d, e, f, g, h, i, j, k, l) => new[] { a, b, c, d, e, f, g, h, i, j, k, l })
            .Subscribe(results.Add);
        await AssertCombinesLatestOfEverySource(sources, results).ConfigureAwait(false);
    }

    /// <summary>Verifies the thirteen-source overload combines the latest value of every source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SyncLatestOverThirteenSourcesCombinesTheLatestValueOfEverySource()
    {
        var sources = CreateSources(ThirteenSources);
        List<int[]> results = [];
        using var subscription = sources[0]
            .SyncLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                sources[9],
                sources[10],
                sources[11],
                sources[12],
                static (a, b, c, d, e, f, g, h, i, j, k, l, m) => new[] { a, b, c, d, e, f, g, h, i, j, k, l, m })
            .Subscribe(results.Add);
        await AssertCombinesLatestOfEverySource(sources, results).ConfigureAwait(false);
    }

    /// <summary>Verifies the fourteen-source overload combines the latest value of every source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SyncLatestOverFourteenSourcesCombinesTheLatestValueOfEverySource()
    {
        var sources = CreateSources(FourteenSources);
        List<int[]> results = [];
        using var subscription = sources[0]
            .SyncLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                sources[9],
                sources[10],
                sources[11],
                sources[12],
                sources[13],
                static (a, b, c, d, e, f, g, h, i, j, k, l, m, n) =>
                    new[] { a, b, c, d, e, f, g, h, i, j, k, l, m, n })
            .Subscribe(results.Add);
        await AssertCombinesLatestOfEverySource(sources, results).ConfigureAwait(false);
    }

    /// <summary>Verifies the fifteen-source overload combines the latest value of every source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SyncLatestOverFifteenSourcesCombinesTheLatestValueOfEverySource()
    {
        var sources = CreateSources(FifteenSources);
        List<int[]> results = [];
        using var subscription = sources[0]
            .SyncLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                sources[9],
                sources[10],
                sources[11],
                sources[12],
                sources[13],
                sources[14],
                static (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o) =>
                    new[] { a, b, c, d, e, f, g, h, i, j, k, l, m, n, o })
            .Subscribe(results.Add);
        await AssertCombinesLatestOfEverySource(sources, results).ConfigureAwait(false);
    }

    /// <summary>Verifies the sixteen-source overload combines the latest value of every source.</summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    [Test]
    public async Task SyncLatestOverSixteenSourcesCombinesTheLatestValueOfEverySource()
    {
        var sources = CreateSources(SixteenSources);
        List<int[]> results = [];
        using var subscription = sources[0]
            .SyncLatest(
                sources[1],
                sources[2],
                sources[3],
                sources[4],
                sources[5],
                sources[6],
                sources[7],
                sources[8],
                sources[9],
                sources[10],
                sources[11],
                sources[12],
                sources[13],
                sources[14],
                sources[15],
                static (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p) =>
                    new[] { a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p })
            .Subscribe(results.Add);
        await AssertCombinesLatestOfEverySource(sources, results).ConfigureAwait(false);
    }
}
