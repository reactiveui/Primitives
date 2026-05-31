// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives;
using ReactiveUI.Primitives.Concurrency;
using ReactiveUI.Primitives.Extensions;
using ReactiveUI.Primitives.Extensions.Internal;
using ReactiveUI.Primitives.Extensions.Operators;
using System.Reactive;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Reactive.Subjects;
using System.Reactive.Threading.Tasks;
using System.IO;
using ReactiveUI.Primitives.Extensions.Tests;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for the fused Select/Where operators and the candidate-walking helpers
/// (<c>WhereSelect</c>, <c>SelectConstant</c>, <c>TrySelect</c>, <c>SelectManyThen</c>,
/// <c>RunAll</c>, <c>FirstMatchFromCandidates</c>).</summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>Constant emitted by SelectConstant tests so the chosen value is unambiguous.</summary>
    private const string SelectConstantSentinel = "constant";

    /// <summary>Fallback value used by FirstMatchFromCandidates when no candidate matches.</summary>
    private const string CandidateFallback = "fallback";

    /// <summary>
    /// Verifies that WhereSelect forwards only values that satisfy the predicate, projected through the selector.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereSelect_ThenEmitsOnlyMatchingProjectedValues()
    {
        var results = new List<string>();

        using var subscription = new[] { 1, SampleValue2, SampleValue3, SampleValue4 }
            .ToObservable()
            .WhereSelect(static x => x % 2 == 0, static x => $"e{x}")
            .Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo(["e2", "e4"]);
    }

    /// <summary>Verifies that WhereSelect forwards source errors to the downstream observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenWhereSelectSourceErrors_ThenErrorForwarded()
    {
        var expected = new InvalidOperationException("boom");
        Exception? caught = null;

        using var subscription = Observable.Throw<int>(expected)
            .WhereSelect(static x => true, static x => x.ToString())
            .Subscribe(static _ => { }, ex => caught = ex);

        await Assert.That(caught).IsEqualTo(expected);
    }

    /// <summary>Verifies that SelectConstant emits the constant for every source element regardless of value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectConstant_ThenEmitsConstantForEachElement()
    {
        var results = new List<string>();

        using var subscription = new[] { 1, SampleValue2, SampleValue3 }
            .ToObservable()
            .SelectConstant(SelectConstantSentinel)
            .Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo([SelectConstantSentinel, SelectConstantSentinel, SelectConstantSentinel]);
    }

    /// <summary>Verifies that TrySelect drops null projected values and emits only the non-null results.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTrySelect_ThenDropsNullProjections()
    {
        var results = new List<string>();

        using var subscription = new[] { 1, SampleValue2, SampleValue3, SampleValue4 }
            .ToObservable()
            .TrySelect(static x => x % 2 == 0 ? $"e{x}" : null)
            .Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo(["e2", "e4"]);
    }

    /// <summary>Verifies that SelectManyThen runs both projections in sequence and forwards the final inner result.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectManyThen_ThenChainsTwoProjections()
    {
        const int ExpectedChained = 11;
        var results = new List<int>();

        // Single source emission keeps the test deterministic — the operator's downstream
        // completes once the inner-inner observable completes, so a multi-emission source
        // would race against the early-completion semantic.
        using var subscription = Observable.Return(1)
            .SelectManyThen(
                static x => Observable.Return(x * SampleValue10),
                static mid => Observable.Return(mid + 1))
            .Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo([ExpectedChained]);
    }

    /// <summary>Verifies that RunAll completes after every source has completed and emits a single RxVoid.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRunAll_ThenEmitsSingleUnitAfterAllComplete()
    {
        var results = new List<RxVoid>();
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        IReadOnlyList<IObservable<RxVoid>> sources =
        [
            Observable.Return(RxVoid.Default),
            Observable.Return(RxVoid.Default),
            Observable.Return(RxVoid.Default),
        ];

        using var subscription = sources.RunAll().Subscribe(
            results.Add,
            () => completed.TrySetResult());

        await completed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        await Assert.That(results).Count().IsEqualTo(1);
    }

    /// <summary>
    /// Verifies that RunAll on an empty list still emits the terminal <see cref="RxVoid.Default"/> and
    /// completes — the empty case is vacuously "all sources completed".
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenRunAllEmpty_ThenEmitsUnitAndCompletes()
    {
        var results = new List<RxVoid>();
        var completed = false;

        using var subscription = Array.Empty<IObservable<RxVoid>>().RunAll().Subscribe(
            results.Add,
            () => completed = true);

        await Assert.That(completed).IsTrue();
        await Assert.That(results).Count().IsEqualTo(1);
    }

    /// <summary>Verifies that FirstMatchFromCandidates emits the first transformed value that satisfies the predicate.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstMatchFromCandidates_ThenEmitsFirstSatisfyingValue()
    {
        var keys = new[] { "a", "b", "c" };
        var results = new List<string>();

        using var subscription = keys.FirstMatchFromCandidates(
            static key => Observable.Return(key + "-raw"),
            static raw => raw.ToUpperInvariant(),
            static transformed => transformed.StartsWith('B'),
            CandidateFallback)
            .Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo(["B-RAW"]);
    }

    /// <summary>Verifies that FirstMatchFromCandidates emits the fallback when no candidate satisfies the predicate.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstMatchFromCandidatesNoMatch_ThenEmitsFallback()
    {
        var keys = new[] { "a", "b", "c" };
        var results = new List<string>();

        using var subscription = keys.FirstMatchFromCandidates(
            static key => Observable.Return(key + "-raw"),
            static raw => raw.ToUpperInvariant(),
            static _ => false,
            CandidateFallback)
            .Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo([CandidateFallback]);
    }

    /// <summary>
    /// Verifies that FirstMatchFromCandidates skips candidates whose projection errors and continues walking the list.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFirstMatchFromCandidatesProjectionErrors_ThenSkipsAndContinues()
    {
        var keys = new[] { "fail", "match" };
        var results = new List<string>();

        using var subscription = keys.FirstMatchFromCandidates(
            static key => key == "fail"
                ? Observable.Throw<string>(new InvalidOperationException("ignored"))
                : Observable.Return(key + "-raw"),
            static raw => raw.ToUpperInvariant(),
            static transformed => transformed.StartsWith('M'),
            CandidateFallback)
            .Subscribe(results.Add);

        await Assert.That(results).IsCollectionEqualTo(["MATCH-RAW"]);
    }
}
