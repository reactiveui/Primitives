// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive.Subjects;
using System.Text.RegularExpressions;

namespace ReactiveUI.Primitives.Extensions.Tests.Operators;

/// <summary>Tests shuffle, regex filtering, and conditional projection.</summary>
public partial class SimpleSyncOperatorTests
{
    /// <summary>Synthetic error message attached to source errors.</summary>
    private const string SourceErrorMessage = "source error";

    /// <summary>Apple sentinel used by regex-filter tests.</summary>
    private const string Apple = "apple";

    /// <summary>Shuffle test sentinels.</summary>
    private const int Shuffle1 = 1;

    /// <summary>Shuffle test sentinel.</summary>
    private const int Shuffle2 = 2;

    /// <summary>Shuffle test sentinel.</summary>
    private const int Shuffle3 = 3;

    /// <summary>Shuffle test sentinel.</summary>
    private const int Shuffle4 = 4;

    /// <summary>Shuffle test sentinel.</summary>
    private const int Shuffle5 = 5;

    /// <summary>Divisor the <c>TrySelect</c> projection uses to keep even values and drop the odd ones.</summary>
    private const int EvenDivisor = 2;

    /// <summary>Inputs used by the <c>Shuffle</c> multiset test.</summary>
    private static readonly int[] ShuffleInput = [Shuffle1, Shuffle2, Shuffle3, Shuffle4, Shuffle5];

    /// <summary>Inputs used by the <c>TrySelect</c> null-projection test; the odd values are dropped.</summary>
    private static readonly int[] TrySelectInput = [1, 2, 3, 4];

    /// <summary>Verifies that <c>Shuffle</c> preserves the multiset of input values.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenShuffle_ThenPreservesMultiset()
    {
        Subject<int[]> subject = new();
        int[]? shuffled = null;
        using var sub = subject.Shuffle().Subscribe(value => shuffled = value);
        subject.OnNext((int[])ShuffleInput.Clone());
        await Assert.That(shuffled).IsNotNull();
        var sorted = (int[])shuffled!.Clone();
        Array.Sort(sorted);
        await Assert.That(sorted).IsCollectionEqualTo(ShuffleInput);
    }

    /// <summary>Verifies that <c>Shuffle</c> forwards <c>null</c> arrays unchanged.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenShuffleNullArray_ThenForwardsAsIs()
    {
        Subject<int[]> subject = new();
        var received = 0;
        int[]? value = null;
        using var sub = subject.Shuffle().Subscribe(v =>
        {
            received++;
            value = v;
        });
        subject.OnNext(null!);
        await Assert.That(received).IsEqualTo(1);
        await Assert.That(value).IsNull();
    }

    /// <summary>Verifies that <c>Shuffle</c> forwards source errors and disposes the RNG.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenShuffleSourceErrors_ThenForwardsError()
    {
        Subject<int[]> subject = new();
        Exception? caught = null;
        InvalidOperationException expected = new(SourceErrorMessage);
        using var sub = subject.Shuffle().Subscribe(
            static _ => { },
            ex => caught = ex);
        subject.OnError(expected);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>Shuffle</c> forwards source completion and disposes the RNG.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenShuffleSourceCompletes_ThenForwardsCompletion()
    {
        Subject<int[]> subject = new();
        var completed = false;
        using var sub = subject.Shuffle().Subscribe(
            static _ => { },
            () => completed = true);
        subject.OnCompleted();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Verifies that <c>Filter</c> with a regex pattern forwards only matching strings.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFilterRegexMatches_ThenForwardsMatching()
    {
        string[] input = [Apple, "banana", "avocado"];
        List<string> results = [];
        using var sub = input.ToObservable().Filter("^a").Subscribe(results.Add);
        await Assert.That(results).IsCollectionEqualTo([Apple, "avocado"]);
    }

    /// <summary>Verifies that <c>Filter</c> with a precompiled regex behaves identically.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFilterRegexCompiled_ThenForwardsMatching()
    {
        string[] input = ["aa", "bb", "ac"];
        List<string> results = [];
        var regex = StartsWithA();
        using var sub = input.ToObservable().Filter(regex).Subscribe(results.Add);
        await Assert.That(results).IsCollectionEqualTo(["aa", "ac"]);
    }

    /// <summary>Verifies that <c>Filter</c> ignores null inputs without throwing.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFilterNullInput_ThenIgnored()
    {
        Subject<string> subject = new();
        List<string> results = [];
        using var sub = subject.Filter("^a").Subscribe(results.Add);
        subject.OnNext(null!);
        subject.OnNext(Apple);
        await Assert.That(results).IsCollectionEqualTo([Apple]);
    }

    /// <summary>Verifies that <c>Filter</c> forwards regex exceptions to <c>OnError</c>.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenFilterRegexThrows_ThenForwardsError()
    {
        RegexMatchTimeoutException expected = new("matching failed");
        ThrowingRegex regex = new(expected);
        Subject<string> subject = new();
        Exception? caught = null;
        using var sub = subject.Filter(regex).Subscribe(
            static _ => { },
            ex => caught = ex);
        subject.OnNext(Apple);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>TrySelect</c> drops null projections and forwards non-nulls.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTrySelectNullProjection_ThenDropped()
    {
        List<string> results = [];
        using var sub = TrySelectInput.ToObservable().TrySelect(static x => x % EvenDivisor == 0 ? x.ToString() : null)
            .Subscribe(results.Add);
        await Assert.That(results).IsCollectionEqualTo(["2", "4"]);
    }

    /// <summary>Verifies that an exception thrown by the <c>TrySelect</c> selector is forwarded.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTrySelectThrows_ThenForwardsError()
    {
        Subject<int> subject = new();
        Exception? caught = null;
        InvalidOperationException expected = new("selector failed");
        using var sub = subject.TrySelect<int, string>(_ => throw expected).Subscribe(
            static _ => { },
            ex => caught = ex);
        subject.OnNext(1);
        await Assert.That(caught).IsSameReferenceAs(expected);
    }

    /// <summary>Verifies that <c>TrySelect</c> forwards source completion.</summary>
    /// <returns>A <see cref = "Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenTrySelectSourceCompletes_ThenForwardsCompletion()
    {
        Subject<int> subject = new();
        var completed = false;
        using var sub = subject.TrySelect(static x => x.ToString()).Subscribe(
            static _ => { },
            () => completed = true);
        subject.OnCompleted();
        await Assert.That(completed).IsTrue();
    }

    /// <summary>Compiled regex matching strings that begin with the letter 'a'.</summary>
    /// <returns>A compile-time generated <see cref = "Regex"/> instance.</returns>
    [GeneratedRegex("^a")]
    private static partial Regex StartsWithA();

    /// <summary>A regex whose matching engine throws the supplied error.</summary>
    private sealed class ThrowingRegex : Regex
    {
        /// <summary>Initializes a new instance of the <see cref="ThrowingRegex"/> class.</summary>
        /// <param name="error">The matching error.</param>
        public ThrowingRegex(Exception error)
        {
            factory = new ThrowingRunnerFactory(error);
            capsize = 1;
        }

        /// <summary>Creates matching engines that fail with the supplied error.</summary>
        /// <param name="error">The matching error.</param>
        private sealed class ThrowingRunnerFactory(Exception error) : RegexRunnerFactory
        {
            /// <inheritdoc/>
            protected override RegexRunner CreateInstance() => new ThrowingRunner(error);
        }

        /// <summary>Throws when matching begins.</summary>
        /// <param name="error">The matching error.</param>
        private sealed class ThrowingRunner(Exception error) : RegexRunner
        {
            /// <inheritdoc/>
            protected override void Go() => throw error;

            /// <inheritdoc/>
            protected override bool FindFirstChar() => true;

            /// <inheritdoc/>
            protected override void InitTrackCount() => runtrackcount = 1;
        }
    }
}
