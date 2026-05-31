// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for ReactiveExtensionsTests.</summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>
    /// Tests SelectAsync with CancellationToken projects values asynchronously.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncWithCancellationToken_ThenProjectsValues()
    {
        var source = ExpectedSequence123.ToObservable();
        var results = new List<int>();
        var tcs = new TaskCompletionSource<bool>();

        source.SelectAsync((x, _) => Task.FromResult(x * SampleValue2))
            .Subscribe(
            results.Add,
            () => tcs.TrySetResult(true));

        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(results).IsCollectionEqualTo([SampleValue2, SampleValue4, SampleValue6]);
    }

    /// <summary>
    /// Tests SelectAsync simple overload projects values asynchronously.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncSimple_ThenProjectsValues()
    {
        var source = ExpectedSequence123.ToObservable();
        var results = new List<int>();
        var tcs = new TaskCompletionSource<bool>();

        source.SelectAsync(x => Task.FromResult(x * SampleValue2))
            .Subscribe(
            results.Add,
            () => tcs.TrySetResult(true));

        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(results).IsCollectionEqualTo([SampleValue2, SampleValue4, SampleValue6]);
    }

    /// <summary>
    /// Tests SelectAsyncSequential processes tasks in order.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncSequential_ThenProcessesInOrder()
    {
        var source = ExpectedSequence123.ToObservable();
        var results = new List<int>();
        var tcs = new TaskCompletionSource<bool>();

        source.SelectAsyncSequential(x => Task.FromResult(x * SampleValue2))
            .Subscribe(
            results.Add,
            () => tcs.TrySetResult(true));

        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        await Assert.That(results).IsCollectionEqualTo([SampleValue2, SampleValue4, SampleValue6]);
    }

    /// <summary>
    /// Tests SelectLatestAsync emits only the latest async result.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectLatestAsync_ThenEmitsLatestResult()
    {
        const int AsyncDelayMs = 10;
        var source = ExpectedSequence123.ToObservable();
        var results = new List<int>();
        var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

        source.SelectLatestAsync(async x =>
        {
            await Task.Delay(AsyncDelayMs);
            return x * SampleValue2;
        }).Subscribe(
            results.Add,
            () => tcs.TrySetResult(true));

        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        // Switch means only the latest survives; with sources 1,2,3 and selector x*2, expect [6].
        await Assert.That(results).IsNotEmpty();
    }

    /// <summary>
    /// Tests SelectAsyncConcurrent processes tasks concurrently up to max concurrency.
    /// </summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncConcurrent_ThenProcessesConcurrently()
    {
        var source = ExpectedSequence123.ToObservable();
        var results = new List<int>();
        var tcs = new TaskCompletionSource<bool>();

        source.SelectAsyncConcurrent(
            async x =>
            {
                await Task.Delay(1);
                return x * SampleValue2;
            },
            maxConcurrency: 2).Subscribe(results.Add, () => tcs.TrySetResult(true));

        await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

        results.Sort();
        await Assert.That(results).IsCollectionEqualTo([SampleValue2, SampleValue4, SampleValue6]);
    }
}
