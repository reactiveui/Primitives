// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using System.Reactive;
using ReactiveUI.Primitives.Extensions.Operators;

namespace ReactiveUI.Primitives.Extensions.Tests;

/// <summary>Tests for ReactiveExtensionsTests.</summary>
public partial class ReactiveExtensionsTests
{
    /// <summary>Tests SelectAsync with CancellationToken projects values asynchronously.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncWithCancellationToken_ThenProjectsValues()
    {
        var source = ExpectedSequence123.ToObservable();
        List<int> results = [];
        TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = source.SelectAsync(static (x, _) => Task.FromResult(x * SampleValue2))
            .Subscribe(
                results.Add,
                () => tcs.TrySetResult(true));

        await tcs.Task;

        await Assert.That(results).IsCollectionEqualTo([SampleValue2, SampleValue4, SampleValue6]);
    }

    /// <summary>Tests SelectAsync simple overload projects values asynchronously.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncSimple_ThenProjectsValues()
    {
        var source = ExpectedSequence123.ToObservable();
        List<int> results = [];
        TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = source.SelectAsync(static x => Task.FromResult(x * SampleValue2))
            .Subscribe(
                results.Add,
                () => tcs.TrySetResult(true));

        await tcs.Task;

        await Assert.That(results).IsCollectionEqualTo([SampleValue2, SampleValue4, SampleValue6]);
    }

    /// <summary>Tests SelectAsyncSequential processes tasks in order.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncSequential_ThenProcessesInOrder()
    {
        var source = ExpectedSequence123.ToObservable();
        List<int> results = [];
        TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = source.SelectAsyncSequential(static x => Task.FromResult(x * SampleValue2))
            .Subscribe(
                results.Add,
                () => tcs.TrySetResult(true));

        await tcs.Task;

        await Assert.That(results).IsCollectionEqualTo([SampleValue2, SampleValue4, SampleValue6]);
    }

    /// <summary>Tests SelectLatestAsync emits only the latest async result.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectLatestAsync_ThenEmitsLatestResult()
    {
        List<int> results = [];
        TaskCompletionSource<int> first = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<int> second = new(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource<int> latest = new(TaskCreationOptions.RunContinuationsAsynchronously);
        Task<int>[] projections = [first.Task, second.Task, latest.Task];
        using SelectLatestAsyncObservable<int, int>.SelectLatestAsyncSink sink = new(
            Observer.Create<int>(results.Add),
            value => projections[value - 1]);

        var firstOperation = sink.OnNextAsync(1);
        var secondOperation = sink.OnNextAsync(SampleValue2);
        var latestOperation = sink.OnNextAsync(SampleValue3);
        latest.SetResult(SampleValue6);
        await latestOperation;
        first.SetResult(SampleValue2);
        second.SetResult(SampleValue4);
        await Task.WhenAll(firstOperation, secondOperation);

        await Assert.That(results).IsCollectionEqualTo([SampleValue6]);
    }

    /// <summary>Tests SelectAsyncConcurrent processes tasks concurrently up to max concurrency.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenSelectAsyncConcurrent_ThenProcessesConcurrently()
    {
        var source = ExpectedSequence123.ToObservable();
        List<int> results = [];
        TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
        const int MaxConcurrency = 2;

        _ = source.SelectAsyncConcurrent(
            static async x =>
            {
                await Task.Yield();
                return x * SampleValue2;
            },
            MaxConcurrency).Subscribe(results.Add, () => tcs.TrySetResult(true));

        await tcs.Task;

        results.Sort();
        await Assert.That(results).IsCollectionEqualTo([SampleValue2, SampleValue4, SampleValue6]);
    }
}
