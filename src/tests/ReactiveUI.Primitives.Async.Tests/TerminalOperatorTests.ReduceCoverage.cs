// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Coverage tests for ReduceAsync compatibility overloads and async witness failure paths.</summary>
public partial class TerminalOperatorTests
{
    /// <summary>Expected sum for the sequence 1, 2, 3.</summary>
    private const int ReduceCoverageExpectedSum = 6;

    /// <summary>Tests AggregateAsync cancellation-token overloads forward through ReduceAsync aliases.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenAggregateAsyncCancellationTokenOverloads_ThenComputeFinalValues()
    {
        using var cancellation = new CancellationTokenSource();
        var asyncResult = await SignalAsync.Range(1, 3).AggregateAsync(
            0,
            async (accumulator, value, token) =>
            {
                await Task.Yield();
                token.ThrowIfCancellationRequested();
                return accumulator + value;
            },
            cancellation.Token);
        var syncResult = await SignalAsync.Range(1, 3).AggregateAsync(
            0,
            static (accumulator, value) => accumulator + value,
            cancellation.Token);
        var selectedResult = await SignalAsync.Range(1, 3).AggregateAsync(
            0,
            static (accumulator, value) => accumulator + value,
            static accumulator => $"Sum={accumulator}",
            cancellation.Token);

        await Assert.That(asyncResult).IsEqualTo(ReduceCoverageExpectedSum);
        await Assert.That(syncResult).IsEqualTo(ReduceCoverageExpectedSum);
        await Assert.That(selectedResult).IsEqualTo("Sum=6");
    }

    /// <summary>Tests async ReduceAsync propagates resumable source errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReduceAsyncAsyncSourceEmitsErrorResume_ThenThrowsSourceException()
    {
        var expectedError = new InvalidOperationException("async reduce resume error");
        var source = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(expectedError, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await source.ReduceAsync(
                0,
                async (accumulator, value, _) =>
                {
                    await Task.Yield();
                    return accumulator + value;
                }));

        await Assert.That(ex!.Message).IsEqualTo("async reduce resume error");
    }

    /// <summary>Tests async ReduceAsync propagates terminal source failures.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenReduceAsyncAsyncSourceFails_ThenThrows()
    {
        var expectedError = new InvalidOperationException("async reduce failed");

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await SignalAsync.Throw<int>(expectedError).ReduceAsync(
                0,
                async (accumulator, value, _) =>
                {
                    await Task.Yield();
                    return accumulator + value;
                }));

        await Assert.That(ex).IsSameReferenceAs(expectedError);
    }
}
