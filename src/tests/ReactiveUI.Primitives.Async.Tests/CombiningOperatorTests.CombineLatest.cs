// Copyright (c) 2019-2026 ReactiveUI Association Incorporated. All rights reserved.
// ReactiveUI Association Incorporated licenses this file to you under the MIT license.
// See the LICENSE file in the project root for full license information.

using ReactiveUI.Primitives.Async.Disposables;
using ReactiveUI.Primitives.Async.Signals;

namespace ReactiveUI.Primitives.Async.Tests;

/// <summary>Tests for the CombineLatest operator.</summary>
public partial class CombiningOperatorTests
{
    /// <summary>Tests CombineLatest two sources combines latest values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestTwoSources_ThenCombinesLatestValues()
    {
        var signal1 = Signal.Create<int>();
        var signal2 = Signal.Create<string>();

        var results = new List<(int, string)>();
        await using var sub = await signal1.Values
            .CombineLatest(signal2.Values, (a, b) => (a, b))
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        await signal1.OnNextAsync(1, CancellationToken.None);
        await signal2.OnNextAsync("a", CancellationToken.None);
        await signal1.OnNextAsync(SampleValue2, CancellationToken.None);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo((1, "a"));
    }

    /// <summary>Verifies that CombineLatest with an empty sources collection completes immediately with success.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestEmptySources_ThenCompletesImmediately()
    {
        IObservableAsync<int>[] sources = [];

        Result? completionResult = null;
        await using var sub = await sources.CombineLatest()
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
    }

    /// <summary>Verifies that CombineLatest completes with success when one source completes without emitting any value.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestOneSourceCompletesWithoutEmitting_ThenCompletes()
    {
        IObservableAsync<int>[] sources = [SignalAsync.Return(1), SignalAsync.Empty<int>()];

        Result? completionResult = null;
        var items = new List<IReadOnlyList<int>>();

        await using var sub = await sources.CombineLatest()
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsSuccess).IsTrue();
        await Assert.That(items).IsEmpty();
    }

    /// <summary>Verifies that CombineLatest propagates a failure when one of the sources errors.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestOneSourceErrors_ThenFailurePropagates()
    {
        IObservableAsync<int>[] sources =
        [
            SignalAsync.Return(1), SignalAsync.Throw<int>(new InvalidOperationException("source fail"))
        ];

        Result? completionResult = null;

        await using var sub = await sources.CombineLatest()
            .SubscribeAsync(
                (_, _) => default,
                null,
                result =>
                {
                    completionResult = result;
                    return default;
                });

        await Assert.That(completionResult).IsNotNull();
        await Assert.That(completionResult!.Value.IsFailure).IsTrue();
    }

    /// <summary>Verifies that CombineLatest forwards error-resume events from inner sources to the downstream observer.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestSourceErrorResume_ThenForwarded()
    {
        var inner = SignalAsync.Create<int>(async (observer, ct) =>
        {
            await observer.OnErrorResumeAsync(new InvalidOperationException("warning"), ct);
            await observer.OnNextAsync(1, ct);
            await observer.OnCompletedAsync(Result.Success);
            return DisposableAsync.Empty;
        });

        IObservableAsync<int>[] sources = [inner];
        var errors = new List<Exception>();
        var items = new List<IReadOnlyList<int>>();

        await using var sub = await sources.CombineLatest()
            .SubscribeAsync(
                (x, _) =>
                {
                    items.Add(x);
                    return default;
                },
                (ex, _) =>
                {
                    errors.Add(ex);
                    return default;
                });

        await Assert.That(errors).Count().IsEqualTo(1);
    }

    /// <summary>Verifies that disposing a CombineLatest subscription during active emissions does not throw.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestDisposedDuringEmission_ThenNoError()
    {
        var signal1 = Signal.Create<int>();
        var signal2 = Signal.Create<int>();

        IObservableAsync<int>[] sources = [signal1.Values, signal2.Values];

        var sub = await sources.CombineLatest()
            .SubscribeAsync(
                (_, _) => default,
                null);

        await signal1.OnNextAsync(1, CancellationToken.None);

        await sub.DisposeAsync();

        await signal1.DisposeAsync();
        await signal2.DisposeAsync();
    }

    /// <summary>Tests CombineLatest with 3 sources combines all latest values.</summary>
    /// <returns>A <see cref="Task"/> representing the asynchronous test operation.</returns>
    [Test]
    public async Task WhenCombineLatestThreeSources_ThenCombinesAll()
    {
        var s1 = Signal.Create<int>();
        var s2 = Signal.Create<int>();
        var s3 = Signal.Create<int>();

        var results = new List<int>();
        await using var sub = await s1.Values
            .CombineLatest(s2.Values, s3.Values, (a, b, c) => a + b + c)
            .SubscribeAsync(
                (x, _) =>
                {
                    results.Add(x);
                    return default;
                },
                null);

        await s1.OnNextAsync(1, CancellationToken.None);
        await s2.OnNextAsync(SampleValue10, CancellationToken.None);
        await s3.OnNextAsync(SampleValue100, CancellationToken.None);

        await Assert.That(results).Count().IsGreaterThanOrEqualTo(1);
        await Assert.That(results[0]).IsEqualTo(Combined111);
    }
}
